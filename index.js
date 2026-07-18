// ─────────────────────────────────────────────────────────────────────────
// YouTube Pro+ — Watch Party Room Relay Server
//
// Plain WebSocket relay. Rooms live in memory only (no database).
// The server never looks at video content — it just forwards small JSON
// messages from the host to everyone else in the same room.
//
// v2: host connections can drop and come back (flaky wifi, Render free-tier
// cold starts, laptop sleep) without killing the room. A dropped host gets
// a grace period to "reclaim" their room with the same code instead of the
// room being deleted the instant the socket closes.
// ─────────────────────────────────────────────────────────────────────────

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I ambiguity
const CODE_REGEX = /^[ABCDEFGHJKLMNPQRSTUVWXYZ23456789]{6}$/; // must match makeRoomCode() output
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;   // auto-clean rooms idle > 6h
const HOST_GRACE_MS = 90 * 1000;          // how long a room survives after the host's socket drops
// (90s comfortably covers a Render free-tier cold start, which can take
// 30-50s, plus normal client reconnect backoff.)

// A guest's socket can drop for reasons that have nothing to do with them
// actually leaving — most commonly, the host changes video and the guest's
// tab falls back to a hard page reload instead of YouTube's in-page nav,
// which kills the content script + WebSocket for a second. Give a guest
// this long to silently resume their same identity (via a guestToken)
// before we announce them as having left the room.
const GUEST_RECONNECT_GRACE_MS = 10 * 1000;

// ── Chat config ─────────────────────────────────────────────────────────
// Chat is 100% in-memory and lives only as long as the Room object does.
// The moment a room is deleted (host ends it, grace period expires, TTL
// sweep, etc.) every message in it is gone — nothing is ever persisted to
// disk or a database.
const CHAT_HISTORY_LIMIT = 200;   // how many recent messages we keep in memory per room
const CHAT_MAX_LEN = 500;         // max characters per chat message
const NAME_MAX_LEN = 24;          // max characters for a display name

// ── Security / reliability limits ────────────────────────────────────────
// These exist to keep one abusive or buggy client from taking down the
// relay for everyone else. All limits are enforced server-side — the
// client never has to be trusted.
const MAX_PAYLOAD_BYTES = 8 * 1024;        // hard cap on a single WS frame (ws library enforces this)
const MAX_TOTAL_ROOMS = 2000;              // global cap on concurrent rooms (memory guard)
const MAX_CLIENTS_PER_ROOM = 50;           // reasonable watch-party size cap
const MAX_TOTAL_CONNECTIONS = 5000;        // global cap on concurrent sockets

const CHAT_RATE_LIMIT = 5;                 // max chat messages...
const CHAT_RATE_WINDOW_MS = 8000;          // ...per this many ms, per client
const GENERAL_RATE_LIMIT = 30;             // max messages of ANY type...
const GENERAL_RATE_WINDOW_MS = 10000;      // ...per this many ms, per connection
const ROOM_ACTION_RATE_LIMIT = 10;         // max create/join/reclaim attempts...
const ROOM_ACTION_RATE_WINDOW_MS = 60000;  // ...per this many ms, per connection
const MAX_VIOLATIONS_BEFORE_KICK = 8;      // repeated rate-limit hits => drop the connection

// Per-IP room-creation throttle. Keyed by remote address so one machine
// can't exhaust MAX_TOTAL_ROOMS by opening many sockets.
const IP_CREATE_LIMIT = 20;                // max rooms created...
const IP_CREATE_WINDOW_MS = 60 * 60 * 1000; // ...per hour, per IP
/** @type {Map<string, number[]>} */
const ipCreateTimestamps = new Map();

// Generic sliding-window rate limiter. Mutates `timestamps` in place and
// returns true if the action is allowed right now.
function checkRateLimit(timestamps, limit, windowMs) {
  const now = Date.now();
  while (timestamps.length && now - timestamps[0] > windowMs) timestamps.shift();
  if (timestamps.length >= limit) return false;
  timestamps.push(now);
  return true;
}

function noteViolation(ws) {
  ws.violations = (ws.violations || 0) + 1;
  if (ws.violations >= MAX_VIOLATIONS_BEFORE_KICK) {
    try { ws.send(JSON.stringify({ type: 'error', message: 'Disconnected for exceeding rate limits.' })); } catch {}
    ws.terminate();
    return true;
  }
  return false;
}

function sanitizeName(name, fallback) {
  const n = (typeof name === 'string' ? name : '').trim().slice(0, NAME_MAX_LEN);
  return n || fallback;
}

function sanitizeText(text) {
  const t = (typeof text === 'string' ? text : '').trim().slice(0, CHAT_MAX_LEN);
  return t;
}

function makeMessageId() {
  return crypto.randomBytes(6).toString('hex');
}

function makeGuestToken() {
  return crypto.randomBytes(12).toString('hex');
}

/** @type {Map<string, Room>} */
const rooms = new Map();

function makeRoomCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () =>
      CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]
    ).join('');
  } while (rooms.has(code));
  return code;
}

function makeClientId() {
  return crypto.randomBytes(8).toString('hex');
}

function makeHostToken() {
  return crypto.randomBytes(16).toString('hex');
}

class Room {
  constructor(code, hostId, hostWs, hostToken) {
    this.code = code;
    this.hostId = hostId;
    // hostId changes every time the host reconnects/reclaims (it's tied to
    // the live socket). hostPublicId never changes for the life of the room,
    // so the host's own past chat bubbles don't flip from "mine" to
    // "someone else's" after they reconnect.
    this.hostPublicId = hostId;
    this.hostToken = hostToken;
    this.hostMissing = false;
    this.graceTimer = null;
    this.clients = new Map(); // clientId -> ws
    this.clients.set(hostId, hostWs);
    this.lastState = null; // { videoId, time, playing, updatedAt }
    this.names = new Map(); // clientId -> display name (in-memory only)
    this.messages = []; // in-memory chat buffer, wiped when the Room is deleted
    // token -> { name, publicId, clientId, leaveTimer }. Lets a guest whose
    // socket drops (e.g. hard reload on video change) silently resume the
    // same identity instead of showing up as "left" then "joined" in chat.
    // publicId is stable across reconnects (for "mine" chat styling);
    // clientId tracks the current live socket and DOES change each reconnect.
    this.guestIdentities = new Map();
    this.touch();
  }
  touch() {
    this.lastUsed = Date.now();
  }
  // The id used to *display*/attribute a message, stable across that
  // person's reconnects — as opposed to ws.clientId, which is a fresh
  // random value every time the underlying socket reconnects.
  publicIdFor(ws) {
    if (ws.isHost && this.hostId === ws.clientId) return this.hostPublicId;
    const identity = ws.guestToken ? this.guestIdentities.get(ws.guestToken) : null;
    return identity ? identity.publicId : ws.clientId;
  }
  // Case/whitespace-insensitive name collision check, so "Sneha" and
  // " sneha " are treated as the same name. excludeClientId lets a client
  // check against everyone EXCEPT their own current entry (e.g. keeping
  // their own name during a rename).
  isNameTaken(name, excludeClientId) {
    const wanted = name.trim().toLowerCase();
    for (const [id, existingName] of this.names) {
      if (id === excludeClientId) continue;
      if (existingName.trim().toLowerCase() === wanted) return true;
    }
    return false;
  }
  broadcast(fromId, payload) {
    const msg = JSON.stringify(payload);
    for (const [id, ws] of this.clients) {
      if (id === fromId) continue;
      if (ws.readyState === ws.OPEN) ws.send(msg);
    }
  }
  // Like broadcast(), but also delivers to the sender — used for chat so
  // everyone (including the person who sent it) sees the same ordered feed
  // and gets the same server-assigned id/timestamp.
  broadcastAll(payload) {
    const msg = JSON.stringify(payload);
    for (const [, ws] of this.clients) {
      if (ws.readyState === ws.OPEN) ws.send(msg);
    }
  }
  addChatMessage(entry) {
    this.messages.push(entry);
    if (this.messages.length > CHAT_HISTORY_LIMIT) this.messages.shift();
  }
  nameFor(clientId) {
    return this.names.get(clientId) || 'Guest';
  }
  // Called whenever the room itself is being deleted, so we don't leave
  // dangling timers around waiting to fire "left the room" into a room
  // that no longer exists.
  clearAllGuestTimers() {
    for (const identity of this.guestIdentities.values()) {
      if (identity.leaveTimer) clearTimeout(identity.leaveTimer);
    }
    this.guestIdentities.clear();
  }
  clearGrace() {
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }
    this.hostMissing = false;
  }
}

// Plain HTTP endpoint — lets you (and free uptime pingers) confirm the
// server is alive, and gives Render something to health-check.
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('YouTube Pro+ room relay is running.');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD_BYTES });

wss.on('connection', (ws, req) => {
  // Global connection cap — protects server memory if something is
  // opening a flood of sockets (bug or abuse).
  if (wss.clients.size > MAX_TOTAL_CONNECTIONS) {
    try { ws.send(JSON.stringify({ type: 'error', message: 'Server is at capacity. Please try again shortly.' })); } catch {}
    ws.close();
    return;
  }

  ws.isAlive = true;
  ws.clientId = makeClientId();
  ws.roomCode = null;
  ws.isHost = false;
  ws.remoteAddr = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  ws.violations = 0;
  ws.generalTimestamps = [];
  ws.chatTimestamps = [];
  ws.roomActionTimestamps = [];

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // Overall flood guard — applies no matter what msg.type is.
    if (!checkRateLimit(ws.generalTimestamps, GENERAL_RATE_LIMIT, GENERAL_RATE_WINDOW_MS)) {
      if (noteViolation(ws)) return;
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed input
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'create_room': {
        if (!checkRateLimit(ws.roomActionTimestamps, ROOM_ACTION_RATE_LIMIT, ROOM_ACTION_RATE_WINDOW_MS)) {
          noteViolation(ws);
          ws.send(JSON.stringify({ type: 'error', message: 'Too many attempts — please slow down.' }));
          return;
        }
        if (rooms.size >= MAX_TOTAL_ROOMS) {
          ws.send(JSON.stringify({ type: 'error', message: 'Server is at capacity. Please try again shortly.' }));
          return;
        }
        const ipTimestamps = ipCreateTimestamps.get(ws.remoteAddr) || [];
        if (!checkRateLimit(ipTimestamps, IP_CREATE_LIMIT, IP_CREATE_WINDOW_MS)) {
          ws.send(JSON.stringify({ type: 'error', message: 'Too many rooms created from this network. Try again later.' }));
          return;
        }
        ipCreateTimestamps.set(ws.remoteAddr, ipTimestamps);

        const code = makeRoomCode();
        const hostToken = makeHostToken();
        const room = new Room(code, ws.clientId, ws, hostToken);
        rooms.set(code, room);
        room.names.set(ws.clientId, sanitizeName(msg.name, 'Host'));
        ws.roomCode = code;
        ws.isHost = true;
        ws.send(JSON.stringify({
          type: 'room_created',
          code,
          clientId: ws.clientId,
          hostToken
        }));
        break;
      }

      // A host whose socket dropped tries to resume the SAME room (same
      // code) instead of spinning up a brand new one that guests don't
      // know about.
      case 'reclaim_host': {
        if (!checkRateLimit(ws.roomActionTimestamps, ROOM_ACTION_RATE_LIMIT, ROOM_ACTION_RATE_WINDOW_MS)) {
          noteViolation(ws);
          ws.send(JSON.stringify({ type: 'error', message: 'Too many attempts — please slow down.' }));
          return;
        }
        const code = (typeof msg.code === 'string' ? msg.code : '').toUpperCase().trim();
        if (!CODE_REGEX.test(code)) {
          ws.send(JSON.stringify({ type: 'reclaim_failed' }));
          return;
        }
        const room = rooms.get(code);
        if (!room || typeof msg.hostToken !== 'string' || room.hostToken !== msg.hostToken) {
          ws.send(JSON.stringify({ type: 'reclaim_failed' }));
          return;
        }

        // Drop the stale ws entry (old socket for the same host), attach fresh one.
        const oldHostName = room.names.get(room.hostId);
        room.names.delete(room.hostId);
        room.clients.delete(room.hostId);
        room.hostId = ws.clientId;
        room.clients.set(ws.clientId, ws);

        let hostName = oldHostName || 'Host';
        if (msg.name) {
          const requestedName = sanitizeName(msg.name, hostName);
          // Keep the old name if the requested one collides with someone
          // else already in the room, rather than failing the reconnect.
          if (requestedName.toLowerCase() === hostName.toLowerCase() || !room.isNameTaken(requestedName, null)) {
            hostName = requestedName;
          }
        }
        room.names.set(ws.clientId, hostName);
        room.clearGrace();
        room.touch();
        ws.roomCode = code;
        ws.isHost = true;

        ws.send(JSON.stringify({
          type: 'room_reclaimed',
          code,
          clientId: room.hostPublicId, // stable across reconnects — keeps "mine" chat styling correct
          hostToken: room.hostToken,
          memberCount: room.clients.size,
          state: room.lastState,
          messages: room.messages
        }));
        break;
      }

      case 'join_room': {
        if (!checkRateLimit(ws.roomActionTimestamps, ROOM_ACTION_RATE_LIMIT, ROOM_ACTION_RATE_WINDOW_MS)) {
          noteViolation(ws);
          ws.send(JSON.stringify({ type: 'error', message: 'Too many attempts — please slow down.' }));
          return;
        }
        const code = (typeof msg.code === 'string' ? msg.code : '').toUpperCase().trim();
        if (!CODE_REGEX.test(code)) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code and try again.' }));
          return;
        }
        const room = rooms.get(code);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code and try again.' }));
          return;
        }

        // If this connection is presenting a token for an identity we
        // already know about, treat it as the SAME guest resuming — no
        // capacity check needed (they already hold a seat) and no
        // join/leave chat spam.
        const rejoinToken = typeof msg.rejoinToken === 'string' ? msg.rejoinToken : null;
        const existing = rejoinToken ? room.guestIdentities.get(rejoinToken) : null;

        if (existing) {
          room.clients.set(ws.clientId, ws);
          existing.clientId = ws.clientId;
          if (msg.name) {
            const requestedName = sanitizeName(msg.name, existing.name);
            // Keep the old name if the new one collides with someone else —
            // don't fail a reconnect over a rename conflict.
            if (requestedName.toLowerCase() === existing.name.toLowerCase() || !room.isNameTaken(requestedName, null)) {
              existing.name = requestedName;
            }
          }
          room.names.set(ws.clientId, existing.name);
          room.touch();
          ws.roomCode = code;
          ws.isHost = false;
          ws.guestToken = rejoinToken;
          ws.send(JSON.stringify({
            type: 'room_joined',
            code,
            clientId: existing.publicId, // stable across reconnects — keeps "mine" chat styling correct
            guestToken: rejoinToken,
            memberCount: room.clients.size,
            hostPaused: room.hostMissing,
            state: room.lastState,
            messages: room.messages
          }));
          // Member count is unchanged from the room's perspective (this
          // person never really left), so nothing to broadcast here.
          break;
        }

        if (room.clients.size >= MAX_CLIENTS_PER_ROOM) {
          ws.send(JSON.stringify({ type: 'error', message: 'This room is full.' }));
          return;
        }

        const guestName = sanitizeName(msg.name, 'Guest');
        if (room.isNameTaken(guestName, null)) {
          ws.send(JSON.stringify({ type: 'error', message: `"${guestName}" is already in use in this room — please choose a different name.` }));
          return;
        }
        const guestToken = makeGuestToken();
        room.guestIdentities.set(guestToken, { name: guestName, publicId: ws.clientId, clientId: ws.clientId, leaveTimer: null });
        room.clients.set(ws.clientId, ws);
        room.names.set(ws.clientId, guestName);
        room.touch();
        ws.roomCode = code;
        ws.isHost = false;
        ws.guestToken = guestToken;
        ws.send(JSON.stringify({
          type: 'room_joined',
          code,
          clientId: ws.clientId, // == publicId at this point, since it's a brand new identity
          guestToken,
          memberCount: room.clients.size,
          hostPaused: room.hostMissing, // let the guest UI show "waiting for host to reconnect"
          state: room.lastState, // so the new viewer can jump to the current spot
          messages: room.messages // recent in-memory chat history for this room only
        }));
        room.broadcast(ws.clientId, { type: 'member_count', count: room.clients.size });

        // Small ephemeral system note in the chat feed — not stored beyond
        // the usual history buffer, just like any other message.
        const joinNote = {
          id: makeMessageId(), system: true, clientId: ws.clientId,
          name: guestName, text: `${guestName} joined the room`, ts: Date.now()
        };
        room.addChatMessage(joinNote);
        room.broadcast(ws.clientId, { type: 'chat', message: joinNote });
        break;
      }

      case 'host_event': {
        const room = rooms.get(ws.roomCode);
        if (!room || !ws.isHost || room.hostId !== ws.clientId) return; // only the current host can drive sync
        room.touch();
        room.lastState = { ...msg.event, updatedAt: Date.now() };
        room.broadcast(ws.clientId, { type: 'sync', event: msg.event });
        break;
      }

      case 'chat_message': {
        const room = rooms.get(ws.roomCode);
        if (!room || !ws.roomCode) return; // must be in a room to chat
        if (!checkRateLimit(ws.chatTimestamps, CHAT_RATE_LIMIT, CHAT_RATE_WINDOW_MS)) {
          if (noteViolation(ws)) return;
          ws.send(JSON.stringify({ type: 'error', transient: true, message: 'You are sending messages too fast — slow down a little.' }));
          return;
        }
        const text = sanitizeText(msg.text);
        if (!text) return; // ignore empty/whitespace-only messages
        room.touch();
        const entry = {
          id: makeMessageId(),
          clientId: room.publicIdFor(ws), // stable across reconnects, unlike ws.clientId
          name: room.nameFor(ws.clientId),
          isHost: ws.isHost && room.hostId === ws.clientId,
          text,
          ts: Date.now()
        };
        room.addChatMessage(entry); // in-memory only, dies with the room
        room.broadcastAll({ type: 'chat', message: entry });
        break;
      }

      case 'leave_room': {
        handleExplicitLeave(ws);
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => cleanupClient(ws));
  ws.on('error', () => cleanupClient(ws));
});

// Intentional "Leave Room" click — close things immediately, no grace period.
function handleExplicitLeave(ws) {
  const room = rooms.get(ws.roomCode);
  if (room) {
    if (ws.isHost && room.hostId === ws.clientId) {
      room.clearGrace();
      room.clearAllGuestTimers();
      // Host ending the room deletes it entirely — messages, names, state,
      // everything in this Room object is discarded right here.
      room.broadcast(ws.clientId, { type: 'room_closed' });
      rooms.delete(room.code);
    } else {
      const name = room.nameFor(ws.clientId);
      room.clients.delete(ws.clientId);
      room.names.delete(ws.clientId);
      if (ws.guestToken) room.guestIdentities.delete(ws.guestToken);
      room.broadcast(ws.clientId, { type: 'member_count', count: room.clients.size });
      const leaveNote = {
        id: makeMessageId(), system: true, clientId: ws.clientId,
        name, text: `${name} left the room`, ts: Date.now()
      };
      room.addChatMessage(leaveNote);
      room.broadcast(ws.clientId, { type: 'chat', message: leaveNote });
    }
  }
  ws.roomCode = null;
  ws.isHost = false;
}

// Socket just dropped (network blip, tab throttled, server restart, etc).
// Guests: remove them, tell the room. Host: give it HOST_GRACE_MS to
// reclaim the room before actually tearing it down.
function cleanupClient(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;

  if (ws.isHost && room.hostId === ws.clientId) {
    room.clients.delete(ws.clientId);
    room.broadcast(ws.clientId, { type: 'host_disconnected' }); // let guests show "reconnecting…" instead of nothing
    room.clearGrace(); // clear any stale timer before setting a new one
    room.hostMissing = true;
    room.graceTimer = setTimeout(() => {
      const stillThere = rooms.get(room.code);
      if (stillThere === room && room.hostMissing) {
        // Grace period expired without the host reclaiming — the room (and
        // every chat message in it) is deleted here.
        room.broadcast(null, { type: 'room_closed' });
        room.clearAllGuestTimers();
        rooms.delete(room.code);
      }
    }, HOST_GRACE_MS);
  } else {
    const name = room.nameFor(ws.clientId);
    const wasPresent = room.clients.has(ws.clientId);
    room.clients.delete(ws.clientId);
    room.names.delete(ws.clientId);

    const identity = ws.guestToken ? room.guestIdentities.get(ws.guestToken) : null;
    if (identity) {
      // Don't announce anything yet — this socket dropping might just be a
      // hard page reload triggered by a video change. Give them
      // GUEST_RECONNECT_GRACE_MS to reconnect with the same guestToken and
      // resume silently. Member count also stays as-is in the meantime so
      // it doesn't flicker down and back up.
      const droppedClientId = ws.clientId;
      identity.leaveTimer = setTimeout(() => {
        // If a new connection already reclaimed this token, identity.clientId
        // will have moved on — in that case this timer is a no-op.
        if (identity.clientId !== droppedClientId) return;
        room.guestIdentities.delete(ws.guestToken);
        room.broadcast(null, { type: 'member_count', count: room.clients.size });
        const leaveNote = {
          id: makeMessageId(), system: true, clientId: droppedClientId,
          name: identity.name, text: `${identity.name} left the room`, ts: Date.now()
        };
        room.addChatMessage(leaveNote);
        room.broadcast(null, { type: 'chat', message: leaveNote });
      }, GUEST_RECONNECT_GRACE_MS);
    } else if (room.clients.size > 0) {
      room.broadcast(ws.clientId, { type: 'member_count', count: room.clients.size });
      if (wasPresent) {
        const leaveNote = {
          id: makeMessageId(), system: true, clientId: ws.clientId,
          name, text: `${name} left the room`, ts: Date.now()
        };
        room.addChatMessage(leaveNote);
        room.broadcast(ws.clientId, { type: 'chat', message: leaveNote });
      }
    }
  }
  ws.roomCode = null;
}

// Ping every 30s to keep connections alive through proxies and to prune dead sockets.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Sweep abandoned rooms periodically (e.g. everyone's tabs are long gone).
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastUsed > ROOM_TTL_MS) {
      room.clearGrace();
      room.clearAllGuestTimers();
      rooms.delete(code);
    }
  }
  // Also prune the per-IP create-room tracker so it doesn't grow forever.
  for (const [ip, timestamps] of ipCreateTimestamps) {
    const fresh = timestamps.filter((t) => now - t <= IP_CREATE_WINDOW_MS);
    if (fresh.length === 0) ipCreateTimestamps.delete(ip);
    else ipCreateTimestamps.set(ip, fresh);
  }
}, 15 * 60 * 1000);

wss.on('close', () => {
  clearInterval(heartbeat);
  clearInterval(sweeper);
});

server.listen(PORT, () => {
  console.log(`YT Pro+ room relay listening on port ${PORT}`);
});
