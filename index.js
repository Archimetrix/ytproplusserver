// ─────────────────────────────────────────────────────────────────────────
// YouTube Pro+ — Watch Party Room Relay Server
//
// Plain WebSocket relay. Rooms live in memory only (no database).
// The server never looks at video content — it just forwards small JSON
// messages from the host to everyone else in the same room.
// ─────────────────────────────────────────────────────────────────────────

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I ambiguity
const ROOM_TTL_MS = 6 * 60 * 60 * 1000; // auto-clean rooms idle > 6h

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

class Room {
  constructor(code, hostId, hostWs) {
    this.code = code;
    this.hostId = hostId;
    this.clients = new Map(); // clientId -> ws
    this.clients.set(hostId, hostWs);
    this.lastState = null; // { videoId, time, playing, updatedAt }
    this.touch();
  }
  touch() {
    this.lastUsed = Date.now();
  }
  broadcast(fromId, payload) {
    const msg = JSON.stringify(payload);
    for (const [id, ws] of this.clients) {
      if (id === fromId) continue;
      if (ws.readyState === ws.OPEN) ws.send(msg);
    }
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

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.clientId = makeClientId();
  ws.roomCode = null;
  ws.isHost = false;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed input
    }

    switch (msg.type) {
      case 'create_room': {
        const code = makeRoomCode();
        const room = new Room(code, ws.clientId, ws);
        rooms.set(code, room);
        ws.roomCode = code;
        ws.isHost = true;
        ws.send(JSON.stringify({ type: 'room_created', code, clientId: ws.clientId }));
        break;
      }

      case 'join_room': {
        const code = (msg.code || '').toUpperCase().trim();
        const room = rooms.get(code);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code and try again.' }));
          return;
        }
        room.clients.set(ws.clientId, ws);
        room.touch();
        ws.roomCode = code;
        ws.isHost = false;
        ws.send(JSON.stringify({
          type: 'room_joined',
          code,
          clientId: ws.clientId,
          memberCount: room.clients.size,
          state: room.lastState // so the new viewer can jump to the current spot
        }));
        room.broadcast(ws.clientId, { type: 'member_count', count: room.clients.size });
        break;
      }

      case 'host_event': {
        const room = rooms.get(ws.roomCode);
        if (!room || !ws.isHost) return; // only the host can drive sync
        room.touch();
        room.lastState = { ...msg.event, updatedAt: Date.now() };
        room.broadcast(ws.clientId, { type: 'sync', event: msg.event });
        break;
      }

      case 'leave_room': {
        cleanupClient(ws);
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => cleanupClient(ws));
  ws.on('error', () => cleanupClient(ws));
});

function cleanupClient(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;

  room.clients.delete(ws.clientId);

  if (ws.isHost) {
    // Host left — tell everyone the party's over and drop the room.
    room.broadcast(ws.clientId, { type: 'room_closed' });
    rooms.delete(ws.roomCode);
  } else if (room.clients.size > 0) {
    room.broadcast(ws.clientId, { type: 'member_count', count: room.clients.size });
  } else if (room.clients.size === 0 && !room.hostId) {
    rooms.delete(ws.roomCode);
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

// Sweep abandoned rooms periodically (e.g. host's tab crashed without a close event).
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastUsed > ROOM_TTL_MS) rooms.delete(code);
  }
}, 15 * 60 * 1000);

wss.on('close', () => {
  clearInterval(heartbeat);
  clearInterval(sweeper);
});

server.listen(PORT, () => {
  console.log(`YT Pro+ room relay listening on port ${PORT}`);
});
