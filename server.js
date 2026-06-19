const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const app = express();
const RESERVED_ROOM_COUNT = 8;

app.get('/health', (req, res) => {
  res.status(200).send({ status: 'healthy', timestamp: Date.now() });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map();
const reservedRooms = new Set();

function randomRoomCode() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  return Array.from({ length: 12 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

function ensureReservedRooms() {
  while (reservedRooms.size < RESERVED_ROOM_COUNT) {
    reservedRooms.add(randomRoomCode());
  }
}

ensureReservedRooms();

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log(`[Socket] Terminating inactive connection: ${ws.id || 'anonymous'}`);
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

const roomMaintenanceInterval = setInterval(() => {
  ensureReservedRooms();
}, 60000);

const ghostCallInterval = setInterval(() => {
  console.log('[Ghost Call] Dispatching synthetic frame load to connection pools...');
  const ghostPayload = JSON.stringify({
    type: 'ping',
    timestamp: Date.now()
  });

  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(ghostPayload);
    }
  });
}, 600000);

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.roomCode = null;
  ws.isHost = false;
  ws.id = Math.random().toString(36).substring(2, 8);

  console.log(`[Socket] New connection authenticated: ${ws.id}`);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'pong' || data.type === 'ping') {
        return;
      }

      console.log(`[Signal] Received packet type '${data.type}' from ${ws.id}`);

      switch (data.type) {
        case 'create': {
          const { code, nickname } = data;
          if (!code) return;

          ws.roomCode = code;
          ws.isHost = true;
          ws.nickname = normalizeNickname(nickname);
          rooms.set(code, { host: ws, guest: null });
          reservedRooms.delete(code);

          console.log(`[Room] Allocation complete for code: ${code} (Host: ${ws.id})`);
          ws.send(JSON.stringify({ type: 'created', code, nickname: ws.nickname }));
          break;
        }

        case 'join': {
          const { code, nickname } = data;
          if (!code) return;

          const room = rooms.get(code);
          if (!room) {
            console.log(`[Room] Fail to route: Room ${code} does not exist.`);
            ws.send(JSON.stringify({ type: 'room-closed', reason: 'not-found' }));
            return;
          }

          if (room.guest) {
            console.log(`[Room] Fail to route: Room ${code} is full.`);
            ws.send(JSON.stringify({ type: 'room-closed', reason: 'room-full' }));
            return;
          }

          ws.roomCode = code;
          ws.isHost = false;
          ws.nickname = normalizeNickname(nickname);
          room.guest = ws;

          console.log(`[Room] Guest ${ws.id} successfully joined room: ${code}`);
          if (room.host && room.host.readyState === WebSocket.OPEN) {
            room.host.send(JSON.stringify({ type: 'peer-joined', nickname: ws.nickname }));
          }
          ws.send(JSON.stringify({ type: 'joined', code, nickname: ws.nickname }));
          break;
        }

        case 'offer':
        case 'answer':
        case 'ice-candidate': {
          const room = rooms.get(ws.roomCode);
          if (room) {
            const target = ws.isHost ? room.guest : room.host;
            if (target && target.readyState === WebSocket.OPEN) {
              target.send(JSON.stringify(data));
            }
          }
          break;
        }

        case 'hang-up': {
          handleRoomTeardown(ws.roomCode);
          break;
        }

        default:
          console.log(`[Warning] Unknown layout packet received: ${data.type}`);
      }
    } catch (err) {
      console.error(`[Error] Packet structural mutation on socket ${ws.id}:`, err.message);
    }
  });

  ws.on('close', () => {
    console.log(`[Socket] Connection severed: ${ws.id}`);
    if (ws.roomCode) {
      handleRoomTeardown(ws.roomCode);
    }
  });
});

function handleRoomTeardown(roomCode) {
  if (!roomCode) return;
  const room = rooms.get(roomCode);

  if (room) {
    console.log(`[Room] Executing state teardown for room: ${roomCode}`);
    const teardownPayload = JSON.stringify({ type: 'room-closed' });

    if (room.host && room.host.readyState === WebSocket.OPEN) {
      room.host.send(teardownPayload);
      room.host.roomCode = null;
    }
    if (room.guest && room.guest.readyState === WebSocket.OPEN) {
      room.guest.send(teardownPayload);
      room.guest.roomCode = null;
    }

    rooms.delete(roomCode);
    reservedRooms.add(roomCode);
  }
}

function normalizeNickname(nickname) {
  const value = String(nickname || '').trim();
  if (!value) {
    return 'guest';
  }
  return value.slice(0, 24);
}

server.on('close', () => {
  clearInterval(heartbeatInterval);
  clearInterval(ghostCallInterval);
  clearInterval(roomMaintenanceInterval);
});

server.listen(PORT, () => {
  console.log(`====================================================`);
  console.log(` Signaling Server Running on Port : ${PORT}`);
  console.log(` Ghost Call Engine Active (10m Intervals)`);
  console.log(`====================================================`);
});
