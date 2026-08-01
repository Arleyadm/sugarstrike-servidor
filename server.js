"use strict";

const http = require("http");
const crypto = require("crypto");
const {WebSocketServer, WebSocket} = require("ws");

// A hospedagem escolhe a porta pela variável PORT; no computador vale a SUGAR_PORT.
const PORT = Math.max(1024, Math.min(65535, Number(process.env.PORT || process.env.SUGAR_PORT) || 8787));
// Na nuvem é preciso aceitar conexões de fora; no computador, só do próprio túnel.
const HOST = process.env.SUGAR_HOST || (process.env.PORT ? "0.0.0.0" : "127.0.0.1");
const MIN_ROOM_SIZE = 2;
const MAX_ROOM_SIZE = 12;
const MAX_ROOMS = 60;
const MAX_MESSAGE_BYTES = 96 * 1024;
const RECONNECT_GRACE_MS = 15000;
// Uma sala sem ninguém conectado é apagada depois deste tempo.
const EMPTY_ROOM_MS = 120000;
const SWEEP_MS = 10000;

const rooms = new Map();     // id -> sala
const sessions = new Map();  // token -> cliente
// Quem está olhando o saguão sem estar em sala nenhuma ainda.
const watchers = new Set();

function makeRoomId() {
  return crypto.randomBytes(5).toString("hex");
}
function sessionToken() {
  return crypto.randomBytes(24).toString("base64url");
}
// Tira caracteres de controle e os sinais que quebrariam o HTML do jogo.
function clean(value, fallback, max) {
  const source = String(value == null ? "" : value);
  let text = "";
  for (let i = 0; i < source.length; i++) {
    const code = source.charCodeAt(i);
    if (code < 32 || code === 127) continue;
    if (code === 60 || code === 62) continue;
    text += source.charAt(i);
  }
  text = text.trim();
  return (text || fallback).slice(0, max);
}
function json(res, status, body) {
  const payload = Buffer.from(JSON.stringify(body, null, 2));
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": payload.length,
    "cache-control": "no-store",
    // O jogo roda de file:// dentro do WebView, então a origem chega como "null".
    "access-control-allow-origin": "*"
  });
  res.end(payload);
}

function connectedCount(room) {
  let count = 0;
  for (const client of room.clients.values()) {
    if (client.ws && client.ws.readyState === WebSocket.OPEN) count += 1;
  }
  return count;
}

function roomSummary(room) {
  return {
    id: room.id,
    name: room.name,
    map: room.map,
    mode: room.mode,
    players: connectedCount(room),
    max: room.max,
    status: room.status,
    locked: room.locked
  };
}

function roomList() {
  const list = [];
  for (const room of rooms.values()) list.push(roomSummary(room));
  list.sort((a, b) => {
    if (a.status !== b.status) return a.status === "lobby" ? -1 : 1;
    return b.players - a.players;
  });
  return list;
}

function send(client, message) {
  if (!client || !client.ws || client.ws.readyState !== WebSocket.OPEN) return;
  try {
    client.ws.send(JSON.stringify(message));
  } catch (_) {
    // O evento de fechamento fará a limpeza.
  }
}

function sendToRoom(room, message, skip) {
  for (const client of room.clients.values()) {
    if (client !== skip) send(client, message);
  }
}

function hostOf(room) {
  return room.hostToken ? room.clients.get(room.hostToken) || null : null;
}

// A lista do saguão chega pelo próprio WebSocket: o app roda de file:// e
// ali o navegador proíbe buscar outra origem por HTTP.
function broadcastRoomList() {
  const payload = JSON.stringify({net: "rooms", rooms: roomList()});
  for (const watcher of watchers) {
    if (!watcher.ws || watcher.ws.readyState !== WebSocket.OPEN) continue;
    try { watcher.ws.send(payload); } catch (_) {}
  }
}

function announceRoom(room) {
  sendToRoom(room, {net: "room_update", room: roomSummary(room)});
  broadcastRoomList();
}

function touchEmptiness(room) {
  room.emptySince = connectedCount(room) === 0 ? Date.now() : 0;
}

function dropRoom(room, why) {
  rooms.delete(room.id);
  for (const client of room.clients.values()) {
    clearTimeout(client.expiry);
    sessions.delete(client.token);
    if (client.ws) {
      try { client.ws.close(1000, why || "room_closed"); } catch (_) {}
    }
  }
  room.clients.clear();
  broadcastRoomList();
  console.log(`[sala ${room.id}] removida (${why || "vazia"}).`);
}

// O dono saiu: promove quem está há mais tempo na sala e devolve todos ao saguão.
function promoteNewHost(room) {
  let candidate = null;
  for (const client of room.clients.values()) {
    if (!client.ws || client.ws.readyState !== WebSocket.OPEN) continue;
    if (!candidate || client.joinedAt < candidate.joinedAt) candidate = client;
  }
  if (!candidate) {
    room.hostToken = null;
    room.status = "lobby";
    touchEmptiness(room);
    return;
  }
  candidate.role = "host";
  candidate.id = "host";
  room.hostToken = candidate.token;
  room.status = "lobby";
  send(candidate, {net: "promoted", id: "host", room: roomSummary(room)});
  for (const client of room.clients.values()) {
    if (client === candidate) continue;
    send(client, {net: "host_changed", room: roomSummary(room)});
  }
  console.log(`[sala ${room.id}] novo dono: ${candidate.name || candidate.token.slice(0, 6)}.`);
}

function expireClient(client) {
  if (client.ws) return;
  const room = rooms.get(client.roomId);
  sessions.delete(client.token);
  if (!room) return;
  room.clients.delete(client.token);
  if (room.hostToken === client.token) {
    room.hostToken = null;
    promoteNewHost(room);
  } else {
    const host = hostOf(room);
    if (host) send(host, {net: "peer_leave", id: client.id});
  }
  touchEmptiness(room);
  announceRoom(room);
  console.log(`[sala ${room.id}] ${client.id} saiu.`);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,OPTIONS",
      "access-control-allow-headers": "content-type"
    });
    res.end();
    return;
  }
  if (url.pathname === "/rooms") {
    return json(res, 200, {rooms: roomList(), capacity: MAX_ROOMS});
  }
  if (url.pathname === "/status" || url.pathname === "/") {
    let players = 0;
    for (const room of rooms.values()) players += connectedCount(room);
    return json(res, 200, {
      game: "Sugar Strike",
      online: true,
      rooms: rooms.size,
      players: players,
      maxRooms: MAX_ROOMS,
      maxPerRoom: MAX_ROOM_SIZE
    });
  }
  json(res, 404, {error: "not_found"});
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false
});

function createRoom(params) {
  if (rooms.size >= MAX_ROOMS) return null;
  const id = makeRoomId();
  const room = {
    id: id,
    name: clean(params.get("name"), "SALA DOCE", 24).toUpperCase(),
    map: clean(params.get("map"), "village", 16),
    mode: clean(params.get("mode"), "deathmatch", 16),
    max: Math.max(MIN_ROOM_SIZE, Math.min(MAX_ROOM_SIZE, Number(params.get("max")) || 8)),
    locked: params.get("locked") === "1",
    status: "lobby",
    hostToken: null,
    clients: new Map(),
    nextPeerId: 1,
    emptySince: Date.now(),
    createdAt: Date.now()
  };
  rooms.set(id, room);
  broadcastRoomList();
  console.log(`[sala ${id}] criada: ${room.name} (${room.map}, até ${room.max}).`);
  return room;
}

function attach(ws, request) {
  const url = new URL(request.url, "http://localhost");
  const params = url.searchParams;

  // Espectador do saguão: só quer a lista de salas, não entra em nenhuma.
  if (params.get("lobby") === "1") {
    const watcher = {ws: ws, isAlive: true};
    watchers.add(watcher);
    ws.on("pong", () => { watcher.isAlive = true; });
    ws.on("close", () => { watchers.delete(watcher); });
    try {
      ws.send(JSON.stringify({net: "rooms", rooms: roomList()}));
    } catch (_) {}
    return;
  }

  const requestedSession = String(params.get("session") || "");
  let client = sessions.get(requestedSession);
  let room = null;
  let resumed = false;

  if (client && rooms.has(client.roomId)) {
    room = rooms.get(client.roomId);
    if (client.ws && client.ws.readyState === WebSocket.OPEN) {
      const previous = client.ws;
      client.ws = null;
      try { previous.close(4000, "session_resumed_elsewhere"); } catch (_) {}
    }
    clearTimeout(client.expiry);
    client.expiry = null;
    client.ws = ws;
    resumed = true;
  } else {
    if (client) sessions.delete(client.token);
    client = null;
    if (params.get("create") === "1") {
      room = createRoom(params);
      if (!room) {
        ws.send(JSON.stringify({net: "no_room", reason: "SERVIDOR_LOTADO"}));
        ws.close(1008, "server_full");
        return;
      }
    } else {
      room = rooms.get(String(params.get("room") || ""));
      if (!room) {
        ws.send(JSON.stringify({net: "no_room", reason: "SALA_NAO_EXISTE"}));
        ws.close(1008, "no_room");
        return;
      }
      if (connectedCount(room) >= room.max) {
        ws.send(JSON.stringify({net: "full"}));
        ws.close(1008, "room_full");
        return;
      }
    }
    const isHost = !room.hostToken;
    const token = sessionToken();
    client = {
      token: token,
      roomId: room.id,
      role: isHost ? "host" : "client",
      id: isHost ? "host" : `p${room.nextPeerId++}`,
      name: clean(params.get("player"), "", 14),
      ws: ws,
      expiry: null,
      isAlive: true,
      joinedAt: Date.now(),
      windowStart: Date.now(),
      messageCount: 0
    };
    sessions.set(token, client);
    room.clients.set(token, client);
    if (isHost) room.hostToken = token;
  }

  ws.sugarClient = client;
  client.isAlive = true;
  touchEmptiness(room);

  send(client, {
    net: "connected",
    role: client.role,
    id: client.id,
    session: client.token,
    resumed: resumed,
    room: roomSummary(room)
  });

  if (client.role === "client") {
    const host = hostOf(room);
    send(client, {net: "welcome", id: client.id});
    if (host) send(host, {net: "peer_join", id: client.id});
  }
  announceRoom(room);
  console.log(`[sala ${room.id}] ${client.id} conectado${resumed ? " novamente" : ""}.`);

  ws.on("pong", () => {
    client.isAlive = true;
  });

  ws.on("message", raw => {
    if (raw.length > MAX_MESSAGE_BYTES) return;
    const currentRoom = rooms.get(client.roomId);
    if (!currentRoom) return;
    const now = Date.now();
    if (now - client.windowStart >= 1000) {
      client.windowStart = now;
      client.messageCount = 0;
    }
    client.messageCount += 1;
    if (client.messageCount > 180) {
      ws.close(1008, "rate_limit");
      return;
    }
    let message;
    try {
      message = JSON.parse(raw.toString("utf8"));
    } catch (_) {
      return;
    }
    if (!message || typeof message !== "object" || Array.isArray(message)) return;

    // Avisos de vitrine: só o dono muda o que a lista pública mostra.
    if (message.ctl) {
      if (currentRoom.hostToken !== client.token) return;
      if (message.ctl === "status") {
        currentRoom.status = message.status === "playing" ? "playing" : "lobby";
      } else if (message.ctl === "config") {
        currentRoom.map = clean(message.map, currentRoom.map, 16);
        currentRoom.mode = clean(message.mode, currentRoom.mode, 16);
        currentRoom.max = Math.max(
          MIN_ROOM_SIZE,
          Math.min(MAX_ROOM_SIZE, Number(message.max) || currentRoom.max)
        );
      }
      announceRoom(currentRoom);
      return;
    }

    if (currentRoom.hostToken === client.token) {
      sendToRoom(currentRoom, message, client);
    } else {
      const host = hostOf(currentRoom);
      if (host) {
        message.from = client.id;
        send(host, message);
      }
    }
  });

  ws.on("close", () => {
    if (client.ws !== ws) return;
    client.ws = null;
    client.expiry = setTimeout(() => expireClient(client), RECONNECT_GRACE_MS);
    const currentRoom = rooms.get(client.roomId);
    if (currentRoom) {
      touchEmptiness(currentRoom);
      announceRoom(currentRoom);
    }
    console.log(`[sala ${client.roomId}] ${client.id} perdeu a conexão; aguardando reconexão.`);
  });
}

server.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url, "http://localhost");
  if (url.pathname !== "/game") {
    socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, ws => attach(ws, request));
});

const heartbeat = setInterval(() => {
  for (const client of sessions.values()) {
    if (!client.ws || client.ws.readyState !== WebSocket.OPEN) continue;
    if (!client.isAlive) {
      client.ws.terminate();
      continue;
    }
    client.isAlive = false;
    client.ws.ping();
  }
  for (const watcher of Array.from(watchers)) {
    if (!watcher.ws || watcher.ws.readyState !== WebSocket.OPEN) {
      watchers.delete(watcher);
      continue;
    }
    if (!watcher.isAlive) {
      watcher.ws.terminate();
      watchers.delete(watcher);
      continue;
    }
    watcher.isAlive = false;
    watcher.ws.ping();
  }
}, 10000);

// Salas vazias somem sozinhas depois de 2 minutos sem ninguém conectado.
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const room of Array.from(rooms.values())) {
    if (connectedCount(room) > 0) {
      room.emptySince = 0;
      continue;
    }
    if (!room.emptySince) room.emptySince = now;
    if (now - room.emptySince >= EMPTY_ROOM_MS) dropRoom(room, "2 min vazia");
  }
}, SWEEP_MS);

function shutdown() {
  clearInterval(heartbeat);
  clearInterval(sweeper);
  for (const client of sessions.values()) {
    clearTimeout(client.expiry);
    if (client.ws) {
      try { client.ws.close(1001, "server_shutdown"); } catch (_) {}
    }
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

server.listen(PORT, HOST, () => {
  console.log("");
  console.log("==============================================");
  console.log(" SUGAR STRIKE - SERVIDOR ONLINE ATIVO");
  console.log(` Salas: http://${HOST}:${PORT}/rooms`);
  console.log(` Estado: http://${HOST}:${PORT}/status`);
  console.log(` Ate ${MAX_ROOMS} salas, ${MAX_ROOM_SIZE} jogadores por sala.`);
  console.log("==============================================");
  console.log("");
});
