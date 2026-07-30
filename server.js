"use strict";

const http = require("http");
const crypto = require("crypto");
const {WebSocketServer, WebSocket} = require("ws");

// A hospedagem escolhe a porta pela variável PORT; no computador vale a SUGAR_PORT.
const PORT = Math.max(1024, Math.min(65535, Number(process.env.PORT || process.env.SUGAR_PORT) || 8787));
// Na nuvem é preciso aceitar conexões de fora; no computador, só do próprio túnel.
const HOST = process.env.SUGAR_HOST || (process.env.PORT ? "0.0.0.0" : "127.0.0.1");
const MAX_CLIENTS = 7;
const MAX_MESSAGE_BYTES = 96 * 1024;
const RECONNECT_GRACE_MS = 15000;
const sessions = new Map();
let hostSession = null;
let nextPeerId = 1;

function json(res, status, body) {
  const payload = Buffer.from(JSON.stringify(body, null, 2));
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": payload.length,
    "cache-control": "no-store"
  });
  res.end(payload);
}

function activePlayers() {
  let count = 0;
  for (const client of sessions.values()) {
    if (client.ws && client.ws.readyState === WebSocket.OPEN) count += 1;
  }
  return count;
}

const server = http.createServer((req, res) => {
  if (req.url === "/status" || req.url === "/") {
    return json(res, 200, {
      game: "Sugar Strike",
      online: true,
      room: "Sala única",
      players: activePlayers(),
      capacity: MAX_CLIENTS + 1,
      hasHost: !!hostSession
    });
  }
  json(res, 404, {error: "not_found"});
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false
});

function send(client, message) {
  if (!client || !client.ws || client.ws.readyState !== WebSocket.OPEN) return;
  try {
    client.ws.send(JSON.stringify(message));
  } catch (_) {
    // O evento de fechamento fará a limpeza.
  }
}

function broadcastFromHost(message) {
  for (const client of sessions.values()) {
    if (client.role === "client") send(client, message);
  }
}

function sessionToken() {
  return crypto.randomBytes(24).toString("base64url");
}

function closeRoomAfterHostLeaves() {
  hostSession = null;
  nextPeerId = 1;
  for (const [token, client] of sessions) {
    clearTimeout(client.expiry);
    if (client.role === "client") {
      send(client, {
        net: "host_left",
        message: "O criador saiu; a sala será reiniciada."
      });
      if (client.ws) {
        setTimeout(() => {
          try { client.ws.close(1012, "room_restart"); } catch (_) {}
        }, 250);
      }
    }
    sessions.delete(token);
  }
  console.log("[sala] O criador saiu; a sala foi reiniciada.");
}

function expireClient(client) {
  if (client.ws) return;
  sessions.delete(client.token);
  if (client.role === "host") {
    closeRoomAfterHostLeaves();
    return;
  }
  if (hostSession) {
    send(hostSession, {net: "peer_leave", id: client.id});
  }
  console.log(`[sala] ${client.id} saiu.`);
}

function attach(ws, request) {
  const url = new URL(request.url, "http://localhost");
  const requestedSession = String(url.searchParams.get("session") || "");
  let client = sessions.get(requestedSession);
  let resumed = false;

  if (client) {
    if (client.ws && client.ws.readyState === WebSocket.OPEN) {
      const previousSocket = client.ws;
      client.ws = null;
      try { previousSocket.close(4000, "session_resumed_elsewhere"); } catch (_) {}
    }
    clearTimeout(client.expiry);
    client.expiry = null;
    client.ws = ws;
    resumed = true;
  } else {
    const connectedClients = Array.from(sessions.values()).filter(
      entry => entry.role === "client"
    ).length;
    if (hostSession && connectedClients >= MAX_CLIENTS) {
      ws.send(JSON.stringify({net: "full"}));
      ws.close(1008, "room_full");
      return;
    }
    const role = hostSession ? "client" : "host";
    const token = sessionToken();
    client = {
      token,
      role,
      id: role === "host" ? "host" : `p${nextPeerId++}`,
      ws,
      expiry: null,
      isAlive: true,
      windowStart: Date.now(),
      messageCount: 0
    };
    sessions.set(token, client);
    if (role === "host") hostSession = client;
  }

  ws.sugarClient = client;
  client.isAlive = true;
  send(client, {
    net: "connected",
    role: client.role,
    id: client.id,
    session: client.token,
    resumed
  });

  if (client.role === "client") {
    send(client, {net: "welcome", id: client.id});
    send(hostSession, {net: "peer_join", id: client.id});
  }
  console.log(`[sala] ${client.id} conectado${resumed ? " novamente" : ""}.`);

  ws.on("pong", () => {
    client.isAlive = true;
  });

  ws.on("message", raw => {
    if (raw.length > MAX_MESSAGE_BYTES) return;
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

    if (client.role === "host") {
      broadcastFromHost(message);
    } else if (hostSession) {
      message.from = client.id;
      send(hostSession, message);
    }
  });

  ws.on("close", () => {
    if (client.ws !== ws) return;
    client.ws = null;
    client.expiry = setTimeout(() => expireClient(client), RECONNECT_GRACE_MS);
    console.log(`[sala] ${client.id} perdeu a conexão; aguardando reconexão.`);
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
}, 10000);

function shutdown() {
  clearInterval(heartbeat);
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
  console.log(` Sala local: http://${HOST}:${PORT}/status`);
  console.log(" Aguardando o túnel criar o endereço público...");
  console.log("==============================================");
  console.log("");
});
