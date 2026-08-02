"use strict";

const {spawn} = require("child_process");
const WebSocket = require("ws");

const PORT = 18987;
const base = `ws://127.0.0.1:${PORT}/game`;
const server = spawn(process.execPath, ["server.js"], {
  cwd: __dirname,
  env: Object.assign({}, process.env, {
    PORT: String(PORT)
  }),
  stdio: ["ignore", "pipe", "pipe"]
});

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function connect(query) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(base + "?" + query);
    const messages = [];
    socket.on("message", raw => messages.push(JSON.parse(raw.toString("utf8"))));
    socket.once("open", () => resolve({socket, messages}));
    socket.once("error", reject);
  });
}

async function take(client, predicate, timeout = 1800) {
  const until = Date.now() + timeout;
  while (Date.now() < until) {
    const index = client.messages.findIndex(predicate);
    if (index >= 0) return client.messages.splice(index, 1)[0];
    await delay(15);
  }
  throw new Error("Mensagem esperada não chegou");
}

async function run() {
  await delay(350);
  const host = await connect("create=1&name=TESTE&mode=team&max=2&spectators=1&player=HOST&identity=host-test");
  const hostConnected = await take(host, message => message.net === "connected");
  const roomId = hostConnected.room.id;

  const guest = await connect(`room=${roomId}&player=GUEST&identity=guest-test`);
  const guestConnected = await take(guest, message => message.net === "connected");
  if (guestConnected.team === hostConnected.team) throw new Error("Equipes não foram equilibradas");

  const spectator = await connect(`room=${roomId}&player=WATCH&identity=watch-test&spectator=1`);
  const spectatorConnected = await take(spectator, message => message.net === "connected");
  if (!spectatorConnected.spectator) throw new Error("Entrada de espectador não foi preservada");
  if (spectatorConnected.room.players !== 2 || spectatorConnected.room.spectators !== 1) {
    throw new Error("Contagem de jogadores/espectadores incorreta");
  }

  spectator.socket.send(JSON.stringify({t: "shoot", w: 3, d: [0, 0, -1]}));
  await delay(180);
  if (host.messages.some(message => message.t === "shoot" && message.from === spectatorConnected.id)) {
    throw new Error("Espectador conseguiu enviar tiro");
  }

  guest.socket.send(JSON.stringify({t: "hello", weapon: 3, version: "1.7.0"}));
  const hello = await take(host, message => message.t === "hello" && message.from === guestConnected.id);
  if (hello.team !== guestConnected.team || hello.spectator) throw new Error("Metadados oficiais não chegaram ao host");

  guest.socket.send(JSON.stringify({t: "state", wep: 3, x: 0, y: 0, z: 0}));
  await take(host, message => message.t === "state" && message.from === guestConnected.id);
  guest.socket.send(JSON.stringify({t: "shoot", w: 3, d: [0, 0, -1]}));
  const shot = await take(host, message => message.t === "shoot" && message.from === guestConnected.id);
  if (shot.serverMag !== 6) throw new Error("Munição autoritativa não foi descontada");

  guest.socket.send(JSON.stringify({t: "reload_start", w: 3}));
  await take(host, message => message.t === "reload_start" && message.from === guestConnected.id);
  await delay(2140);
  guest.socket.send(JSON.stringify({t: "reload_commit", w: 3}));
  const reload = await take(host, message => message.t === "reload_commit" && message.from === guestConnected.id);
  if (reload.mag !== 7 || reload.res !== 34) throw new Error("Recarga autoritativa incorreta");

  host.socket.close();
  guest.socket.close();
  spectator.socket.close();
  console.log("OK spectator/team/reload protocol");
}

run().then(() => {
  server.kill();
}).catch(error => {
  server.kill();
  console.error(error.stack || error);
  process.exitCode = 1;
});
