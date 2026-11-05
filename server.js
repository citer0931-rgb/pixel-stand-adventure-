// Signaling-сервер для игры. Только комнаты и пересылка WebRTC-сообщений (offer/answer/ICE).
// Игровой трафик и логика сюда не попадают — они идут по WebRTC DataChannel.
// Запуск: npm install ws && node server.js   (index.html должен лежать рядом)
const http = require('http'), fs = require('fs'), os = require('os'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // без похожих символов: I L O 0 1
const WAIT_MS = +process.env.WAIT_MS || 10 * 60 * 1000; // комната без второго игрока живёт 10 минут
const MAX_ROOMS = 500, MAX_MSG = 16 * 1024;
const cleanName = t => (typeof t === 'string' ? t : '').replace(/[\u0000-\u001f<>"'&\\]/g, '').trim().slice(0, 16) || 'Игрок';
const rooms = new Map(); // код -> { h: сокет хоста, g: сокет гостя | null, tm: таймер }

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') { res.end('ok'); return; }
  if (u === '/' || u === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (e, b) => {
      if (e) { res.writeHead(404); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(b);
    });
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });
const send = (s, o) => { try { s && s.readyState === 1 && s.send(JSON.stringify(o)); } catch (e) {} };

function newCode() {
  for (let n = 0; n < 50; n++) {
    let c = '';
    for (let i = 0; i < 4; i++) c += ALPHA[crypto.randomInt(ALPHA.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function arm(c, r) { clearTimeout(r.tm); r.tm = setTimeout(() => expire(c), WAIT_MS); }
function expire(c) { // никто не вошёл за WAIT_MS
  const r = rooms.get(c); if (!r) return;
  rooms.delete(c); r.h.room = null; send(r.h, { t: 'err', e: 'timeout' });
}
function leave(ws) {
  const c = ws.room; if (!c) return; ws.room = null;
  const r = rooms.get(c); if (!r) return;
  if (ws === r.h) { // хост ушёл — комната закрыта
    clearTimeout(r.tm); rooms.delete(c);
    if (r.g) { r.g.room = null; send(r.g, { t: 'left' }); }
  } else if (ws === r.g) { // гость ушёл — комната снова ждёт второго
    r.g = null; send(r.h, { t: 'left' }); arm(c, r);
  }
}

wss.on('connection', ws => {
  ws.alive = true; ws.room = null; ws.bad = 0; ws.n = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {});
  ws.on('close', () => leave(ws));
  ws.on('message', (data, isBinary) => {
    if (isBinary || ++ws.n > 200) return ws.terminate(); // не текст / слишком много сообщений
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return ws.terminate(); }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'create') {
      if (ws.room) return send(ws, { t: 'err', e: 'bad' });
      const c = rooms.size < MAX_ROOMS && newCode();
      if (!c) return send(ws, { t: 'err', e: 'busy' });
      const r = { h: ws, g: null, tm: 0, pub: m.pub !== 0 && m.pub !== false, name: cleanName(m.name) }; rooms.set(c, r); ws.room = c; arm(c, r);
      send(ws, { t: 'created', code: c });
    } else if (m.t === 'join') {
      if (ws.room) return send(ws, { t: 'err', e: 'bad' });
      const c = typeof m.code === 'string' ? m.code.toUpperCase() : '';
      const r = /^[A-Z0-9]{4}$/.test(c) && rooms.get(c);
      if (!r) { if (++ws.bad > 10) ws.terminate(); else send(ws, { t: 'err', e: 'none' }); return; }
      if (r.g) return send(ws, { t: 'err', e: 'full' });
      r.g = ws; ws.room = c; clearTimeout(r.tm);
      send(ws, { t: 'joined' }); send(r.h, { t: 'peer' });
    } else if (m.t === 'list') { // открытые комнаты, в которых ждут второго игрока
      const list = [];
      for (const [code, r] of rooms) if (r.pub && !r.g && list.length < 30) list.push({ code, name: r.name });
      send(ws, { t: 'rooms', list });
    } else if (m.t === 'sig') { // пересылаем только offer/answer/ICE второму игроку
      const r = rooms.get(ws.room), d = m.d; if (!r || !d || typeof d !== 'object') return;
      const o = ws === r.h ? r.g : r.h;
      if (d.sdp && typeof d.sdp === 'object') send(o, { t: 'sig', d: { sdp: { type: String(d.sdp.type), sdp: String(d.sdp.sdp) } } });
      else if (d.c && typeof d.c === 'object') send(o, { t: 'sig', d: { c: d.c } });
    }
  });
});

// keep-alive (чтобы прокси не рвали сокет) + сброс счётчика сообщений + удаление «мёртвых» сокетов
setInterval(() => wss.clients.forEach(ws => {
  if (!ws.alive) return ws.terminate();
  ws.alive = false; ws.n = 0; try { ws.ping(); } catch (e) {}
}), 15000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Signaling-сервер запущен на порту ' + PORT);
  for (const l of Object.values(os.networkInterfaces()))
    for (const a of l) if (a.family === 'IPv4' && !a.internal) console.log('Открой на телефонах: http://' + a.address + ':' + PORT);
});
