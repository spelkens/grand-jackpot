// Le Grand Jackpot : sert le jeu et relaie le chat + les cadeaux du live TikTok vers la page.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import * as TL from "tiktok-live-connector";

const dir = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const Conn = TL.TikTokLiveConnection || TL.WebcastPushConnection || TL.default?.TikTokLiveConnection || TL.default?.WebcastPushConnection;

// Clé Euler Stream facultative (augmente les limites gratuites)
if (process.env.EULER_API_KEY && TL.SignConfig) TL.SignConfig.apiKey = process.env.EULER_API_KEY;

const page = () => fs.readFileSync(path.join(dir, "public", "index.html"), "utf8")
  .replace("<!--SERVER-->", "<script>window.JACKPOT_SERVER=1</script>");

const server = http.createServer((req, res) => {
  if (req.url.startsWith("/debug")) {
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
    res.end(JSON.stringify({ liveUser, liveOk, status: lastStatus.msg, clients: wss.clients.size, events: DBG.counts, lastChats: DBG.chats, errors: DBG.errors, since: DBG.since }, null, 2));
    return;
  }
  if (req.url.startsWith("/health")) { res.writeHead(200, { "Content-Type": "text/plain" }); res.end("ok"); return; }
  const html = `<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#071233"></head><body>${page()}</body></html>`;
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
  res.end(html);
});

const DBG = { counts: {}, chats: [], errors: [], since: null };
const dbgErr = m => { DBG.errors.unshift(new Date().toISOString().slice(11, 19) + " " + String(m).slice(0, 300)); DBG.errors.length = Math.min(DBG.errors.length, 15); };
const EV = TL.WebcastEvent || TL.default?.WebcastEvent || {};
const CHAT = EV.CHAT || "chat", GIFT = EV.GIFT || "gift", END = EV.STREAM_END || "streamEnd";
const wss = new WebSocketServer({ server, path: "/ws" });
let lastTry = 0;
let live = null, liveUser = null, liveOk = false, lastStatus = { t: "status", ok: false, msg: "Pas encore connecté au live" };

function broadcast(o) {
  const s = JSON.stringify(o);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}
function status(ok, msg) { liveOk = ok; lastStatus = { t: "status", ok, msg }; broadcast(lastStatus); console.log("[status]", msg); }

const who = d => ({
  id: d.user?.uniqueId || d.uniqueId || d.user?.userId || d.userId || "?",
  nick: d.user?.nickname || d.nickname || d.user?.uniqueId || d.uniqueId || "?"
});

async function connectLive(username) {
  username = String(username || "").trim().replace(/^@/, "");
  if (!username) return status(false, "Écris ton pseudo TikTok");
  if (live && liveUser === username && liveOk) return status(true, "Connecté au live de @" + username);
  const wait = 30000 - (Date.now() - lastTry);            // évite de griller la limite gratuite d'essais
  if (wait > 0) return status(false, `Patiente ${Math.ceil(wait / 1000)} s avant de réessayer`);
  lastTry = Date.now();
  try { live?.disconnect?.(); } catch {}
  live = null; liveUser = username;
  status(false, "Connexion au live de @" + username + "…");
  const opts = { processInitialData: false, enableExtendedGiftInfo: false, fetchRoomInfoOnConnect: true };
  if (process.env.EULER_API_KEY) opts.signApiKey = process.env.EULER_API_KEY;
  const conn = new Conn(username, opts);
  DBG.counts = {}; DBG.chats = []; DBG.since = new Date().toISOString();
  const emit0 = conn.emit.bind(conn);                      // compte tous les événements reçus (diagnostic)
  conn.emit = (name, ...a) => { DBG.counts[name] = (DBG.counts[name] || 0) + 1; return emit0(name, ...a); };

  const onChat = d => {
    const m = { t: "chat", ...who(d), text: d.comment ?? d.content ?? "" };
    DBG.chats.unshift(m.nick + " : " + m.text); DBG.chats.length = Math.min(DBG.chats.length, 15);
    broadcast(m);
  };
  conn.on(CHAT, onChat);
  if (CHAT !== "chat") conn.on("chat", onChat);
  conn.on(GIFT, d => {
    const type = d.giftDetails?.giftType ?? d.giftType;
    if (type === 1 && !d.repeatEnd) return;            // série de cadeaux pas encore finie
    const unit = d.giftDetails?.diamondCount ?? d.diamondCount ?? d.extendedGiftInfo?.diamond_count ?? 1;
    const count = d.repeatCount || 1;
    broadcast({ t: "gift", ...who(d), diamonds: Math.max(1, unit * count), name: d.giftDetails?.giftName || d.giftName || "cadeau" });
  });
  conn.on(END, () => status(false, "Le live est terminé"));
  conn.on("disconnected", () => { if (live === conn) status(false, "Déconnecté du live. Appuie sur Connecter pour réessayer."); });
  conn.on("error", e => { console.error("[tiktok]", e?.message || e); dbgErr(e?.message || e?.info || JSON.stringify(e)); });

  try {
    await conn.connect();
    live = conn;
    status(true, "Connecté au live de @" + username);
  } catch (e) {
    const m = String(e?.message || e); dbgErr(m);
    const hint = /offline|not.*live|LIVE has ended|isn't online/i.test(m) ? "ton live n'est pas lancé" :
                 /rate|limit|429/i.test(m) ? "trop d'essais, réessaie dans 1 minute" : /Business plan/i.test(m) ? "service de connexion limité, réessaie dans 1 minute" : m.slice(0, 120);
    status(false, "Connexion impossible : " + hint);
  }
}

wss.on("connection", ws => {
  ws.send(JSON.stringify(lastStatus));
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "connect") connectLive(m.username);
  });
});

server.listen(PORT, () => console.log("Grand Jackpot prêt sur le port " + PORT));
