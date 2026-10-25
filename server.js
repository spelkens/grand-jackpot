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
  if (req.url.startsWith("/health")) { res.writeHead(200, { "Content-Type": "text/plain" }); res.end("ok"); return; }
  const html = `<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#071233"></head><body>${page()}</body></html>`;
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
  res.end(html);
});

const wss = new WebSocketServer({ server, path: "/ws" });
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
  try { live?.disconnect?.(); } catch {}
  live = null; liveUser = username;
  status(false, "Connexion au live de @" + username + "…");
  const conn = new Conn(username, { processInitialData: false, enableExtendedGiftInfo: true });

  conn.on("chat", d => broadcast({ t: "chat", ...who(d), text: d.comment || "" }));
  conn.on("gift", d => {
    const type = d.giftDetails?.giftType ?? d.giftType;
    if (type === 1 && !d.repeatEnd) return;            // série de cadeaux pas encore finie
    const unit = d.giftDetails?.diamondCount ?? d.diamondCount ?? d.extendedGiftInfo?.diamond_count ?? 1;
    const count = d.repeatCount || 1;
    broadcast({ t: "gift", ...who(d), diamonds: Math.max(1, unit * count), name: d.giftDetails?.giftName || d.giftName || "cadeau" });
  });
  conn.on("streamEnd", () => status(false, "Le live est terminé"));
  conn.on("disconnected", () => { if (live === conn) status(false, "Déconnecté du live. Appuie sur Connecter pour réessayer."); });
  conn.on("error", e => console.error("[tiktok]", e?.message || e));

  try {
    await conn.connect();
    live = conn;
    status(true, "Connecté au live de @" + username);
  } catch (e) {
    const m = String(e?.message || e);
    const hint = /offline|not.*live|LIVE has ended|isn't online/i.test(m) ? "ton live n'est pas lancé" :
                 /rate|limit|429/i.test(m) ? "trop d'essais, réessaie dans 1 minute" : m.slice(0, 120);
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
