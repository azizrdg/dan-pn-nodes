"use strict";

require("dotenv").config();
const express = require("express");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");

const logger = require("./logger");
const db = require("./db");
const { XrayGrpcClient } = require("./xrayGrpc");
const { buildVlessLink, buildHysteria2Link } = require("./linkBuilder");
const { startHysteriaTrafficPoller } = require("./hysteriaTrafficPoller");

const {
  PORT = "8443",
  NODE_API_SECRET,
  XRAY_API_ADDRESS = "127.0.0.1:10085",
  REALITY_PUBLIC_KEY,
  REALITY_SERVER_NAMES,
  REALITY_SHORT_ID,
  VLESS_PORT = "443",
  HYSTERIA2_PORT = "8443",
  PUBLIC_HOST,
  HYSTERIA2_INSECURE = "1",
  VLESS_INBOUND_TAG = "vless-reality-in",
  HYSTERIA_TRAFFIC_STATS_URL,
  HYSTERIA_TRAFFIC_STATS_SECRET,
  HYSTERIA_TRAFFIC_POLL_INTERVAL_MS = "30000",
} = process.env;

if (!NODE_API_SECRET) {
  logger.error("NODE_API_SECRET is not set, refusing to start");
  process.exit(1);
}
if (!PUBLIC_HOST) {
  logger.error("PUBLIC_HOST is not set, refusing to start");
  process.exit(1);
}

const grpcClient = new XrayGrpcClient(XRAY_API_ADDRESS);

const app = express();
app.use(express.json());

const START_TIME = Date.now();

// --- auth middleware, кроме /health и внутреннего хука Hysteria2 ---
// /internal/hysteria/auth дёргает сам процесс hysteria-server (localhost,
// та же машина), а не главный сервис — у него нет X-Node-Secret,
// поэтому путь исключён из общего мидлвара по аналогии с /health.
// Внешний трафик на этот путь в норме доходить не должен вообще,
// т.к. hysteria-server и node-agent всегда на одном хосте.
app.use((req, res, next) => {
  if (req.path === "/health" || req.path === "/internal/hysteria/auth") {
    return next();
  }
  const secret = req.header("X-Node-Secret");
  if (!secret || secret !== NODE_API_SECRET) {
    return res.status(403).json({ error: "forbidden" });
  }
  next();
});

function emailTagFor(externalId) {
  // email/tag должен быть уникальным и стабильным для stats/gRPC
  return `client-${externalId}`;
}

function buildConfigLink(row) {
  if (row.protocol === "vless_reality") {
    return buildVlessLink({
      uuid: row.uuid,
      publicHost: PUBLIC_HOST,
      vlessPort: VLESS_PORT,
      realityPublicKey: REALITY_PUBLIC_KEY,
      realityServerNames: REALITY_SERVER_NAMES,
      shortId: REALITY_SHORT_ID,
      externalId: row.external_id,
    });
  }
  return buildHysteria2Link({
    password: row.secret,
    publicHost: PUBLIC_HOST,
    hysteria2Port: HYSTERIA2_PORT,
    insecure: HYSTERIA2_INSECURE === "1",
    externalId: row.external_id,
  });
}

async function xrayAddClient({ protocol, external_id, uuid }) {
  if (protocol === "vless_reality") {
    await grpcClient.addVlessUser({
      tag: VLESS_INBOUND_TAG,
      email: emailTagFor(external_id),
      uuid,
      flow: "xtls-rprx-vision",
    });
  }
  // hysteria2: Xray его не обслуживает, отдельный процесс hysteria-server
  // не хранит список клиентов сам — авторизация идёт через HTTP-хук
  // /internal/hysteria/auth на каждое подключение. Конфигурировать
  // здесь нечего.
}

async function xrayRemoveClient({ protocol, external_id }) {
  if (protocol === "vless_reality") {
    await grpcClient.removeUser({
      tag: VLESS_INBOUND_TAG,
      email: emailTagFor(external_id),
    });
  }
  // hysteria2: см. комментарий в xrayAddClient. После db.deleteClient
  // следующая же попытка подключения с этим паролем получит ok:false
  // от /internal/hysteria/auth автоматически (findByHysteria2Password
  // ничего не найдёт).
}

// --- GET /health ---
app.get("/health", async (req, res) => {
  const xrayOk = await grpcClient.ping().catch(() => false);
  res.json({
    status: "ok",
    clients_count: db.clientsCount(),
    uptime_seconds: Math.floor((Date.now() - START_TIME) / 1000),
    xray_version: xrayOk ? "reachable" : "unreachable",
  });
});

// --- POST /internal/hysteria/auth ---
// Хук, который на каждое новое подключение клиента дёргает сам процесс
// hysteria-server (auth.type: http в его конфиге). Тело запроса —
// формат протокола apernet/hysteria HTTP auth: { addr, auth, tx }.
app.post("/internal/hysteria/auth", async (req, res) => {
  const { auth } = req.body || {};

  if (!auth) {
    logger.op("hysteria_auth", null, { ok: false, reason: "no_password" });
    return res.json({ ok: false });
  }

  const row = db.findByHysteria2Password(auth);

  if (!row) {
    logger.op("hysteria_auth", null, { ok: false, reason: "not_found" });
    return res.json({ ok: false });
  }

  if (
    row.traffic_limit_bytes != null &&
    row.bytes_uploaded + row.bytes_downloaded >= row.traffic_limit_bytes
  ) {
    logger.op("hysteria_auth", row.external_id, { ok: false, reason: "traffic_limit" });
    return res.json({ ok: false });
  }

  if (row.expires_at && new Date(row.expires_at).getTime() <= Date.now()) {
    logger.op("hysteria_auth", row.external_id, { ok: false, reason: "expired" });
    return res.json({ ok: false });
  }

  // Именно external_id возвращается как id — он же ключ в trafficStats
  // API Hysteria2, это критично для сведения статистики поллером.
  logger.op("hysteria_auth", row.external_id, { ok: true });
  res.json({ ok: true, id: row.external_id });
});

// --- POST /clients ---
app.post("/clients", async (req, res) => {
  const { external_id, protocol, traffic_limit_bytes, expires_at } = req.body || {};

  if (!external_id || !["vless_reality", "hysteria2"].includes(protocol)) {
    return res.status(400).json({ error: "invalid external_id or protocol" });
  }

  if (db.getClient(external_id)) {
    return res.status(409).json({ error: "client already exists" });
  }

  const isVless = protocol === "vless_reality";
  const generatedUuid = isVless ? uuidv4() : null;
  const generatedSecret = isVless
    ? REALITY_SHORT_ID
    : crypto.randomBytes(16).toString("hex");

  try {
    await xrayAddClient({
      protocol,
      external_id,
      uuid: generatedUuid,
      secret: generatedSecret,
    });
  } catch (err) {
    logger.error("failed to add client in Xray", {
      external_id,
      error: err.message,
    });
    return res.status(503).json({ error: "xray unavailable" });
  }

  const row = db.createClient({
    external_id,
    protocol,
    uuid: generatedUuid,
    secret: generatedSecret,
    email_tag: emailTagFor(external_id),
    traffic_limit_bytes: traffic_limit_bytes ?? null,
    expires_at: expires_at ?? null,
  });

  logger.op("add", external_id, { protocol });

  res.status(201).json({
    uuid: row.uuid,
    short_id: isVless ? REALITY_SHORT_ID : undefined,
    config_link: buildConfigLink(row),
  });
});

// --- DELETE /clients/:external_id ---
app.delete("/clients/:external_id", async (req, res) => {
  const external_id = req.params.external_id;
  const row = db.getClient(external_id);
  if (!row) return res.status(404).json({ error: "not found" });

  try {
    await xrayRemoveClient({ protocol: row.protocol, external_id });
  } catch (err) {
    logger.error("failed to remove client in Xray", {
      external_id,
      error: err.message,
    });
    return res.status(503).json({ error: "xray unavailable" });
  }

  db.deleteClient(external_id);
  logger.op("remove", external_id, { protocol: row.protocol });

  res.json({ ok: true });
});

// --- GET /clients/:external_id/stats ---
app.get("/clients/:external_id/stats", async (req, res) => {
  const external_id = req.params.external_id;
  const row = db.getClient(external_id);
  if (!row) return res.status(404).json({ error: "not found" });

  if (row.protocol === "hysteria2") {
    // Накопленные значения обновляются фоновым поллером
    // (hysteriaTrafficPoller.js), живой запрос к Hysteria2 здесь не нужен.
    return res.json({
      bytes_uploaded: row.bytes_uploaded,
      bytes_downloaded: row.bytes_downloaded,
    });
  }

  try {
    const stats = await grpcClient.getUserStats(emailTagFor(external_id));
    res.json(stats);
  } catch (err) {
    logger.error("failed to query stats", { external_id, error: err.message });
    res.status(503).json({ error: "xray unavailable" });
  }
});

// --- POST /clients/:external_id/regenerate ---
app.post("/clients/:external_id/regenerate", async (req, res) => {
  const external_id = req.params.external_id;
  const row = db.getClient(external_id);
  if (!row) return res.status(404).json({ error: "not found" });

  const isVless = row.protocol === "vless_reality";
  const newUuid = isVless ? uuidv4() : null;
  const newSecret = isVless
    ? REALITY_SHORT_ID
    : crypto.randomBytes(16).toString("hex");

  try {
    await xrayRemoveClient({ protocol: row.protocol, external_id });
    await xrayAddClient({
      protocol: row.protocol,
      external_id,
      uuid: newUuid,
      secret: newSecret,
    });
  } catch (err) {
    logger.error("failed to regenerate client in Xray", {
      external_id,
      error: err.message,
    });
    return res.status(503).json({ error: "xray unavailable" });
  }

  db.deleteClient(external_id);
  const newRow = db.createClient({
    external_id,
    protocol: row.protocol,
    uuid: newUuid,
    secret: newSecret,
    email_tag: emailTagFor(external_id),
    traffic_limit_bytes: row.traffic_limit_bytes,
    expires_at: row.expires_at,
  });

  logger.op("regenerate", external_id, { protocol: row.protocol });

  res.json({
    uuid: newRow.uuid,
    short_id: isVless ? REALITY_SHORT_ID : undefined,
    config_link: buildConfigLink(newRow),
  });
});

// --- глобальный обработчик ошибок, чтобы процесс не падал ---
app.use((err, req, res, next) => {
  logger.error("unhandled error", { error: err.message, stack: err.stack });
  res.status(500).json({ error: "internal error" });
});

process.on("unhandledRejection", (reason) => {
  logger.error("unhandledRejection", { reason: String(reason) });
});
process.on("uncaughtException", (err) => {
  logger.error("uncaughtException", { error: err.message, stack: err.stack });
  // Не завершаем процесс намеренно — pm2 всё равно перезапустит при падении,
  // но по возможности хотим пережить одиночную ошибку в обработчике запроса.
});

startHysteriaTrafficPoller({
  db,
  statsUrl: HYSTERIA_TRAFFIC_STATS_URL,
  secret: HYSTERIA_TRAFFIC_STATS_SECRET,
  intervalMs: Number(HYSTERIA_TRAFFIC_POLL_INTERVAL_MS),
});

app.listen(Number(PORT), () => {
  logger.info(`node-agent listening on port ${PORT}`);
});
