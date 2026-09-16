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
  VLESS_PORT = "443",
  VLESS_WS_PATH,
  VLESS_INBOUND_TAG = "vless-ws-in",
  HYSTERIA2_PORT = "8443",
  PUBLIC_HOST,
  HYSTERIA2_INSECURE = "1",
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
if (!VLESS_WS_PATH) {
  logger.error("VLESS_WS_PATH is not set, refusing to start");
  process.exit(1);
}

const grpcClient = new XrayGrpcClient(XRAY_API_ADDRESS);

const app = express();
app.use(express.json());

const START_TIME = Date.now();

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
  return `client-${externalId}`;
}

function buildConfigLink(row) {
  if (row.protocol === "vless_reality") {
    return buildVlessLink({
      uuid: row.uuid,
      publicHost: PUBLIC_HOST,
      vlessPort: VLESS_PORT,
      wsPath: VLESS_WS_PATH,
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
    });
  }
}

async function xrayRemoveClient({ protocol, external_id }) {
  if (protocol === "vless_reality") {
    await grpcClient.removeUser({
      tag: VLESS_INBOUND_TAG,
      email: emailTagFor(external_id),
    });
  }
}

/**
 * Прогоняет всех vless-клиентов из локальной БД обратно в Xray через gRPC.
 *
 * ПОЧЕМУ ЭТО НУЖНО: config.json всегда содержит пустой settings.clients —
 * реальные пользователи живут ТОЛЬКО в памяти запущенного процесса Xray,
 * добавленные через AlterInbound. Раньше при любом `systemctl restart xray`
 * (в т.ч. автоматическом после падения) ВСЕ существующие клиенты молча
 * переставали подключаться, хотя их запись в SQLite оставалась цела — до
 * первого ручного /regenerate. Это также ключевой кирпичик для полного
 * пересоздания ноды (restore-node.js): после восстановления сервера с той
 * же БД (или после replay через POST /clients с явным uuid) все клиенты
 * должны сами оказаться в Xray без участия пользователя.
 */
async function replayExistingClientsIntoXray() {
  const rows = db.db
    .prepare(`SELECT * FROM clients WHERE protocol = 'vless_reality'`)
    .all();

  if (rows.length === 0) return;

  logger.info(`replay: найдено ${rows.length} vless-клиентов, добавляем в Xray...`);

  let xrayReady = false;
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    xrayReady = await grpcClient.ping().catch(() => false);
    if (xrayReady) break;
    await new Promise((r) => setTimeout(r, 2000));
  }
  if (!xrayReady) {
    logger.error("replay: Xray API недоступен после 10 попыток (20с), replay пропущен");
    return;
  }

  let ok = 0;
  let failed = 0;
  for (const row of rows) {
    try {
      await xrayAddClient({ protocol: row.protocol, external_id: row.external_id, uuid: row.uuid });
      ok += 1;
    } catch (err) {
      failed += 1;
      logger.warn("replay: не удалось добавить клиента в Xray", {
        external_id: row.external_id,
        error: err.message,
      });
    }
  }
  logger.info(`replay: завершён, успешно=${ok}, ошибок=${failed}`);
}

app.get("/health", async (req, res) => {
  const xrayOk = await grpcClient.ping().catch(() => false);
  res.json({
    status: "ok",
    clients_count: db.clientsCount(),
    uptime_seconds: Math.floor((Date.now() - START_TIME) / 1000),
    xray_version: xrayOk ? "reachable" : "unreachable",
  });
});

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

  logger.op("hysteria_auth", row.external_id, { ok: true });
  res.json({ ok: true, id: row.external_id });
});

// --- POST /clients ---
// ВАЖНО: поле uuid в теле запроса ОПЦИОНАЛЬНО. Обычная покупка его не
// передаёт (генерируется новый uuidv4()). Оно нужно ИСКЛЮЧИТЕЛЬНО для
// restore-node.js — при пересоздании упавшей ноды главный сервер повторно
// создаёт всех клиентов, явно передавая их СТАРЫЙ uuid из своей БД, чтобы
// у пользователя не поменялась ссылка подключения.
app.post("/clients", async (req, res) => {
  const { external_id, protocol, traffic_limit_bytes, expires_at, uuid } = req.body || {};

  if (!external_id || !["vless_reality", "hysteria2"].includes(protocol)) {
    return res.status(400).json({ error: "invalid external_id or protocol" });
  }

  if (db.getClient(external_id)) {
    return res.status(409).json({ error: "client already exists" });
  }

  const isVless = protocol === "vless_reality";
  const generatedUuid = isVless ? (uuid || uuidv4()) : null;
  // Reality-shortId (общий "секрет" на ноду) больше не существует — для
  // vless_reality (историческое имя протокола в схеме, по факту теперь
  // TLS+WS) поле secret не используется вообще.
  const generatedSecret = isVless ? null : crypto.randomBytes(16).toString("hex");

  try {
    await xrayAddClient({
      protocol,
      external_id,
      uuid: generatedUuid,
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

  logger.op("add", external_id, { protocol, restored: Boolean(uuid) });

  res.status(201).json({
    uuid: row.uuid,
    config_link: buildConfigLink(row),
  });
});

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

app.get("/clients/:external_id/stats", async (req, res) => {
  const external_id = req.params.external_id;
  const row = db.getClient(external_id);
  if (!row) return res.status(404).json({ error: "not found" });

  if (row.protocol === "hysteria2") {
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

app.post("/clients/:external_id/regenerate", async (req, res) => {
  const external_id = req.params.external_id;
  const row = db.getClient(external_id);
  if (!row) return res.status(404).json({ error: "not found" });

  const isVless = row.protocol === "vless_reality";
  const newUuid = isVless ? uuidv4() : null;
  const newSecret = isVless ? null : crypto.randomBytes(16).toString("hex");

  try {
    await xrayRemoveClient({ protocol: row.protocol, external_id });
    await xrayAddClient({
      protocol: row.protocol,
      external_id,
      uuid: newUuid,
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
    config_link: buildConfigLink(newRow),
  });
});

app.use((err, req, res, next) => {
  logger.error("unhandled error", { error: err.message, stack: err.stack });
  res.status(500).json({ error: "internal error" });
});

process.on("unhandledRejection", (reason) => {
  logger.error("unhandledRejection", { reason: String(reason) });
});
process.on("uncaughtException", (err) => {
  logger.error("uncaughtException", { error: err.message, stack: err.stack });
});

startHysteriaTrafficPoller({
  db,
  statsUrl: HYSTERIA_TRAFFIC_STATS_URL,
  secret: HYSTERIA_TRAFFIC_STATS_SECRET,
  intervalMs: Number(HYSTERIA_TRAFFIC_POLL_INTERVAL_MS),
});

app.listen(Number(PORT), () => {
  logger.info(`node-agent listening on port ${PORT}`);
  // Не блокируем старт HTTP-сервера ожиданием Xray — реплей идёт в фоне.
  replayExistingClientsIntoXray().catch((err) =>
    logger.error("replay: непредвиденная ошибка", { error: err.message })
  );
});
