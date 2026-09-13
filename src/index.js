"use strict";

require("dotenv").config();
const express = require("express");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");

const logger = require("./logger");
const db = require("./db");
const { XrayGrpcClient } = require("./xrayGrpc");
const { XrayConfigFileManager } = require("./xrayConfigFile");
const { buildVlessLink, buildHysteria2Link } = require("./linkBuilder");

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
  XRAY_CONFIG_PATH = "/usr/local/etc/xray/config.json",
  HYSTERIA2_INSECURE = "1",
  VLESS_INBOUND_TAG = "vless-reality-in",
  HYSTERIA2_INBOUND_TAG = "hysteria2-in",
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
const hysteria2Files = new XrayConfigFileManager({
  configPath: XRAY_CONFIG_PATH,
  inboundTag: HYSTERIA2_INBOUND_TAG,
});

const app = express();
app.use(express.json());

const START_TIME = Date.now();

// --- auth middleware, кроме /health ---
app.use((req, res, next) => {
  if (req.path === "/health") return next();
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

async function xrayAddClient({ protocol, external_id, uuid, secret }) {
  const email = emailTagFor(external_id);
  if (protocol === "vless_reality") {
    await grpcClient.addVlessUser({
      tag: VLESS_INBOUND_TAG,
      email,
      uuid,
      flow: "xtls-rprx-vision",
    });
  } else {
    hysteria2Files.addHysteria2Client({ email, password: secret });
  }
}

async function xrayRemoveClient({ protocol, external_id }) {
  const email = emailTagFor(external_id);
  if (protocol === "vless_reality") {
    await grpcClient.removeUser({ tag: VLESS_INBOUND_TAG, email });
  } else {
    hysteria2Files.removeHysteria2Client({ email });
  }
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

app.listen(Number(PORT), () => {
  logger.info(`node-agent listening on port ${PORT}`);
});
