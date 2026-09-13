"use strict";

const path = require("path");
const fs = require("fs");
const Database = require("better-sqlite3");

const DATA_DIR = path.join(__dirname, "..", "data");
const DB_PATH = path.join(DATA_DIR, "clients.db");

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS clients (
    external_id         TEXT PRIMARY KEY,
    protocol            TEXT NOT NULL CHECK(protocol IN ('vless_reality','hysteria2')),
    uuid                TEXT,           -- vless: user id. hysteria2: не используется
    secret               TEXT,           -- hysteria2: password. vless: short_id
    email_tag           TEXT NOT NULL,  -- уникальный email/tag для Xray user / stats
    traffic_limit_bytes INTEGER,
    expires_at           TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now'))
  );
`);

const stmts = {
  insert: db.prepare(`
    INSERT INTO clients (external_id, protocol, uuid, secret, email_tag, traffic_limit_bytes, expires_at)
    VALUES (@external_id, @protocol, @uuid, @secret, @email_tag, @traffic_limit_bytes, @expires_at)
  `),
  get: db.prepare(`SELECT * FROM clients WHERE external_id = ?`),
  delete: db.prepare(`DELETE FROM clients WHERE external_id = ?`),
  count: db.prepare(`SELECT COUNT(*) AS c FROM clients`),
};

module.exports = {
  db,
  createClient(row) {
    stmts.insert.run(row);
    return stmts.get.get(row.external_id);
  },
  getClient(externalId) {
    return stmts.get.get(externalId);
  },
  deleteClient(externalId) {
    return stmts.delete.run(externalId);
  },
  clientsCount() {
    return stmts.count.get().c;
  },
};
