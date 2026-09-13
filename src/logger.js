"use strict";

/**
 * Простой логгер в stdout. pm2 сам добавляет свои метки времени в
 * `pm2 logs`, но ТЗ требует timestamp + external_id в самой строке лога,
 * чтобы это было видно и при перенаправлении stdout в файл напрямую.
 */
function ts() {
  return new Date().toISOString();
}

function log(level, message, meta = {}) {
  const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
  const line = `[${ts()}] [${level}] ${message}${metaStr}`;
  if (level === "ERROR") {
    console.error(line);
  } else {
    console.log(line);
  }
}

module.exports = {
  info: (message, meta) => log("INFO", message, meta),
  warn: (message, meta) => log("WARN", message, meta),
  error: (message, meta) => log("ERROR", message, meta),
  // Специально для операций add/remove/regenerate, где обязателен external_id
  op: (operation, external_id, meta = {}) =>
    log("OP", operation, { external_id, ...meta }),
};
