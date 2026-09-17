"use strict";
// Приблизительное определение количества УНИКАЛЬНЫХ IP-адресов,
// подключившихся под одним VLESS-клиентом (email = "client-<external_id>",
// см. index.js emailTagFor), за скользящее окно времени — используется
// как эвристика количества одновременно используемых устройств (см.
// main/src/scheduler.pollDeviceLimits, который агрегирует это число со
// ВСЕХ активных нод через getAggregatedDeviceCount).
//
// Xray-core не отдаёт список активных IP по gRPC API "из коробки" — нет
// метода вида "GetUserOnlineIps". Поэтому подсчёт идёт через разбор
// access-лога, который сам Xray пишет на каждое принятое соединение
// (см. main/scripts/lib/provisionNode.js buildXrayConfig -> log.access).
// Это НЕ 100%-но точный подсчёт устройств (несколько TCP-соединений с
// одного IP — это ожидаемо и не считается лишним устройством, так как
// мы дедуплицируем по IP), но для защиты от расшаривания одной подписки
// на много людей — достаточно.
const fs = require("fs");
const readline = require("readline");
const logger = require("./logger");

// Формат строки access-лога Xray:
// "2024/01/02 15:04:05 [Info] [12345678] 203.0.113.10:51000 accepted
//  tcp:example.com:443 [vless-ws-in] email: client-u42-abcd1234"
// Нам нужны IP (перед первым ":" в адресе источника) и email (после
// "email: ").
const LOG_LINE_RE = /(\d{1,3}(?:\.\d{1,3}){3}):\d+\s+accepted.*email:\s*(\S+)/;

const WINDOW_MS = 10 * 60 * 1000; // 10 минут — считаем устройство "активным"
const EMAIL_PREFIX = "client-";

class DeviceLimiter {
  constructor({ logPath }) {
    this.logPath = logPath;
    // external_id -> Map<ip, lastSeenTimestamp>
    this.seen = new Map();
    this._position = 0;
    this._watcher = null;
  }

  start() {
    if (!this.logPath) {
      logger.warn("deviceLimiter: путь к access-логу не задан, отключён");
      return;
    }
    try {
      if (!fs.existsSync(this.logPath)) {
        // Файл появится после первого реального подключения через Xray —
        // это нормально при первом старте на чистой ноде.
        fs.writeFileSync(this.logPath, "");
      }
      this._position = fs.statSync(this.logPath).size;
      this._watcher = fs.watch(this.logPath, () => this._readNewLines());
      logger.info("deviceLimiter: отслеживание access-лога запущено", { logPath: this.logPath });
    } catch (err) {
      logger.warn("deviceLimiter: не удалось запустить отслеживание лога", { error: err.message });
    }
  }

  _readNewLines() {
    fs.stat(this.logPath, (err, stats) => {
      if (err) return;
      if (stats.size < this._position) {
        // Логротация — файл усечён/пересоздан, читаем с начала.
        this._position = 0;
      }
      if (stats.size === this._position) return;

      const stream = fs.createReadStream(this.logPath, {
        start: this._position,
        end: stats.size,
      });
      this._position = stats.size;

      const rl = readline.createInterface({ input: stream });
      rl.on("line", (line) => this._handleLine(line));
    });
  }

  _handleLine(line) {
    const match = LOG_LINE_RE.exec(line);
    if (!match) return;
    const [, ip, emailTag] = match;
    const externalId = emailTag.startsWith(EMAIL_PREFIX) ? emailTag.slice(EMAIL_PREFIX.length) : emailTag;

    if (!this.seen.has(externalId)) this.seen.set(externalId, new Map());
    this.seen.get(externalId).set(ip, Date.now());
  }

  /**
   * @param {string} externalId
   * @returns {string[]} уникальные IP, "виденные" за последние WINDOW_MS
   */
  getIps(externalId) {
    const ipMap = this.seen.get(externalId);
    if (!ipMap) return [];
    const now = Date.now();
    const alive = [];
    for (const [ip, lastSeen] of ipMap.entries()) {
      if (now - lastSeen <= WINDOW_MS) {
        alive.push(ip);
      } else {
        ipMap.delete(ip);
      }
    }
    return alive;
  }
}

module.exports = { DeviceLimiter };
