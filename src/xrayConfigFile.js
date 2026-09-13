"use strict";

const fs = require("fs");
const { execFileSync } = require("child_process");
const logger = require("./logger");

/**
 * Fallback-путь управления пользователями Hysteria2: правка config.json +
 * systemctl restart xray. Согласно ТЗ это крайний случай (рвёт сессии
 * остальных клиентов), используется намеренно, т.к. для Hysteria2-inbound
 * в Xray нет надёжного динамического gRPC-добавления пользователя.
 */
class XrayConfigFileManager {
  constructor({ configPath, inboundTag }) {
    this.configPath = configPath;
    this.inboundTag = inboundTag;
  }

  _read() {
    const raw = fs.readFileSync(this.configPath, "utf8");
    return JSON.parse(raw);
  }

  _write(config) {
    const tmpPath = `${this.configPath}.tmp`;
    fs.writeFileSync(tmpPath, JSON.stringify(config, null, 2), "utf8");
    fs.renameSync(tmpPath, this.configPath);
  }

  _findInbound(config) {
    const inbound = (config.inbounds || []).find(
      (ib) => ib.tag === this.inboundTag
    );
    if (!inbound) {
      throw new Error(
        `inbound with tag "${this.inboundTag}" not found in ${this.configPath}`
      );
    }
    if (!inbound.settings) inbound.settings = {};
    if (!Array.isArray(inbound.settings.clients)) {
      inbound.settings.clients = [];
    }
    return inbound;
  }

  restart() {
    execFileSync("systemctl", ["restart", "xray"], { stdio: "pipe" });
  }

  addHysteria2Client({ email, password }) {
    const config = this._read();
    const inbound = this._findInbound(config);

    const exists = inbound.settings.clients.some((c) => c.name === email);
    if (exists) {
      throw new Error(`client "${email}" already exists in ${this.inboundTag}`);
    }

    inbound.settings.clients.push({
      password,
      name: email,
    });

    this._write(config);
    this.restart();
    logger.info("hysteria2 client added via config.json + restart", { email });
  }

  removeHysteria2Client({ email }) {
    const config = this._read();
    const inbound = this._findInbound(config);

    const before = inbound.settings.clients.length;
    inbound.settings.clients = inbound.settings.clients.filter(
      (c) => c.name !== email
    );

    if (inbound.settings.clients.length === before) {
      // Клиента и так не было — не считаем это ошибкой (идемпотентность)
      logger.warn("hysteria2 client not found on removal", { email });
      return;
    }

    this._write(config);
    this.restart();
    logger.info("hysteria2 client removed via config.json + restart", { email });
  }
}

module.exports = { XrayConfigFileManager };
