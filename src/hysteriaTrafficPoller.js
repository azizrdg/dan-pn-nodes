"use strict";

const logger = require("./logger");

/**
 * Периодически опрашивает trafficStats API самого процесса
 * hysteria-server (127.0.0.1:9999 по умолчанию) и накапливает дельты
 * трафика в БД через db.incrementTraffic.
 *
 * Формат ответа: { "<external_id>": { "tx": <int>, "rx": <int> }, ... }
 * — только по клиентам, у которых была активность с прошлого опроса
 * (clear=1). "<external_id>" — это то же значение, которое node-agent
 * вернул в ответе /internal/hysteria/auth как "id".
 *
 * Запускается только если передан statsUrl (т.е. в .env задан
 * HYSTERIA_TRAFFIC_STATS_URL) — на нодах без Hysteria2 поллер не
 * стартует и никуда не стучится.
 */
function startHysteriaTrafficPoller({ db, statsUrl, secret, intervalMs }) {
  if (!statsUrl) {
    logger.info(
      "HYSTERIA_TRAFFIC_STATS_URL не задан, поллер трафика Hysteria2 отключён"
    );
    return null;
  }

  async function poll() {
    try {
      const url = `${statsUrl.replace(/\/$/, "")}/traffic?clear=1`;

      // Формат авторизации подтверждён по документации apernet/hysteria
      // для http trafficStats API: заголовок Authorization: Bearer <secret>.
      // Если версия hysteria-server, которую ставит add-node.js,
      // использует другой заголовок — поменять здесь.
      const res = await fetch(url, {
        headers: secret ? { Authorization: `Bearer ${secret}` } : {},
      });

      if (!res.ok) {
        logger.warn("hysteria traffic poll: не-OK ответ", { status: res.status });
        return;
      }

      const data = await res.json();

      for (const [externalId, stat] of Object.entries(data || {})) {
        const tx = Number(stat && stat.tx) || 0;
        const rx = Number(stat && stat.rx) || 0;
        if (tx === 0 && rx === 0) continue;
        db.incrementTraffic(externalId, tx, rx);
      }
    } catch (err) {
      // Сервис Hysteria2 недоступен или ответ некорректен — не падаем,
      // просто ждём следующего тика.
      logger.warn("hysteria traffic poll failed", { error: err.message });
    }
  }

  const timer = setInterval(poll, intervalMs);
  if (timer.unref) timer.unref();

  logger.info("hysteria traffic poller started", { statsUrl, intervalMs });

  return timer;
}

module.exports = { startHysteriaTrafficPoller };
