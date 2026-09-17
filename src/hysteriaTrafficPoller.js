"use strict";

const logger = require("./logger");

/**
 * Периодически опрашивает trafficStats API самого процесса
 * hysteria-server (127.0.0.1:9999 по умолчанию) и накапливает дельты
 * трафика в БД через db.incrementTraffic.
 *
 * Формат ответа /traffic: { "<external_id>": { "tx": <int>, "rx": <int> }, ... }
 * — только по клиентам, у которых была активность с прошлого опроса
 * (clear=1). "<external_id>" — то же значение, которое node-agent
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
      logger.warn("hysteria traffic poll failed", { error: err.message });
    }
  }

  const timer = setInterval(poll, intervalMs);
  if (timer.unref) timer.unref();

  logger.info("hysteria traffic poller started", { statsUrl, intervalMs });

  return timer;
}

/**
 * Разовый запрос количества АКТИВНЫХ СОЕДИНЕНИЙ конкретного клиента через
 * эндпоинт /online trafficStats API Hysteria2 (в отличие от /traffic,
 * ничего не "очищает" — можно опрашивать в любой момент по требованию).
 * Формат ответа: { "<external_id>": <кол-во соединений>, ... }.
 *
 * ВАЖНО: это количество СОЕДИНЕНИЙ, а не уникальных IP (в отличие от
 * VLESS-подсчёта через deviceLimiter.js) — для Hysteria2 у нас нет
 * дешёвого способа получить исходные IP через HTTP trafficStats API,
 * поэтому используется как приблизительная эвристика количества
 * устройств (см. index.js GET /clients/:id/devices).
 */
async function getOnlineCount(statsUrl, secret, externalId) {
  if (!statsUrl) return 0;
  try {
    const url = `${statsUrl.replace(/\/$/, "")}/online`;
    const res = await fetch(url, {
      headers: secret ? { Authorization: `Bearer ${secret}` } : {},
    });
    if (!res.ok) return 0;
    const data = await res.json();
    const value = data && data[externalId];
    return Number(value) || 0;
  } catch (err) {
    logger.warn("hysteria online query failed", { error: err.message });
    return 0;
  }
}

module.exports = { startHysteriaTrafficPoller, getOnlineCount };
