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

  let running = false;
  async function poll() {
    if (running) return;
    running = true;
    try {
      const url = `${statsUrl.replace(/\/$/, "")}/traffic?clear=1`;

      const res = await fetch(url, {
        headers: secret ? { Authorization: secret } : {},
      signal: AbortSignal.timeout(5000),
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
        const row = db.getClient(externalId);
        if (!row) {
          await kickClients(statsUrl, secret, [externalId]);
          continue;
        }
        db.incrementTraffic(externalId, rx, tx);
        const current = db.getClient(externalId);
        if (isDenied(current)) await kickClients(statsUrl, secret, [externalId]);
      }
      const denied = db.db.prepare(`SELECT external_id FROM clients WHERE revoked = 1
        OR (expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now'))
        OR (traffic_limit_bytes IS NOT NULL AND bytes_uploaded + bytes_downloaded >= traffic_limit_bytes)`).all();
      if (denied.length) await kickClients(statsUrl, secret, denied.map((row) => row.external_id));
    } catch (err) {
      logger.warn("hysteria traffic poll failed", { error: err.message });
    } finally { running = false; }
  }

  const timer = setInterval(poll, intervalMs);
  poll();
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
      headers: secret ? { Authorization: secret } : {},
      signal: AbortSignal.timeout(5000),
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

function isDenied(row) {
  return !row || Boolean(row.revoked) || (row.expires_at && new Date(row.expires_at).getTime() <= Date.now()) ||
    (row.traffic_limit_bytes != null && row.bytes_uploaded + row.bytes_downloaded >= row.traffic_limit_bytes);
}
async function kickClients(statsUrl, secret, ids) {
  if (!statsUrl || !ids.length) return;
  const res = await fetch(`${statsUrl.replace(/\/$/, '')}/kick`, {
    method: 'POST', headers: { Authorization: secret || '', 'Content-Type': 'application/json' },
    body: JSON.stringify(ids), signal: AbortSignal.timeout(5000)
  });
  if (!res.ok) throw new Error(`Hysteria kick: HTTP ${res.status}`);
}
module.exports = { startHysteriaTrafficPoller, getOnlineCount, kickClients, isDenied };
