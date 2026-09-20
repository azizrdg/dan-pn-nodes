"use strict";
// Pull-синхронизация ноды с главным сервисом.
//
// Главный сервис (БД) — единственный источник истины о том, КОМУ разрешён
// доступ (см. main/src/nodes/syncApi.js). Раз в MAIN_SYNC_INTERVAL_MS
// (по умолчанию 10 минут) нода скачивает полный список разрешённых
// клиентов и приводит в соответствие и локальную SQLite, и Xray:
//   - клиентов, которых в списке нет (заблокирован, подписка истекла,
//     устройство удалено, сбой push-удаления) — УДАЛЯЕТ;
//   - клиентов из списка, которых нет локально (нода была недоступна, БД
//     пересоздана, push не дошёл) — ДОБАВЛЯЕТ;
//   - клиентов с изменившимся uuid — пересоздаёт.
//
// Защита от гонок с обычным API ноды (POST/DELETE /clients от главного
// сервиса): всё, что API трогало после НАЧАЛА текущего цикла синхронизации
// (map `touched`, наполняется в index.js), в этом цикле пропускается —
// список мог устареть, разберёмся на следующем тике.
//
// Защита от "стирания всех": действуем ТОЛЬКО при HTTP 200 и ok:true с
// массивом clients. Любая ошибка сети/сервера — ничего не удаляем.
const logger = require("./logger");

const FETCH_TIMEOUT_MS = 30000;
const TOUCH_MARGIN_MS = 5000;
const TOUCH_RETENTION_MS = 60 * 60 * 1000;
const FIRST_RUN_DELAY_MS = 20000; // после replayExistingClientsIntoXray()

function startMainSync({
  db,
  grpcClient,
  xrayAddClient,
  xrayRemoveClient,
  emailTagFor,
  touched,
  url,
  secret,
  intervalMs,
}) {
  if (!url) {
    logger.warn("main-sync: MAIN_SYNC_URL не задан — синхронизация с главным сервисом ОТКЛЮЧЕНА");
    return null;
  }

  let running = false;

  async function removeLocal(row) {
    try {
      await xrayRemoveClient({ protocol: row.protocol, external_id: row.external_id });
    } catch (err) {
      // Ошибка удаления может значить "пользователя в Xray уже нет" — тогда
      // достаточно убрать строку из БД. Но если сам Xray недоступен, строку
      // оставляем и повторим на следующем тике.
      const xrayUp = await grpcClient.ping().catch(() => false);
      if (!xrayUp) {
        logger.warn("main-sync: Xray недоступен, удаление отложено", { external_id: row.external_id });
        return false;
      }
    }
    db.deleteClient(row.external_id);
    return true;
  }

  async function addLocal(want) {
    const args = { protocol: "vless_reality", external_id: want.external_id, uuid: want.uuid };
    try {
      await xrayAddClient(args);
    } catch (err) {
      // Возможно, пользователь уже есть в Xray (БД потеряна) — пересоздаём.
      try {
        await xrayRemoveClient(args);
        await xrayAddClient(args);
      } catch (err2) {
        logger.warn("main-sync: не удалось добавить клиента в Xray", {
          external_id: want.external_id,
          error: err2.message,
        });
        return false;
      }
    }
    db.createClient({
      external_id: want.external_id,
      protocol: "vless_reality",
      uuid: want.uuid,
      secret: null,
      email_tag: emailTagFor(want.external_id),
      traffic_limit_bytes: want.traffic_limit_bytes != null ? Number(want.traffic_limit_bytes) : null,
      expires_at: want.expires_at ?? null,
      device_limit: 1,
    });
    return true;
  }

  async function tick() {
    if (running) return;
    running = true;
    const startedAt = Date.now();
    try {
      const res = await fetch(url, {
        headers: { "X-Node-Secret": secret },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      if (!res.ok) {
        logger.warn("main-sync: главный сервис вернул не-OK, синхронизация пропущена", { status: res.status });
        return;
      }
      const data = await res.json();
      if (!data || data.ok !== true || !Array.isArray(data.clients)) {
        logger.warn("main-sync: некорректный ответ главного сервиса, синхронизация пропущена");
        return;
      }

      // Чистим устаревшие отметки "API трогал клиента".
      for (const [id, ts] of touched) {
        if (startedAt - ts > TOUCH_RETENTION_MS) touched.delete(id);
      }
      const recentlyTouched = (id) => (touched.get(id) || 0) >= startedAt - TOUCH_MARGIN_MS;

      const desired = new Map(data.clients.map((c) => [String(c.external_id), c]));
      const local = db.db.prepare("SELECT * FROM clients").all();
      const localMap = new Map(local.map((r) => [r.external_id, r]));

      let removed = 0;
      let added = 0;
      let replaced = 0;
      let skipped = 0;

      // 1) Лишние: есть у нас, но главный сервис доступа не разрешает.
      for (const row of local) {
        if (desired.has(row.external_id)) continue;
        if (recentlyTouched(row.external_id)) {
          skipped += 1;
          continue;
        }
        if (await removeLocal(row)) removed += 1;
      }

      // 2) Недостающие / изменившиеся.
      for (const [id, want] of desired) {
        if (recentlyTouched(id)) {
          skipped += 1;
          continue;
        }
        const row = localMap.get(id);
        if (row && row.protocol === "vless_reality" && row.uuid === want.uuid) continue;

        if (row) {
          if (!(await removeLocal(row))) continue;
          if (await addLocal(want)) replaced += 1;
        } else if (await addLocal(want)) {
          added += 1;
        }
      }

      logger.info("main-sync: синхронизация завершена", {
        desired: desired.size,
        local_before: local.length,
        added,
        removed,
        replaced,
        skipped,
      });
    } catch (err) {
      logger.warn("main-sync: ошибка синхронизации (ничего не удалено по этой причине)", { error: err.message });
    } finally {
      running = false;
    }
  }

  const first = setTimeout(tick, FIRST_RUN_DELAY_MS);
  if (first.unref) first.unref();
  const timer = setInterval(tick, intervalMs);
  if (timer.unref) timer.unref();

  logger.info("main-sync: запущена", { url, intervalMs });
  return { timer, tick };
}

module.exports = { startMainSync };
