"use strict";

/**
 * Ссылка подключения VLESS+WS+TLS. host — ОБЩИЙ для ВСЕХ нод хост
 * (Cloudflare Load Balancer, см. main/src/config/env.js
 * CLOUDFLARE_CONNECT_HOSTNAME), передаётся node-agent'у через
 * PUBLIC_HOST — это НЕ адрес/домен конкретной ноды. sni/host намеренно
 * равны общему хосту — это принципиально: ссылка НИКОГДА не должна
 * указывать на конкретный сервер (и уж тем более на главный сервис).
 */
function buildVlessLink({
  uuid,
  publicHost,
  vlessPort,
  wsPath,
  externalId,
}) {
  const params = new URLSearchParams({
    security: "tls",
    sni: publicHost,
    fp: "chrome",
    type: "ws",
    host: publicHost,
    path: wsPath,
    encryption: "none",
  });
  return `vless://${uuid}@${publicHost}:${vlessPort}?${params.toString()}#${encodeURIComponent(
    externalId
  )}`;
}

/**
 * Ссылка подключения Hysteria2. host здесь — ОБЩАЯ DNS round-robin
 * запись (CLOUDFLARE_HYSTERIA_HOSTNAME, см. main/src/config/env.js),
 * а НЕ адрес конкретной ноды — Cloudflare не проксирует UDP, поэтому
 * вместо Load Balancer'а используется обычный round-robin из нескольких
 * A-записей с одним именем; клиентская ОС сама перебирает адреса.
 */
function buildHysteria2Link({
  password,
  host,
  hysteria2Port,
  insecure,
  externalId,
}) {
  const params = new URLSearchParams({
    insecure: insecure ? "1" : "0",
  });
  return `hysteria2://${password}@${host}:${hysteria2Port}/?${params.toString()}#${encodeURIComponent(
    externalId
  )}`;
}

module.exports = { buildVlessLink, buildHysteria2Link };
