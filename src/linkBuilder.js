"use strict";

function buildVlessLink({
  uuid,
  publicHost,
  vlessPort,
  wsPath,
  externalId,
}) {
  // TLS+WebSocket за Cloudflare: настоящий сертификат на настоящий домен,
  // никакой имитации чужого TLS-хендшейка (как было в Reality) — поэтому
  // ни pbk/sid/serverNames тут больше нет, только обычные TLS+WS параметры.
  // sni и host намеренно равны publicHost — это домен ноды, за которым
  // стоит Cloudflare.
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

function buildHysteria2Link({
  password,
  publicHost,
  hysteria2Port,
  insecure,
  externalId,
}) {
  const params = new URLSearchParams({
    insecure: insecure ? "1" : "0",
  });
  return `hysteria2://${password}@${publicHost}:${hysteria2Port}/?${params.toString()}#${encodeURIComponent(
    externalId
  )}`;
}

module.exports = { buildVlessLink, buildHysteria2Link };
