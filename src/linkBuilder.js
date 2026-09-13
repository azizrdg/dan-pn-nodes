"use strict";

function buildVlessLink({
  uuid,
  publicHost,
  vlessPort,
  realityPublicKey,
  realityServerNames,
  shortId,
  externalId,
}) {
  const sni = realityServerNames.split(",")[0].trim();
  const params = new URLSearchParams({
    security: "reality",
    sni,
    fp: "chrome",
    pbk: realityPublicKey,
    sid: shortId,
    type: "tcp",
    flow: "xtls-rprx-vision",
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
