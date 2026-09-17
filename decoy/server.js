"use strict";
// Минимальный статический HTTP-сервер без зависимостей — отдаёт нейтральную
// HTML-страницу на ЛЮБОЙ путь. Слушает только 127.0.0.1, снаружи недоступен
// напрямую — до него долетают ТОЛЬКО запросы, которые Xray fallback
// (см. provisionNode.js buildXrayConfig) не смог распознать как настоящий
// VLESS/WS-хендшейк. Задача: чтобы обычный GET на домен ноды выглядел как
// обращение к рядовому сайту, а не молчал/не отдавал ошибку — именно
// "тишина" или странный ответ на голый GET — типичный признак VPN-сервера
// при активном сканировании.
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = parseInt(process.env.DECOY_PORT || "8080", 10);
const PUBLIC_DIR = path.join(__dirname, "public");

const INDEX_HTML = fs.readFileSync(path.join(PUBLIC_DIR, "index.html"));

const server = http.createServer((req, res) => {
  // Один и тот же нейтральный ответ на любой путь и метод — не пытаемся
  // изображать полноценный сайт с роутингом, этого достаточно, чтобы
  // GET / (самая частая проверка) не выглядел подозрительно.
  res.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Content-Length": INDEX_HTML.length,
    Server: "nginx",
  });
  res.end(INDEX_HTML);
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`[decoy-site] listening on 127.0.0.1:${PORT}`);
});