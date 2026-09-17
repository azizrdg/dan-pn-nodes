// Локальный dev-конфиг pm2 для node/*. Соответствует тому, что
// автоматически генерирует main/scripts/lib/provisionNode.js
// (buildNodeAgentEcosystem) при реальном провижининге ноды: два
// процесса — node-agent и decoy-site (фолбэк Xray для нераспознанных
// подключений, см. node/decoy/server.js).
module.exports = {
  apps: [
    {
      name: "node-agent",
      script: "src/index.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
      },
    },
    {
      name: "decoy-site",
      script: "decoy/server.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 20,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
        DECOY_PORT: "8080",
      },
    },
  ],
};
