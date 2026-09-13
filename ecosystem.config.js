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
  ],
};
