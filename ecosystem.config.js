require('dotenv').config();

module.exports = {
  apps: [
    {
      name: 'claude-mongo-sync',
      script: './dist/index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '200M',

      // NODE_ENV only - other env vars loaded from .env
      env: {
        NODE_ENV: 'development',
      },
      env_production: {
        NODE_ENV: 'production',
      },

      // Restart behavior
      exp_backoff_restart_delay: 100,
      max_restarts: 10,
      min_uptime: '10s',

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: '~/.pm2/logs/claude-mongo-sync-error.log',
      out_file: '~/.pm2/logs/claude-mongo-sync-out.log',
      merge_logs: true,

      // Graceful shutdown
      kill_timeout: 10000,
      listen_timeout: 5000,
      shutdown_with_message: true,
    },
  ],
};
