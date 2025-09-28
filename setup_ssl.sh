#!/usr/bin/env bash
set -euo pipefail

# setup_ssl.sh
# Automate Let's Encrypt (Certbot) SSL issuance and auto-renew via cron/systemd timer.
# Usage:
#   sudo bash setup_ssl.sh your.domain you@example.com

DOMAIN=${1:-}
EMAIL=${2:-}

if [[ -z "$DOMAIN" || -z "$EMAIL" ]]; then
  echo "Usage: sudo bash setup_ssl.sh <domain> <email>"
  exit 1
fi

echo "==> Installing nginx, certbot, and plugin..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y nginx certbot python3-certbot-nginx

echo "==> Ensuring nginx is running"
systemctl enable nginx --now

echo "==> Requesting/Installing certificate for $DOMAIN"
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m "$EMAIL"

echo "==> Setting up cron for renewal (twice a day check)"
# Certbot installs a systemd timer by default. We also add a cron as fallback.
(crontab -l 2>/dev/null | grep -v 'certbot renew' ; echo "0 3,15 * * * /usr/bin/certbot renew --quiet --deploy-hook 'systemctl reload nginx'") | crontab -

echo "==> Done. Nginx will be reloaded automatically after renewal."

