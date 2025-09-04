# Twitter Likes Fetcher — EC2 (Production) Quickstart

Minimal, production-focused instructions for deploying on EC2 with Docker Compose. This app fetches all users who liked a tweet using OAuth 1.0a, stores state in SQLite, and exports CSVs.

## 1) Install Docker (Amazon Linux 2023)
```bash
sudo dnf -y install docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
newgrp docker
```

## 2) Install Docker Compose (Amazon Linux 2023)
```bash
# Install Docker Compose v2 CLI plugin (x86_64)
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/download/v2.27.1/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# If on ARM/Graviton, use the aarch64 binary instead:
# sudo curl -SL https://github.com/docker/compose/releases/download/v2.27.1/docker-compose-linux-aarch64 -o /usr/local/lib/docker/cli-plugins/docker-compose
# sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Verify Compose v2
docker compose version
```

## 3) Prepare data dir and .env (from repo root)
```bash
sudo mkdir -p /var/lib/x-likers
cat > .env << 'EOF'
CONSUMER_KEY=YOUR_CONSUMER_KEY
CONSUMER_SECRET=YOUR_CONSUMER_SECRET
ACCESS_TOKEN=YOUR_ACCESS_TOKEN
ACCESS_TOKEN_SECRET=YOUR_ACCESS_TOKEN_SECRET
TWEET_ID=1234567890123456789
# Optional
EXPORT_MODE=final
S3_URI=
EOF
```

## 4) Run with Docker Compose
```bash
docker compose up -d --build
docker compose logs -f
```

## 5) Data and resume
- Host path: `/var/lib/x-likers`
- DB: `/var/lib/x-likers/state.db` (checkpointing + dedupe)
- CSV: `{tweet_id}_likers_<epoch>.csv`

## 6) Troubleshooting (quick)
- 401/403: credentials/permissions
- 404: tweet not accessible
- 429: expected on free tier; the service waits until reset (visible countdown)

## 7) Useful checks
```bash
# Show state
docker exec -it twitter-likes-fetcher sqlite3 /data/state.db "SELECT tweet_id,next_token,done,total_users_found FROM state;"

# Count rows for the tweet
docker exec -it twitter-likes-fetcher sqlite3 /data/state.db "SELECT COUNT(*) FROM likers WHERE tweet_id=$TWEET_ID;"
```

Notes:
- Container restarts automatically (`restart: always`) and resumes from DB.
- Do not use QUICK_TEST in production. Default `max_results=100` and header-aware waits.
