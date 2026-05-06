# triptracker

Live vehicle location sharing for Tesla via [TeslaMate](https://github.com/teslamate-org/teslamate). Subscribes to TeslaMate's MQTT broker, tracks position history in SQLite, and serves a shareable map page over Server-Sent Events.

## Prerequisites

- TeslaMate running with MQTT enabled
- Docker + Docker Compose

## Quick start

Create a `docker-compose.yml` and a `.env` file, then `docker compose up -d`.

### docker-compose.yml

```yaml
services:
  triptracker:
    image: node:20-alpine
    working_dir: /app
    command: sh -c "apk add --no-cache python3 make g++ && npm install && node server.js"
    ports:
      - "8055:3000"
    volumes:
      - ./triptracker:/app
    env_file:
      - .env
    restart: unless-stopped

    # Optional: Traefik reverse proxy labels (remove ports: above if using this)
    # labels:
    #   traefik.enable: "true"
    #   traefik.http.routers.triptracker.entrypoints: websecure
    #   traefik.http.routers.triptracker.rule: "Host(`roadtrip.example.com`)"
    #   traefik.http.routers.triptracker.tls.certresolver: le
    #   traefik.http.services.triptracker.loadbalancer.server.port: 3000
    #   traefik.http.services.triptracker.loadbalancer.responseForwarding.flushInterval: -1
    # networks:
    #   - web
    #   - default

# networks:
#   web:
#     external: true
```

### .env

```env
MQTT_HOST=192.168.1.100
MQTT_USERNAME=teslamate
MQTT_PASSWORD=changeme
CAR_ID=1
TRIP_PATH=mysecretpath
HISTORY_HOURS=24
BASE_URL=http://localhost:8055
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | TeslaMate MQTT broker host |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | — | MQTT username (if required) |
| `MQTT_PASSWORD` | — | MQTT password (if required) |
| `CAR_ID` | `2` | TeslaMate car ID |
| `TRIP_PATH` | random | URL slug for the share link — set this to something hard to guess |
| `HISTORY_HOURS` | `30` | How many hours of position history to retain |
| `NAV_PIN_DAYS` | `10` | Days to retain navigation destination pins on the map |
| `BASE_URL` | — | Public base URL; used to print the full share link on startup |
| `PORT` | `3000` | Internal HTTP port |
| `DB_PATH` | `./history.db` | SQLite database path |

## Share link

On startup the container logs the share path (and full URL if `BASE_URL` is set):

```
Trip tracker running.
Share path: /r/mysecretpath
Full URL: http://localhost:8055/r/mysecretpath
```

Send that URL to anyone who should be able to follow along. There is no authentication — keep `TRIP_PATH` unguessable or put it behind a reverse proxy.
