import express from 'express';
import mqtt from 'mqtt';
import Database from 'better-sqlite3';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Config
const MQTT_HOST = process.env.MQTT_HOST || 'localhost';
const MQTT_PORT = parseInt(process.env.MQTT_PORT || '1883');
const MQTT_USERNAME = process.env.MQTT_USERNAME;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;
const PORT = parseInt(process.env.PORT || '3000');
const CAR_ID = process.env.CAR_ID || '2';
const HISTORY_HOURS = parseInt(process.env.HISTORY_HOURS || '30');
const NAV_PIN_DAYS  = parseInt(process.env.NAV_PIN_DAYS  || '10');
const DB_PATH = process.env.DB_PATH || join(__dirname, 'history.db');
const BASE_URL = process.env.BASE_URL;
const TRIP_PATH = process.env.TRIP_PATH || crypto.randomBytes(9).toString('base64url').slice(0, 12);

let vehicleName = 'Vehicle';

const HISTORY_MS  = HISTORY_HOURS * 60 * 60 * 1000;
const NAV_PIN_MS  = NAV_PIN_DAYS  * 24 * 60 * 60 * 1000;

// State
const state = {
  lat: null,
  lon: null,
  speed: null,
  heading: null,
  vehicleState: null,
  shiftState: null,
  geofence: null,
  parkDescription: null,
  destination: null,
  minutesETA: null,
  distanceMiles: null,
  trafficDelay: null,
  destLat: null,
  destLon: null,
  batteryAtArrival: null,
  battery: null,
  rangeKm: null,
};

let history = [];
let navPins = [];
let trackedDest = null;
let pendingLat = null;
let prevVehicleState = null;
let startupGeocoded = false;
const clients = new Set();

function maybeStartupGeocode() {
  if (startupGeocoded) return;
  if (!state.lat || !state.lon) return;
  if (state.geofence || state.parkDescription) return;
  const eff = effectiveState();
  if (eff !== 'parked' && eff !== 'charging') return;
  startupGeocoded = true;
  reverseGeocode(state.lat, state.lon).then((desc) => {
    if (desc) { state.parkDescription = desc; broadcast(); }
  });
}

function normalize(val) {
  if (val === null || val === undefined || val === '' || val === 'nil') return null;
  return val;
}

function parseFloat_(val) {
  const n = parseFloat(val);
  return isNaN(n) ? null : n;
}

function parseInt_(val) {
  const n = parseInt(val);
  return isNaN(n) ? null : n;
}

function formatETA(minutesFromNow) {
  const d = new Date(Date.now() + minutesFromNow * 60 * 1000);
  let h = d.getHours();
  const m = d.getMinutes().toString().padStart(2, '0');
  const ampm = h >= 12 ? 'pm' : 'am';
  h = h % 12 || 12;
  return `${h}:${m} ${ampm}`;
}

function effectiveState() {
  const vs = state.vehicleState;
  const ss = state.shiftState;
  if (vs === 'online') {
    if (ss === 'D' || ss === 'R') return 'driving';
    return 'parked';
  }
  return vs;
}

function computeStatus() {
  const vs = effectiveState();
  if (vs === 'driving') {
    if (state.destination) {
      const dist = state.distanceMiles != null ? `${state.distanceMiles.toFixed(1)} mi` : '';
      const eta = state.minutesETA != null ? `ETA ${formatETA(state.minutesETA)}` : '';
      const traffic = (state.trafficDelay != null && state.trafficDelay >= 2)
        ? `+${Math.round(state.trafficDelay)} min traffic`
        : '';
      const parts = [`En route to ${state.destination}`];
      if (dist) parts.push(dist);
      if (eta) parts.push(eta);
      if (traffic) parts.push(traffic);
      return parts.join(' · ');
    }
    return state.speed != null ? `Driving · ${Math.round(state.speed)} mph` : 'Driving';
  }
  if (vs === 'parked') {
    if (state.geofence) return `Parked at ${state.geofence}`;
    if (state.parkDescription) return `Parked near ${state.parkDescription}`;
    return 'Parked';
  }
  if (vs === 'charging') {
    const parts = ['Charging'];
    if (state.battery != null) parts.push(`${state.battery}%`);
    const loc = state.geofence || state.parkDescription;
    if (loc) parts.push(loc);
    return parts.join(' · ');
  }
  return 'Vehicle offline';
}

function decimateHistory(hist) {
  const RECENT_CUTOFF = Date.now() - 60 * 60 * 1000;
  const BUCKET_MS = 60_000;
  const result = [];
  let lastBucket = -1;
  for (const pt of hist) {
    if (pt.ts >= RECENT_CUTOFF) {
      result.push(pt);
    } else {
      const bucket = Math.floor(pt.ts / BUCKET_MS);
      if (bucket !== lastBucket) {
        result.push(pt);
        lastBucket = bucket;
      }
    }
  }
  return result;
}

function buildPayload() {
  return JSON.stringify({
    vehicleName,
    state: { ...state, vehicleState: effectiveState() },
    status: computeStatus(),
    history: decimateHistory(history),
    navPins,
  });
}

function broadcast() {
  if (clients.size === 0) return;
  const msg = `data:${buildPayload()}\n\n`;
  for (const res of clients) {
    res.write(msg);
  }
}

function recordNavPin(lat, lon, name) {
  if (lat === null || lon === null) return;
  stmtNavPinInsert.run(lat, lon, name, Date.now());
  stmtNavPinPrune.run(Date.now() - NAV_PIN_MS);
  navPins = stmtNavPinLoad.all(Date.now() - NAV_PIN_MS);
}

async function reverseGeocode(lat, lon) {
  try {
    const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=json`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'triptracker/1.0' },
    });
    if (!res.ok) return null;
    const data = await res.json();
    const a = data.address || {};
    const city = a.city || a.town || a.village;
    const state_ = a.state;
    if (a.neighbourhood && city) return `${a.neighbourhood}, ${city}`;
    if (a.suburb && city) return `${a.suburb}, ${city}`;
    if (city && state_) return `${city}, ${state_}`;
    if (a.county && state_) return `${a.county}, ${state_}`;
    return null;
  } catch {
    return null;
  }
}

// SQLite setup
const db = new Database(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS positions (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    ts  INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_positions_ts ON positions(ts);
  CREATE TABLE IF NOT EXISTS nav_pins (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    lat  REAL NOT NULL,
    lon  REAL NOT NULL,
    name TEXT,
    ts   INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_nav_pins_ts ON nav_pins(ts);
`);

const stmtInsert = db.prepare('INSERT INTO positions (lat, lon, ts) VALUES (?, ?, ?)');
const stmtPrune = db.prepare('DELETE FROM positions WHERE ts < ?');
const stmtLoad = db.prepare('SELECT lat, lon, ts FROM positions WHERE ts > ? ORDER BY ts ASC');

const stmtNavPinInsert = db.prepare('INSERT INTO nav_pins (lat, lon, name, ts) VALUES (?, ?, ?, ?)');
const stmtNavPinPrune  = db.prepare('DELETE FROM nav_pins WHERE ts < ?');
const stmtNavPinLoad   = db.prepare('SELECT lat, lon, name, ts FROM nav_pins WHERE ts > ? ORDER BY ts ASC');

// Load history from DB
const cutoff = Date.now() - HISTORY_MS;
history = stmtLoad.all(cutoff);
navPins = stmtNavPinLoad.all(Date.now() - NAV_PIN_MS);

// Seed current position from last known DB row so map shows car on startup
if (history.length > 0) {
  const last = history[history.length - 1];
  state.lat = last.lat;
  state.lon = last.lon;
}

// MQTT
const mqttOpts = { host: MQTT_HOST, port: MQTT_PORT };
if (MQTT_USERNAME) mqttOpts.username = MQTT_USERNAME;
if (MQTT_PASSWORD) mqttOpts.password = MQTT_PASSWORD;

const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}:${MQTT_PORT}`, mqttOpts);
const topicPrefix = `teslamate/cars/${CAR_ID}/`;

mqttClient.on('connect', () => {
  mqttClient.subscribe(`${topicPrefix}#`);
  startHttp();
});

mqttClient.on('message', (topic, message) => {
  const key = topic.slice(topicPrefix.length);
  const raw = message.toString();

  switch (key) {
    case 'location': {
      let loc;
      try { loc = JSON.parse(raw); } catch { break; }
      if (!loc) break;
      const lat = loc.latitude ?? null;
      const lon = loc.longitude ?? null;
      if (lat !== null) { state.lat = lat; }
      if (lon !== null) { state.lon = lon; }
      if (state.lat !== null && state.lon !== null) {
        const ts = Date.now();
        stmtInsert.run(state.lat, state.lon, ts);
        stmtPrune.run(Date.now() - HISTORY_MS);
        history = stmtLoad.all(Date.now() - HISTORY_MS);
        maybeStartupGeocode();
        broadcast();
      }
      break;
    }

    case 'speed': {
      const kph = parseFloat_(raw);
      state.speed = kph !== null ? kph * 0.621371 : null;
      break;
    }

    case 'heading':
      state.heading = parseFloat_(raw);
      break;

    case 'battery_level':
      state.battery = parseInt_(raw);
      break;

    case 'est_battery_range_km':
      state.rangeKm = parseFloat_(raw);
      break;

    case 'display_name':
      vehicleName = normalize(raw) || 'Vehicle';
      break;

    case 'geofence':
      state.geofence = normalize(raw);
      break;

    case 'state': {
      const newState = normalize(raw);
      prevVehicleState = state.vehicleState;
      state.vehicleState = newState;

      if (newState === 'driving') {
        state.parkDescription = null;
      }

      const toParkedOrCharging = (newState === 'parked' || newState === 'charging');
      const wasntAlready = prevVehicleState !== newState;
      if (toParkedOrCharging && wasntAlready && !state.geofence && state.lat !== null) {
        startupGeocoded = true;
        reverseGeocode(state.lat, state.lon).then((desc) => {
          if (desc) { state.parkDescription = desc; broadcast(); }
        });
      } else {
        maybeStartupGeocode();
      }
      broadcast();
      break;
    }

    case 'shift_state': {
      const ss = normalize(raw);
      const prev = state.shiftState;
      state.shiftState = ss;
      if ((ss === 'D' || ss === 'R') && prev !== ss) {
        state.parkDescription = null;
      }
      if (ss === 'P' && prev !== 'P' && !state.geofence && state.lat !== null) {
        startupGeocoded = true;
        reverseGeocode(state.lat, state.lon).then((desc) => {
          if (desc) { state.parkDescription = desc; broadcast(); }
        });
      } else {
        maybeStartupGeocode();
      }
      broadcast();
      break;
    }

    case 'active_route': {
      let parsed;
      try { parsed = JSON.parse(raw); } catch { break; }
      if (!parsed || parsed.error !== null) {
        if (trackedDest) {
          recordNavPin(trackedDest.lat, trackedDest.lon, trackedDest.name);
          trackedDest = null;
        }
        state.destination = null;
        state.minutesETA = null;
        state.distanceMiles = null;
        state.trafficDelay = null;
        state.destLat = null;
        state.destLon = null;
        state.batteryAtArrival = null;
      } else {
        const newLat  = parsed.location?.latitude  ?? null;
        const newLon  = parsed.location?.longitude ?? null;
        const newName = normalize(parsed.destination);
        if (trackedDest && newLat !== null && newLon !== null) {
          const latDiff = Math.abs(newLat - trackedDest.lat);
          const lonDiff = Math.abs(newLon - trackedDest.lon);
          if (latDiff > 0.001 || lonDiff > 0.001) {
            recordNavPin(trackedDest.lat, trackedDest.lon, trackedDest.name);
            trackedDest = { lat: newLat, lon: newLon, name: newName };
          }
        } else if (!trackedDest && newLat !== null && newLon !== null) {
          trackedDest = { lat: newLat, lon: newLon, name: newName };
        }
        state.destination = newName;
        state.minutesETA = parsed.minutes_to_arrival ?? null;
        state.distanceMiles = parsed.miles_to_arrival ?? null;
        state.trafficDelay = parsed.traffic_minutes_delay ?? null;
        state.destLat = newLat;
        state.destLon = newLon;
        state.batteryAtArrival = parsed.energy_at_arrival ?? null;
      }
      broadcast();
      break;
    }
  }
});

// HTTP
function startHttp() {
  const app = express();

  app.get('/health', (_req, res) => {
    res.send('OK');
  });

  app.get(`/r/${TRIP_PATH}`, (_req, res) => {
    res.sendFile(join(__dirname, 'public', 'index.html'));
  });

  app.get(`/r/${TRIP_PATH}/events`, (req, res) => {
    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'X-Accel-Buffering': 'no',
      'Connection': 'keep-alive',
    });
    res.flushHeaders();

    res.write(`data:${buildPayload()}\n\n`);

    clients.add(res);
    req.on('close', () => clients.delete(res));
  });

  app.use((_req, res) => {
    res.status(404).send();
  });

  app.listen(PORT, '0.0.0.0', () => {
    console.log('Trip tracker running.');
    console.log(`Share path: /r/${TRIP_PATH}`);
    if (BASE_URL) {
      console.log(`Full URL: ${BASE_URL}/r/${TRIP_PATH}`);
    }
  });
}
