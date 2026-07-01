import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const FLAGSTAFF = { lat: 35.1983, lon: -111.6513 };
const AIRNOW_BBOX = "-112.5,34.5,-110.5,36.0";
const PA_BOUNDS = { nwlng: -112.3, nwlat: 37.0, selng: -110.0, selat: 34.3 };
const MAX_PA_LIST = 60;
const PA_HISTORY_BATCH = 3;
const MAX_DIST_MI = 20;
const OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast";
const WIND_COORD_PRECISION = 2; // ~1.1km grid — dedupe nearby stations onto one Open-Meteo query point
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function pm25ToAQI(pm: number): number {
  if (pm <= 0) return 0;
  const bp: [number, number, number, number][] = [
    [0.0, 12.0, 0, 50], [12.1, 35.4, 51, 100], [35.5, 55.4, 101, 150],
    [55.5, 150.4, 151, 200], [150.5, 250.4, 201, 300],
    [250.5, 350.4, 301, 400], [350.5, 500.4, 401, 500],
  ];
  const c = Math.min(pm, 500.4);
  for (const [cLo, cHi, iLo, iHi] of bp) {
    if (c <= cHi) return Math.round((iHi - iLo) / (cHi - cLo) * (c - cLo) + iLo);
  }
  return 500;
}

const toUTCStr = (ts: number) => new Date(ts * 1000).toISOString().slice(0, 16);

function degToCompass(deg: number): string {
  const dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"];
  return dirs[Math.round(deg / 22.5) % 16];
}

const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

async function fetchHistoryBatched(
  sensors: any[], startTs: number, endTs: number, average: number, apiKey: string
): Promise<PromiseSettledResult<any>[]> {
  const results: PromiseSettledResult<any>[] = [];
  for (let i = 0; i < sensors.length; i += PA_HISTORY_BATCH) {
    const batch = sensors.slice(i, i + PA_HISTORY_BATCH);
    const batchResults = await Promise.allSettled(
      batch.map((s: any) =>
        fetch(`https://api.purpleair.com/v1/sensors/${s.sensor_index}/history?${new URLSearchParams({
          start_timestamp: String(startTs), end_timestamp: String(endTs),
          average: String(average), fields: "pm2.5_cf_1",
        })}`, { headers: { "X-API-Key": apiKey } }).then(r => r.json())
      )
    );
    results.push(...batchResults);
    if (i + PA_HISTORY_BATCH < sensors.length) await delay(150);
  }
  return results;
}

function roundCoord(n: number): number {
  return Math.round(n * 10 ** WIND_COORD_PRECISION) / 10 ** WIND_COORD_PRECISION;
}

// Fetches per-location wind history from Open-Meteo (free, no API key), batched into a single
// request across all unique station coordinates so every station gets its own wind reading
// instead of one global station's wind being applied to every marker.
async function fetchWindBatched(
  locations: { lat: number; lon: number }[], pastDays: number
): Promise<Map<string, any[]>> {
  const uniq = new Map<string, { lat: number; lon: number }>();
  for (const loc of locations) {
    const key = `${roundCoord(loc.lat)},${roundCoord(loc.lon)}`;
    if (!uniq.has(key)) uniq.set(key, loc);
  }
  const keys = [...uniq.keys()];
  const lats = keys.map(k => k.split(",")[0]).join(",");
  const lons = keys.map(k => k.split(",")[1]).join(",");

  const url = `${OPEN_METEO_URL}?${new URLSearchParams({
    latitude: lats, longitude: lons,
    hourly: "wind_speed_10m,wind_direction_10m",
    wind_speed_unit: "mph",
    past_days: String(pastDays),
    forecast_days: "1",
    timezone: "UTC",
  })}`;

  const result = new Map<string, any[]>();
  try {
    const res = await fetch(url);
    const json = await res.json();
    const arr = Array.isArray(json) ? json : [json];
    arr.forEach((entry: any, i: number) => {
      const hourly = entry?.hourly;
      if (!hourly?.time) return;
      const readings = hourly.time.map((t: string, idx: number) => ({
        time: t.slice(0, 16),
        dir: Math.round(hourly.wind_direction_10m[idx]),
        compass: degToCompass(hourly.wind_direction_10m[idx]),
        speedMph: Math.round(hourly.wind_speed_10m[idx]),
      }));
      result.set(keys[i], readings);
    });
  } catch (_e) {
    // leave result empty — the frontend already handles missing wind gracefully
  }
  return result;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });

  const airnowKey = Deno.env.get("AIRNOW_API_KEY");
  const purpleairKey = Deno.env.get("PURPLEAIR_API_KEY");
  if (!airnowKey) {
    return new Response(JSON.stringify({ error: "AIRNOW_API_KEY not set" }), {
      status: 500, headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  const url = new URL(req.url);
  const period = url.searchParams.get("period") ?? "24h";

  // Check Postgres cache
  const sb = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
  );
  const { data: cacheRow } = await sb
    .from("aqi_cache")
    .select("data, cached_at")
    .eq("period", period)
    .single();

  if (cacheRow) {
    const age = Date.now() - new Date(cacheRow.cached_at).getTime();
    if (age < CACHE_TTL_MS) {
      return new Response(JSON.stringify(cacheRow.data), {
        headers: { ...CORS, "Content-Type": "application/json" },
      });
    }
  }

  const hoursBack = period === "7d" ? 168 : period === "3d" ? 72 : 24;
  const now = new Date();
  const startTime = new Date(now.getTime() - hoursBack * 3_600_000);
  const fmt = (d: Date) => d.toISOString().slice(0, 13);

  const airnowUrl = `https://www.airnowapi.org/aq/data/?${new URLSearchParams({
    startDate: fmt(startTime), endDate: fmt(now), parameters: "O3,PM25",
    BBOX: AIRNOW_BBOX, dataType: "A", format: "application/json",
    verbose: "1", monitorType: "0", includerawconcentrations: "0", API_KEY: airnowKey,
  })}`;

  const paListUrl = `https://api.purpleair.com/v1/sensors?${new URLSearchParams({
    fields: "sensor_index,name,latitude,longitude,pm2.5",
    location_type: "0",
    nwlng: String(PA_BOUNDS.nwlng), nwlat: String(PA_BOUNDS.nwlat),
    selng: String(PA_BOUNDS.selng), selat: String(PA_BOUNDS.selat),
  })}`;

  const [airnowResult, paListResult] = await Promise.allSettled([
    fetch(airnowUrl).then(r => r.json()),
    purpleairKey
      ? fetch(paListUrl, { headers: { "X-API-Key": purpleairKey } }).then(r => r.json())
      : Promise.resolve(null),
  ]);

  const sites: Record<string, any> = {};

  if (airnowResult.status === "fulfilled" && Array.isArray(airnowResult.value)) {
    for (const r of airnowResult.value) {
      if (r.AQI <= 0) continue;
      const distMi = haversine(FLAGSTAFF.lat, FLAGSTAFF.lon, r.Latitude, r.Longitude);
      if (distMi > MAX_DIST_MI) continue;
      const key = `airnow:${r.FullAQSCode || `${r.Latitude},${r.Longitude}`}`;
      if (!sites[key]) sites[key] = { name: r.SiteName, lat: r.Latitude, lon: r.Longitude, source: "airnow", readings: [], distMi };
      sites[key].readings.push({ time: r.UTC, aqi: r.AQI, parameter: r.Parameter });
    }
  }

  let nearestPA: any[] = [];
  if (paListResult.status === "fulfilled" && paListResult.value?.data) {
    const fields: string[] = paListResult.value.fields;
    const fi = (f: string) => fields.indexOf(f);
    nearestPA = paListResult.value.data
      .map((row: any[]) => ({
        sensor_index: row[fi("sensor_index")],
        name: row[fi("name")],
        lat: row[fi("latitude")],
        lon: row[fi("longitude")],
        pm: row[fi("pm2.5")],
      }))
      .filter((s: any) => s.lat && s.lon && s.pm != null && s.pm > 0)
      .map((s: any) => ({ ...s, distMi: haversine(FLAGSTAFF.lat, FLAGSTAFF.lon, s.lat, s.lon) }))
      .filter((s: any) => s.distMi <= MAX_DIST_MI)
      .sort((a: any, b: any) => a.distMi - b.distMi)
      .slice(0, MAX_PA_LIST);
  }

  if (nearestPA.length > 0 && purpleairKey) {
    const average = period === "7d" ? 1440 : 60;
    const startTs = Math.floor(startTime.getTime() / 1000);
    const endTs = Math.floor(now.getTime() / 1000);
    const histResults = await fetchHistoryBatched(nearestPA, startTs, endTs, average, purpleairKey);

    nearestPA.forEach((sensor: any, i: number) => {
      const result = histResults[i];
      const key = `purpleair:${sensor.sensor_index}`;
      if (result.status === "fulfilled" && result.value?.data?.length) {
        const hFields: string[] = result.value.fields;
        const tsIdx = hFields.indexOf("time_stamp");
        const pmIdx = hFields.indexOf("pm2.5_cf_1");
        const readings = result.value.data
          .filter((row: any[]) => row[pmIdx] != null && row[pmIdx] > 0)
          .map((row: any[]) => ({ time: toUTCStr(row[tsIdx]), aqi: pm25ToAQI(row[pmIdx]), parameter: "PM2.5" }));
        if (readings.length > 0) {
          sites[key] = { name: sensor.name, lat: sensor.lat, lon: sensor.lon, source: "purpleair", readings, distMi: sensor.distMi };
        }
      } else {
        const aqi = pm25ToAQI(sensor.pm);
        if (aqi > 0) {
          sites[key] = {
            name: sensor.name, lat: sensor.lat, lon: sensor.lon, source: "purpleair", distMi: sensor.distMi,
            readings: [{ time: toUTCStr(Math.floor(now.getTime() / 1000)), aqi, parameter: "PM2.5" }],
          };
        }
      }
    });
  }

  // Per-station wind: each station gets its own Open-Meteo history for its exact coordinates,
  // instead of one global reading applied to every station.
  const pastDays = Math.max(1, Math.ceil(hoursBack / 24));
  const siteValues = Object.values(sites) as any[];
  const windByCoord = siteValues.length
    ? await fetchWindBatched(siteValues.map(s => ({ lat: s.lat, lon: s.lon })), pastDays)
    : new Map<string, any[]>();
  for (const s of siteValues) {
    const coordKey = `${roundCoord(s.lat)},${roundCoord(s.lon)}`;
    s.wind = { hourly: windByCoord.get(coordKey) ?? [] };
  }

  const stations = Object.values(sites)
    .filter((s: any) => s.readings.length > 0)
    .map((s: any) => ({ ...s, distMi: Math.round(s.distMi) }))
    .sort((a: any, b: any) => {
      if (a.source !== b.source) return a.source === "purpleair" ? -1 : 1;
      return a.distMi - b.distMi;
    });

  const result = { stations, period, timestamp: now.toISOString() };

  // Write to Postgres cache (fire and forget — don't block the response)
  sb.from("aqi_cache")
    .upsert({ period, data: result, cached_at: now.toISOString() })
    .then(() => {});

  return new Response(
    JSON.stringify(result),
    { headers: { ...CORS, "Content-Type": "application/json" } }
  );
});
