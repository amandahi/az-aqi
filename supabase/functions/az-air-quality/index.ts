import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const FLAGSTAFF = { lat: 35.1983, lon: -111.6513 };
const AIRNOW_BBOX = "-112.5,34.5,-110.5,36.0";
const PA_BOUNDS = { nwlng: -112.3, nwlat: 37.0, selng: -110.0, selat: 34.3 };
const MAX_PA_LIST = 60;
const PA_HISTORY_BATCH = 3;
const MAX_DIST_MI = 20;
const NWS_STATION = "KFLG";
const NWS_HEADERS = { "User-Agent": "AZAQIDashboard/1.0 (amandahi@gmail.com)", "Accept": "application/geo+json" };
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

  const nwsCurrentUrl = `https://api.weather.gov/stations/${NWS_STATION}/observations/latest`;
  const nwsHourlyUrl = `https://api.weather.gov/stations/${NWS_STATION}/observations?start=${startTime.toISOString()}&end=${now.toISOString()}`;

  const [airnowResult, paListResult, nwsCurrentResult, nwsHourlyResult] = await Promise.allSettled([
    fetch(airnowUrl).then(r => r.json()),
    purpleairKey
      ? fetch(paListUrl, { headers: { "X-API-Key": purpleairKey } }).then(r => r.json())
      : Promise.resolve(null),
    fetch(nwsCurrentUrl, { headers: NWS_HEADERS }).then(r => r.json()),
    fetch(nwsHourlyUrl, { headers: NWS_HEADERS }).then(r => r.json()),
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

  const wind: any = { current: null, hourly: [] };
  if (nwsCurrentResult.status === "fulfilled") {
    const props = nwsCurrentResult.value?.properties;
    if (props) {
      const dir = props.windDirection?.value;
      const spd = props.windSpeed?.value;
      if (dir != null && spd != null) {
        wind.current = { dir: Math.round(dir), compass: degToCompass(dir), speedKph: Math.round(spd), speedMph: Math.round(spd * 0.621371) };
      }
    }
  }
  if (nwsHourlyResult.status === "fulfilled" && nwsHourlyResult.value?.features) {
    wind.hourly = nwsHourlyResult.value.features
      .map((f: any) => {
        const p = f.properties;
        const dir = p.windDirection?.value;
        const spd = p.windSpeed?.value;
        if (dir == null || spd == null) return null;
        return { time: p.timestamp?.slice(0, 16).replace("+00:00", ""), dir: Math.round(dir), compass: degToCompass(dir), speedMph: Math.round(spd * 0.621371) };
      })
      .filter(Boolean)
      .reverse();
  }

  const stations = Object.values(sites)
    .filter((s: any) => s.readings.length > 0)
    .map((s: any) => ({ ...s, distMi: Math.round(s.distMi) }))
    .sort((a: any, b: any) => {
      if (a.source !== b.source) return a.source === "purpleair" ? -1 : 1;
      return a.distMi - b.distMi;
    });

  const result = { stations, period, timestamp: now.toISOString(), wind };

  // Write to Postgres cache (fire and forget — don't block the response)
  sb.from("aqi_cache")
    .upsert({ period, data: result, cached_at: now.toISOString() })
    .then(() => {});

  return new Response(
    JSON.stringify(result),
    { headers: { ...CORS, "Content-Type": "application/json" } }
  );
});
