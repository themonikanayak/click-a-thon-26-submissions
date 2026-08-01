-- [WRITE — seed] step 02 of the offline pipeline.
-- #####################################################################
-- 02_seed.sql — data to build the offline pipeline on. TWO independent
-- seed sources live here; run ONE of them (they are alternatives, not
-- additive):
--
--   SECTION A — SYNTHETIC SMOKE SEED (active/default). A few inline sessions
--     with now()-relative timestamps + their content mapping. Needs no external
--     files, so this is what `run_sql.py --all` executes to smoke-test the whole
--     pipeline end to end.
--
--   SECTION B — REAL CSV BATCH LOAD (offline / history backfill). Loads the
--     hackathon CSVs via FROM INFILE. Commented out by default: it needs
--     SonyLiv/data/*.csv present and it TRUNCATEs + replaces whatever Section A
--     seeded. Uncomment it (and comment out Section A) — or run this file with
--     that section active — for a real-data offline build. The LIVE app ingests
--     via ClickPipes (Redpanda) and needs neither section.
--
--   clickhouse client --host <h> --user default --secure --queries-file 02_seed.sql
-- Run AFTER 01_schema.sql.
-- #####################################################################


-- =====================================================================
-- SECTION A — SYNTHETIC SMOKE SEED  (active; used by --all)
-- =====================================================================

-- A1) CONTENT MAPPING (+ dictionary reload) ------------------------------------
-- Content the synthetic sessions below reference (100-102). The 1001-3004 rows
-- match the LIVE producer's content_ids so dictGet('title',...) resolves for a
-- live deployment too; harmless extra rows for the offline smoke test.
INSERT INTO sonyliv_concurrency.content_dim (content_id, title, video_type, category) VALUES
  (100,  'Live Match A', 'live', 'sports'),
  (101,  'Movie B',      'vod',  'drama'),
  (102,  'Show C',       'vod',  'comedy'),
  (1001, 'Live Match 1', 'live', 'sports'),
  (1002, 'Live Match 2', 'live', 'sports'),
  (1003, 'Live Match 3', 'live', 'sports'),
  (2001, 'Movie A',      'vod',  'drama'),
  (2002, 'Movie B',      'vod',  'drama'),
  (3001, 'Show A',       'vod',  'comedy'),
  (3002, 'Show B',       'vod',  'comedy'),
  (3003, 'Show C',       'vod',  'comedy'),
  (3004, 'Show D',       'vod',  'comedy');

SYSTEM RELOAD DICTIONARY sonyliv_concurrency.content_dict;

-- verify the mapping/dictionary resolves
SELECT 100 AS content_id,
       dictGet('sonyliv_concurrency.content_dict','title',      toInt64(100)) AS title,
       dictGet('sonyliv_concurrency.content_dict','video_type', toInt64(100)) AS video_type,
       dictGet('sonyliv_concurrency.content_dict','category',   toInt64(100)) AS category;

-- A2) SAMPLE EVENTS (last ~8 min, so the live MVs pick them up) -----------------
-- Session A: content 100, plays throughout, still open (no end)
-- Session B: content 100, pauses min-4→min-3 (excluded), then ends
-- Session C: content 101, plays then ends
INSERT INTO sonyliv_concurrency.events_raw
  (video_session_id, user_id, content_id, event_type, event, event_timestamp, session_start_epoch,
   platform, app_version, country, audio_language, subtitle_language, player_version)
VALUES
  ('sA','uA',100,'VideoSessionStart','VideoSessionStart', now64(3)-INTERVAL 8 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sA','uA',100,'VideoPlay','Play',                       now64(3)-INTERVAL 8 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sA','uA',100,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 7 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sA','uA',100,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 5 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sA','uA',100,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 3 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sA','uA',100,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 1 MINUTE, now64(3)-INTERVAL 8 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),

  ('sB','uB',100,'VideoSessionStart','VideoSessionStart', now64(3)-INTERVAL 6 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),
  ('sB','uB',100,'VideoPlay','Play',                       now64(3)-INTERVAL 6 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),
  ('sB','uB',100,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 5 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),
  ('sB','uB',100,'VideoHeartbeat','pause',                 now64(3)-INTERVAL 4 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),
  ('sB','uB',100,'VideoHeartbeat','resume',                now64(3)-INTERVAL 3 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),
  ('sB','uB',100,'VideoSessionEnd','VideoSessionEnd',      now64(3)-INTERVAL 1 MINUTE, now64(3)-INTERVAL 6 MINUTE,'IPHONE','1.0','india','eng','unk','1.0'),

  ('sC','uC',101,'VideoSessionStart','VideoSessionStart', now64(3)-INTERVAL 5 MINUTE, now64(3)-INTERVAL 5 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sC','uC',101,'VideoPlay','Play',                       now64(3)-INTERVAL 5 MINUTE, now64(3)-INTERVAL 5 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sC','uC',101,'VideoHeartbeat','buffer-health',         now64(3)-INTERVAL 4 MINUTE, now64(3)-INTERVAL 5 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0'),
  ('sC','uC',101,'VideoSessionEnd','VideoSessionEnd',      now64(3)-INTERVAL 2 MINUTE, now64(3)-INTERVAL 5 MINUTE,'ANDROID_PHONE','1.0','india','hin','unk','1.0');

-- A3) populate the serving layer from the seed
SYSTEM REFRESH VIEW sonyliv_concurrency.mv_session_intervals;
SYSTEM WAIT VIEW    sonyliv_concurrency.mv_session_intervals;
SYSTEM REFRESH VIEW sonyliv_concurrency.concurrency_hot_abs_mv;
SYSTEM WAIT VIEW    sonyliv_concurrency.concurrency_hot_abs_mv;

-- A4) smoke test — should show a concurrency curve for the last few minutes
SELECT minute, sum(concurrent) AS concurrency
FROM sonyliv_concurrency.concurrency_now
GROUP BY minute ORDER BY minute;


-- =====================================================================
-- SECTION B — REAL CSV BATCH LOAD  (offline / history; DISABLED by default)
-- =====================================================================
-- Uncomment this block (and comment out Section A) to load the hackathon CSVs.
-- Uses a TEMPORARY staging table so nothing extra is added to the schema.
-- Requires SonyLiv/data/ch-hackathon-*.csv relative to the client's CWD.
--
-- TRUNCATE TABLE sonyliv_concurrency.events_raw;
-- TRUNCATE TABLE sonyliv_concurrency.content_dim;
--
-- -- content (header: content_id,title,video_type,category)
-- INSERT INTO sonyliv_concurrency.content_dim
-- FROM INFILE 'SonyLiv/data/ch-hackathon-content-data.csv' FORMAT CSVWithNames;
-- SYSTEM RELOAD DICTIONARY sonyliv_concurrency.content_dict;
--
-- -- events: land in a temp table (ms epochs), then convert into events_raw
-- CREATE TEMPORARY TABLE _stg
-- (
--     content_id Int64, video_session_id String, user_id String,
--     event_type LowCardinality(String), event LowCardinality(String), event_timestamp UInt64,
--     platform LowCardinality(String), app_version LowCardinality(String), country LowCardinality(String),
--     audio_language LowCardinality(String), subtitle_language LowCardinality(String),
--     player_version LowCardinality(String), session_start_epoch UInt64
-- );
--
-- INSERT INTO _stg
-- FROM INFILE 'SonyLiv/data/ch-hackathon-raw-data.csv' FORMAT CSVWithNames;
--
-- INSERT INTO sonyliv_concurrency.events_raw
-- SELECT video_session_id, user_id, content_id, event_type, event,
--        fromUnixTimestamp64Milli(event_timestamp, 'UTC'),
--        fromUnixTimestamp64Milli(session_start_epoch, 'UTC'),
--        -- same edge normalization as the live MV (00_config.sql); column order MUST
--        -- match events_raw (platform, app_version, country, audio, subtitle, player):
--        platform, norm_dim(app_version), country,
--        norm_lang(audio_language), norm_lang(subtitle_language), norm_dim(player_version)
-- FROM _stg;
--
-- DROP TEMPORARY TABLE _stg;
--
-- -- sanity
-- SELECT count() AS raw_rows, uniqExact(video_session_id) AS sessions,
--        min(event_timestamp) AS min_ts, max(event_timestamp) AS max_ts
-- FROM sonyliv_concurrency.events_raw;
