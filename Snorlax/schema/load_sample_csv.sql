-- #####################################################################
-- 02_load.sql — BATCH load (offline test / history backfill). The LIVE app
-- ingests via 07 (Redpanda→ClickPipes); this is only for local runs.
-- Uses a TEMPORARY staging table so nothing extra is added to the schema.
--   clickhouse client --host <h> --user default --secure --queries-file 02_load.sql
-- #####################################################################

TRUNCATE TABLE sonyliv_concurrency.events_raw;
TRUNCATE TABLE sonyliv_concurrency.content_dim;

-- content (header: content_id,title,video_type,category)
INSERT INTO sonyliv_concurrency.content_dim
FROM INFILE 'SonyLiv/data/ch-hackathon-content-data.csv' FORMAT CSVWithNames;
SYSTEM RELOAD DICTIONARY sonyliv_concurrency.content_dict;

-- events: land in a temp table (ms epochs), then convert into events_raw
CREATE TEMPORARY TABLE _stg
(
    content_id UInt64, video_session_id String, user_id String,
    event_type LowCardinality(String), event LowCardinality(String), event_timestamp UInt64,
    platform LowCardinality(String), app_version LowCardinality(String), country LowCardinality(String),
    audio_language LowCardinality(String), subtitle_language LowCardinality(String),
    player_version LowCardinality(String), session_start_epoch UInt64
);

INSERT INTO _stg
FROM INFILE 'SonyLiv/data/ch-hackathon-raw-data.csv' FORMAT CSVWithNames;

INSERT INTO sonyliv_concurrency.events_raw
SELECT video_session_id, user_id, content_id, event_type, event,
       fromUnixTimestamp64Milli(event_timestamp, 'UTC'),
       fromUnixTimestamp64Milli(session_start_epoch, 'UTC'),
       -- same edge normalization as the live MV (config.sql); column order MUST
       -- match events_raw (platform, app_version, country, audio, subtitle, player):
       platform, norm_dim(app_version), country,
       norm_lang(audio_language), norm_lang(subtitle_language), norm_dim(player_version)
FROM _stg;

DROP TEMPORARY TABLE _stg;

-- sanity
SELECT count() AS raw_rows, uniqExact(video_session_id) AS sessions,
       min(event_timestamp) AS min_ts, max(event_timestamp) AS max_ts
FROM sonyliv_concurrency.events_raw;
