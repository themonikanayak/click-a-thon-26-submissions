-- 001_fix_content_dict_complex_key.sql
-- #####################################################################
-- Fixes content_dict on LIVE ClickHouse Cloud: it was created (out-of-band)
-- with LAYOUT(HASHED()), whose simple-key implementation silently requires
-- UInt64 keys. content_dim.content_id is Int64 and legitimately holds
-- negative placeholder IDs (review #1) — any lookup on one throws
-- "Value in column Int64 cannot be safely converted into type UInt64",
-- even through dictGetOrDefault. Reproduced live:
--   SELECT dictGetOrDefault('sonyliv_concurrency.content_dict','title',
--                            toInt64(-987654322), 'Unknown')
--
-- FIX: switch to LAYOUT(COMPLEX_KEY_HASHED()), which supports arbitrary key
-- types including negative Int64. Every dictGet/dictGetOrDefault call site
-- against content_dict must wrap the key in a tuple: tuple(content_id)
-- (already updated in 01_schema.sql / 02_seed.sql / 04_approaches.sql /
-- ui_queries.sql alongside this migration).
--
-- Idempotent: DROP IF EXISTS + CREATE is safe to re-run any number of times.
-- #####################################################################

DROP DICTIONARY IF EXISTS sonyliv_concurrency.content_dict;
CREATE DICTIONARY sonyliv_concurrency.content_dict
( content_id Int64, title String, video_type String, category String )
PRIMARY KEY content_id
SOURCE(CLICKHOUSE( USER 'default' PASSWORD '' DB 'sonyliv_concurrency' TABLE 'content_dim' ))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 600 MAX 1200);
