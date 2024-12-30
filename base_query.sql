WITH FirstOpenUsers AS (
    SELECT
        "user_pseudo_id",
        TIME_FORMAT(__time, 'yyyy-MM-dd') AS first_open_time
    FROM "com_satisfy_asmr_relax_perfect_tidy_analytics_events"
    WHERE event_name = 'first_open'
    AND __time >= '{start_time}'
    AND __time < '{end_time}'
)
-- DailySessionData AS (
--   SELECT
--     le."user_pseudo_id",
--     TIMESTAMPDIFF(
--       day,
--       TIME_PARSE(fu.first_open_time, 'yyyy-MM-dd'),
--       TIME_PARSE(TIME_FORMAT(le.__time, 'yyyy-MM-dd'), 'yyyy-MM-dd')
--     ) AS day_from_install,
--     CAST(JSON_VALUE(le."event_params", '$.ga_session_number') AS INTEGER) AS session_number,
--     le.__time AS event_time,
--     CAST(COALESCE(JSON_VALUE(le."event_params", '$.level'), '0') AS INTEGER) AS level,
--     le.event_name
--   FROM "com_satisfy_asmr_relax_perfect_tidy_analytics_events" le
--   JOIN FirstOpenUsers fu ON le."user_pseudo_id" = fu."user_pseudo_id"
--   WHERE le.event_name IN ('session_start', 'level_end')
-- ),

-- SessionDurations AS (
--   SELECT
--     "user_pseudo_id",
--     day_from_install,
--     session_number,
--     MIN(CASE WHEN event_name = 'session_start' THEN event_time END) AS session_start_time,
--     MAX(CASE WHEN event_name = 'level_end' THEN event_time END) AS session_end_time
--   FROM DailySessionData
--   GROUP BY "user_pseudo_id", day_from_install, session_number
-- )
--   SELECT
--     "user_pseudo_id",
--     day_from_install,
    
--     -- Total sessions for each day (count distinct session numbers)
--     (MAX(session_number) - MIN(session_number) + 1) AS total_sessions,

--     -- Total playtime in seconds for each day by summing each session's duration
--     SUM(TIMESTAMPDIFF(MINUTE, session_start_time, session_end_time)) AS total_playtime
--   FROM SessionDurations
--   WHERE session_start_time IS NOT NULL AND session_end_time IS NOT NULL
--   GROUP BY "user_pseudo_id", day_from_install

SELECT
  le."user_pseudo_id",
  TIMESTAMPDIFF(
    day,
    TIME_PARSE(fu.first_open_time, 'yyyy-MM-dd'),
    TIME_PARSE(TIME_FORMAT(le.__time, 'yyyy-MM-dd'), 'yyyy-MM-dd')
  ) AS day_from_install,
  CAST(JSON_VALUE(le."event_params", '$.ga_session_number') AS INTEGER) AS session_number,
  APPROX_COUNT_DISTINCT(CAST(COALESCE(JSON_VALUE(le."event_params", '$.level'), '0') AS INTEGER)) AS levels_played
FROM "com_satisfy_asmr_relax_perfect_tidy_analytics_events" le
JOIN FirstOpenUsers fu ON le."user_pseudo_id" = fu."user_pseudo_id"
WHERE le.event_name IN ('session_start', 'level_end')
AND __time >= '{start_time}' 
GROUP BY 1, 2, 3
