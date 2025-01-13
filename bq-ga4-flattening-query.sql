WITH BASE AS(SELECT DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Qatar') AS event_date,
                    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
                    user_id,
                    user_pseudo_id, 
                    LOWER(TRIM(event_name)) AS event_name,
                    LOWER(CAST(params.key AS STRING)) AS param_key, 
                    TRIM(LOWER(COALESCE(params.value.string_value, 
                    CAST(params.value.int_value AS STRING),
                    CAST(params.value.float_value AS STRING),
                    CAST(params.value.double_value AS STRING)))) AS param_value
              FROM `prj-dev.analytics_43070975.events_*`, UNNEST(event_params) AS params     
              WHERE PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)),

BPIVOT AS(SELECT DISTINCT event_date, event_name, user_pseudo_id,
    MAX(IF(param_key = "ga_session_id", param_value, NULL)) AS id_session,
    MAX(IF(param_key = "user_id", param_value,NULL)) AS id_user,
    MAX(IF(param_key = "event_category", param_value,NULL)) AS event_category,
    MAX(IF(param_key = "event_action", param_value,NULL)) AS event_action,
    MAX(IF(param_key = "event_label", param_value,NULL)) AS event_label,
    MAX(IF(param_key = "event_trigger", param_value,NULL)) AS event_trigger,
    MAX(IF(param_key = "language", param_value,NULL)) AS platform_language,
    MAX(IF(param_key = "engaged_session_event", param_value,NULL)) AS engaged_session_event,
    MAX(IF(param_key = "session_engaged", param_value,NULL)) AS session_engaged,
    MAX(IF(param_key = "engagement_time_msec", param_value,NULL)) AS engagement_time_msec,
    MAX(IF(param_key = "medium", param_value,NULL)) AS campaign_medium,
    MAX(IF(param_key = "campaign", param_value,NULL)) AS campaign_mame,
    MAX(IF(param_key = "source", param_value,NULL)) AS campaign_source
FROM BASE
GROUP BY 1,2,3)

SELECT *
FROM BPIVOT
