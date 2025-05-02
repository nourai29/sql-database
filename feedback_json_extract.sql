WITH BASE AS (
                SELECT  JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.message_id') AS message_id,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.user_id') AS user_id,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.user_query') AS search_query,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.ai_summary') AS summary,
                        title, description, redirect_url,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.feedback_val') AS feedback_val,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.platform') AS platform,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.language') AS language,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.total_tokens') AS total_tokens,
                        JSON_VALUE(JSON_SERIALIZE(JSON_DOCUMENT), '$.timestamp') AS timestamp,  
                FROM FEEDBACK_LOGS,
                        JSON_TABLE(JSON_SERIALIZE(JSON_DOCUMENT), '$.titles[*]' COLUMNS (
                                                                                            title VARCHAR2(4000) PATH '$.title',
                                                                                            description VARCHAR2(4000) PATH '$.description',
                                                                                            redirect_url VARCHAR2(4000) PATH '$.redirect_url'))
                ORDER BY created_on DESC
                )

SELECT *
FROM BASE
WHERE CAST(timestamp AS DATE) > DATE '2025-05-01'
