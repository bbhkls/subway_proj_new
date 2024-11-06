UPDATE dbt_schema."{{ params.param1 }}"
SET actual_flg = 0
WHERE ({{ params.key_p }}, valid_from_dttm) IN 
(
SELECT 
{{ params.key_p }}, min(valid_from_dttm) 
FROM 
dbt_schema."{{ params.param1 }}"
WHERE actual_flg = 1
GROUP BY {{ params.key_p }}, actual_flg
HAVING COUNT(*) > 1
)