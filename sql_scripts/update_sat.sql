UPDATE dbt_schema."GPR_RV_S_CLIENT"
SET actual_flg = 0
WHERE (client_rk, valid_from_dttm) IN 
(
SELECT 
client_rk, min(valid_from_dttm) 
FROM 
dbt_schema."GPR_RV_S_CLIENT"
WHERE actual_flg = 1
GROUP BY client_rk, actual_flg
HAVING COUNT(*) > 1
)