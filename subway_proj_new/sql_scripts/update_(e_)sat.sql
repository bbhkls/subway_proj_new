/*
params.key_p - type: tuple строк - названия полей таблицы, по которым происходит группировка - однозначно определяем запись в группе
    например, 
        для M_Sattelite: 
            ("client_rk", 'phone_number_desc', 'question1_cnt', 'question2_cnt', 'question3_cnt')
        для (E_)Sattelite:
            ("client_rk",)

params.param1 - название таблицы сателита
*/

UPDATE dbt_schema."{{ params.param1 }}"
SET actual_flg = 0
WHERE ({% for attr in params.key_p %} {{ attr }} {% if not loop.last %}, {% endif %}{% endfor %}, valid_from_dttm) IN 
(
SELECT 
{% for attr in params.key_p %} {{ attr }} {% if not loop.last %}, {% endif %}{% endfor %}, min(valid_from_dttm) 
FROM 
dbt_schema."{{ params.param1 }}"
WHERE actual_flg = 1
GROUP BY {% for attr in params.key_p %} {{ attr }} {% if not loop.last %}, {% endif %}{% endfor %}, actual_flg
HAVING COUNT(*) > 1
)
