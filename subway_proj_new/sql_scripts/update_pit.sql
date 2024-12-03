--Обработка случаев, когда что-то изменилось
update dbt_schema."GPR_BV_P_{{params.dim}}" up_tb
set valid_to_dttm = '{{execution_date}}' :: timestamp - interval '1 minute'
where (valid_from_dttm, {{params.dim}}_rk) in
	(select min(valid_from_dttm) valid_from_dttm, {{params.dim}}_rk 
	from dbt_schema."GPR_BV_P_{{params.dim}}" 
	where valid_to_dttm  = '5999-01-01' :: timestamp 
	group by {{params.dim}}_rk 
	having count(valid_from_dttm) > 1);

--Обработка случаев, когда удалился экземпляр на всех источниках
update dbt_schema."GPR_BV_P_{{params.dim}}" up_tb
set 
	valid_to_dttm = '{{ execution_date }}',
	dataflow_dttm = '{{ execution_date }}',
	dataflow_id = '{{ run_id }}'
where 
	valid_to_dttm = '5999-01-01' and
	{{params.dim}}_rk in
	(select 
	t.{{params.dim}}_rk
	from dbt_schema."GPR_BV_A_{{params.dim}}" t
	join
		(
		{% for tbl in params.satellits %}
		select {{params.dim}}_rk, valid_from_dttm
		from dbt_schema."{{tbl[0]}}" 
		where
		{% if tbl[7:8] == 'M' %}  row_num = 1 and {% endif %}
		actual_flg = 1 
		and delete_flg = 1 
		and valid_from_dttm  = 
			(select max(valid_from_dttm) from dbt_schema."{{tbl[0]}}")
		{% if not loop.last %}union all {% endif %}
		{% endfor %}
		) t1
	on t.x_{{params.dim}}_rk = t1.{{params.dim}}_rk
	join
	(
		select t1.{{params.dim}}_rk, count(*) cnt_obj
		from dbt_schema."GPR_BV_A_{{params.dim}}" t1
		join
			(
			{% for tbl in params.satellits %}
			select
				{{params.dim}}_rk, 
				{% for col in tbl[1] %}
				{{col}} {% if not loop.last %}, {% endif %}
				{% endfor %}
			from dbt_schema."{{tbl[0]}}" where actual_flg = 1 
			{% if tbl[7:8] == 'M' %} and row_num = 1 and {% endif %}
			{% if not loop.last %} union all {% endif %}
			{% endfor %}
			) t2
			on t1.x_{{params.dim}}_rk = t2.{{params.dim}}_rk
			group by t1.{{params.dim}}_rk, 
			{% for col in params.satellits[0][1] %}
				t2.{{col}} {% if not loop.last %}, {% endif %}
			{% endfor %}
	) t2
	on t.{{params.dim}}_rk = t2.{{params.dim}}_rk
	group by t.{{params.dim}}_rk, cnt_obj having count(valid_from_dttm) = cnt_obj); 

commit;