/*
params.sat_table - название таблицы, где собрана информация со всех satellite по сущности клиент
params.logic_key - логический ключ сущности для (пере)унификации
params.dim - имя сущности
*/

MERGE INTO dbt_schema."GPR_BV_A_{{params.dim}}" sal
USING 
	(select 
		dataflow_id,
	    dataflow_dttm,
	    {{params.dim}}_rk,
	    x_{{params.dim}}_rk
	from
		(
		select 
			distinct s1.dataflow_id, s1.dataflow_dttm, s1.{{params.dim}}_rk x_{{params.dim}}_rk,
			first_value
				(s2.{{params.dim}}_rk) 
				over(partition by s1.{{params.dim}}_rk 
				order by 'array_position(array[3515641477,107023,2], s2.source_system_dk)', s2.{{params.dim}}_rk
				rows between unbounded preceding and unbounded following) 
				{{params.dim}}_rk
		from 
			{{params.sat_table}} s1
			join 
			{{params.sat_table}} s2
			on {% for col in params.logic_key %} 
	                s1.{{ col }} = s2.{{ col }} 
	                {% if not loop.last %} and {% endif %} 
	            {% endfor %}
		))sats
ON sal.x_{{params.dim}}_rk = sats.x_{{params.dim}}_rk
WHEN matched and sal.{{params.dim}}_rk != sats.{{params.dim}}_rk then 
  UPDATE SET {{params.dim}}_rk = sats.{{params.dim}}_rk, dataflow_id = sats.dataflow_id, dataflow_dttm = sats.dataflow_dttm
WHEN NOT MATCHED THEN
  INSERT 
  VALUES (sats.dataflow_id, sats.dataflow_dttm, sats.{{params.dim}}_rk, sats.x_{{params.dim}}_rk);
	
commit;