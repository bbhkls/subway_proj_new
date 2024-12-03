merge into dbt_schema."GPR_EM_DIM_{{params.dim}}" dim
using {{params.vie}} vie
on vie.{{params.dim}}_rk = dim.{{params.dim}}_rk
and vie.valid_from_dttm = dim.valid_from_dttm
when matched then
	update set
	dataflow_id = vie.dataflow_id,
	dataflow_dttm = vie.dataflow_dttm,
	valid_to_dttm = vie.valid_to_dttm,
    {% for col in params.column_dim %}
        {{col}} = vie.{{col}}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
when not matched then
	insert values
	(
        vie.dataflow_id, 
        vie.dataflow_dttm, 
        vie.{{params.dim}}_rk, 
        vie.valid_from_dttm, 
        vie.valid_to_dttm, 
        {% for col in params.column_dim %}
            {{col}}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    );

update dbt_schema."GPR_EM_DIM_{{params.dim}}" dim
set valid_to_dttm = 
(
    select max(valid_from_dttm) - interval '1 minute' 
    from dbt_schema."GPR_EM_DIM_{{params.dim}}" 
    where dim.{{params.dim}}_rk = {{params.dim}}_rk
)
where (valid_from_dttm, {{params.dim}}_rk) in 
(
    select min(valid_from_dttm), {{params.dim}}_rk
    from dbt_schema."GPR_EM_DIM_{{params.dim}}"
    where valid_to_dttm = '5999-01-01' 
    group by {{params.dim}}_rk 
    having count(valid_from_dttm) > 1
);

commit;