with 
exploded_results as (
select
  {% for column_name, column_type in telemetry_schema.items() %}
    {{column_name}} {{column_type}},
  {% endfor %}
  sigma_pre_flux,
  uuid() as detection_id,
  timestamp as detection_ts, 
  host_id as detection_host, 
  explode(map_keys(map_filter(sigma_final, (k,v) -> v = TRUE))) as detection_rule_name
from
  global_temp.post_flux_eval_condition
)

insert into {{suspected_anomalies_table}} 
select 
  a.*,
  r.detection_action
from 
    exploded_results as a 
    join global_temp.sigma_rule_to_action as r
    on a.detection_rule_name = r.detection_rule_name
