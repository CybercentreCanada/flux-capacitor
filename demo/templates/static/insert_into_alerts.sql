
insert into {{alerts_table}}
select
  {% for column_name, column_type in telemetry_schema.items() -%}
    {{column_name}},
  {% endfor -%}
  sigma_pre_flux,
  detection_id,
  detection_ts,
  detection_host,
  detection_rule_name,
  detection_action,
  sigma
from
  global_temp.validated_events
