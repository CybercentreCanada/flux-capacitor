
create table {{alerts_table}} (
  {% for column_name, column_type in telemetry_schema.items() %}
    {{column_name}} {{column_type}},
   {% endfor %}
  sigma_pre_flux map<string, map<string, boolean>>,
  detection_id string,
  detection_ts timestamp,
  detection_host string,
  detection_rule_name string,
  detection_action string,
  sigma map<string, map<string, boolean>>
)
using iceberg
tblproperties (
  'commit.retry.num-retries'='20'
)
