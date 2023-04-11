
create table {{alerts_table}} (
  /* include all the columns of the original telemetry */
  {% for column_name, column_type in telemetry_schema.items() -%}
    {{column_name}} {{column_type}},
  {% endfor -%}
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
  'commit.retry.num-retries'='20',
  'write.parquet.compression-codec' = 'zstd',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '100',
  'read.split.target-size'='33554432',
  'write.parquet.row-group-size-bytes'='33554432',
  'write.spark.fanout.enabled'='true'
)
