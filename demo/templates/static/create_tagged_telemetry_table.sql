create table {{tagged_telemetry_table}} (
  /* include all the columns of the original telemetry */
  {% for column_name, column_type in telemetry_schema.items() -%}
    {{column_name}} {{column_type}},
  {% endfor -%}
  /* add tests performed by pre-flux statement */
  sigma_pre_flux map<string, map<string, boolean>>,
  /* add flag indicating if any of the temporal proximity tests are positive */
  has_temporal_proximity_tags boolean
)
using iceberg
TBLPROPERTIES (
  'write.metadata.compression-codec'='gzip',
  'commit.retry.num-retries'='20',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.id' = 'true',
  'write.parquet.bloom-filter-enabled.column.host_id' = 'true',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '100',
  'read.split.target-size'='33554432',
  'write.parquet.row-group-size-bytes'='33554432',
  'write.target-file-size-bytes'='33554432'

)
PARTITIONED BY (days(timestamp))