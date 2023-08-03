
create table {{process_telemetry_table}} (
   {%- for column_name, column_type in telemetry_schema.items() -%}
    {{column_name}} {{column_type}} {{ "," if not loop.last }}
   {% endfor -%}
)
using iceberg
TBLPROPERTIES (
  'write.metadata.compression-codec'='gzip',
  'commit.retry.num-retries'='20',
  'write.parquet.compression-codec' = 'zstd',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '100',
  'read.split.target-size'='33554432',
  'write.parquet.row-group-size-bytes'='33554432',
  'write.spark.fanout.enabled'='true'

)