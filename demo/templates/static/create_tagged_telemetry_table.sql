create table {{tagged_telemetry_table}} (
  {% for column_name, column_type in telemetry_schema.items() %}
    {{column_name}} {{column_type}},
  {% endfor %}
  sigma_pre_flux map<string, map<string, boolean>>
)
using iceberg
TBLPROPERTIES (
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.id' = 'true'
)
PARTITIONED BY (days(timestamp), bucket(1000, host_id))