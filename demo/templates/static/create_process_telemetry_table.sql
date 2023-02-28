
create table {{process_telemetry_table}} (
   {% for column_name, column_type in telemetry_schema.items() %}
    {{column_name}} {{column_type}} {{ "," if not loop.last }}
   {% endfor %}
)
using iceberg