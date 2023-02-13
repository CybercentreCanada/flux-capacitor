
from util import (
    get_spark,
    create_spark_session,
    tagged_telemetry_table_only,
    catalog,
    schema,
    
)

the_hour = "1970-01-01 01:00:00"

sql = f"""
CALL {catalog}.system.rewrite_data_files(
        table => '{schema}.{tagged_telemetry_table_only}',
        strategy => 'sort', 
        sort_order => 'host_id',
        where => 'timestamp >= TIMESTAMP \\'{the_hour}\\'
            AND timestamp < TIMESTAMP \\'{the_hour}\\' + INTERVAL 1 HOUR'
    )
"""

print(sql)

create_spark_session("rewrite tagged telemetry", 1)
get_spark().sql(sql).show()

