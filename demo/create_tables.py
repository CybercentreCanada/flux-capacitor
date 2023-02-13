from util import create_spark_session, alerts_table, drop, run, tagged_telemetry_table, process_telemetry_table, suspected_anomalies_table

create_spark_session("create tables", 1)

drop(tagged_telemetry_table)
drop(process_telemetry_table)
drop(suspected_anomalies_table)
drop(alerts_table)
run("create_alert_table")
run("create_tagged_telemetry_table")
run("alter_tagged_telemetry_table")
run("create_process_telemetry_table")
#run("populate_process_telemetry_table")
run("create_suspected_anomalies_table")
# run("generate_synthetic_telemetry")
