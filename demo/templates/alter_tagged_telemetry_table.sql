ALTER TABLE {{tagged_telemetry_table}} WRITE ORDERED BY hours(timestamp), host_id