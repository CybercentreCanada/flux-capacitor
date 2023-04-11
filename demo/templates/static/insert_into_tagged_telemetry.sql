
insert into {{tagged_telemetry_table}} 

    select
        {% for column_name, column_type in telemetry_schema.items() %}
            {{column_name}} {{column_type}},
        {% endfor %}

        sigma_pre_flux,

        /* if the map contains entries, then we know a temporal rule had at least one TRUE flag */
        cardinality(
            /* filter all entries keeping only the ones that are temporal
               AND the ones that have at least one tag that is TRUE */
            map_filter(sigma_pre_flux, (rule_name, tags_map) ->
                array_contains(rule_names_that_are_temporal, rule_name)
                AND array_contains(map_values(tags_map), TRUE)
            )
        ) > 0 as has_temporal_proximity_tags

    from (
        select
            *
        from
            global_temp.post_flux_eval_condition
            join (
                /* convert the sigma_rule_to_action into an array of rule names that are of temporal type */
                select
                    collect_list(detection_rule_name) as rule_names_that_are_temporal
                from
                    global_temp.sigma_rule_to_action
                where
                    detection_action = 'temporal'
            )
    )
