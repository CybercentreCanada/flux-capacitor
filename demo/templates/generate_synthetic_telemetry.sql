
select
   e.col1 as timestamp,
   host_id,
   e.col2 as id,
   e.col3 as parent_id,
   captured_folder_colname,
   Name,
   ImagePath,
   e.col4 as Commandline
from
(
    select
        *,
        explode(events) as e
    from (
        select
            'host-' || floor(rand() * 5000) as host_id,
            cast(value as string) as id,
            uuid() as captured_folder_colname,
            uuid() as Name,
            uuid() as ImagePath,
            case 
            when value % 100000 = 0 then
                array(
                    struct(cast(ts - 0.002 as timestamp), 'to' || value, '0', 'temporal_ordered_a'),
                    struct(cast(ts - 0.002 as timestamp), 't' || value, '0', 'temporal_c'),
                    struct(cast(ts - 0.001 as timestamp), 'tt' || value, '0', 'temporal_b'),
                    struct(cast(ts - 0.001 as timestamp), 'tto' || value, '0', 'temporal_ordered_b'),
                    struct(cast(ts as timestamp), 'ttt' || value, '0', 'temporal_a'),
                    struct(cast(ts as timestamp), 'ttto' || value, '0', 'temporal_ordered_c')
                )
            when value % 50000 = 0 then
                array(
                    struct(cast(ts - 0.002 as timestamp), 'aaa' || value, '0', 'ancestor_a'),
                    struct(cast(ts - 0.001 as timestamp), 'aa' || value, 'aaa' || value, 'ancestor_b'),
                    struct(cast(ts - 0.001 as timestamp), 'pp' || value, '0', 'parent_a parent_b'),
                    struct(cast(ts as timestamp), 'a' || value, 'aa' || value, 'ancestor_x'),
                    struct(cast(ts as timestamp), 'p' || value, 'pp' || value, 'parent_x')
                )
            else 
                array(struct(
                    cast(ts as timestamp),
                    cast(floor(rand() * 100000) as string),
                    cast(floor(rand() * 100000) as string),
                    uuid()))
            end as events
        from
            rate_view
        )
    )