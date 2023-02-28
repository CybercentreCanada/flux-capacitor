
select
   event.ts as timestamp,
   host_id,
   event.id,
   event.parent_id,
   captured_folder_colname,
   Name,
   ImagePath,
   event.Commandline
from
(
    select
        *,
        explode(gen_events) as event
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
                    struct(cast(cast(timestamp as long) - 0.002 as timestamp) as ts,  'to' || value as id, '0' as parent_id, 'temporal_ordered_a' as Commandline),
                    struct(cast(cast(timestamp as long) - 0.002 as timestamp) as ts,   't' || value as id, '0' as parent_id,         'temporal_c' as Commandline),
                    struct(cast(cast(timestamp as long) - 0.001 as timestamp) as ts,  'tt' || value as id, '0' as parent_id,         'temporal_b' as Commandline),
                    struct(cast(cast(timestamp as long) - 0.001 as timestamp) as ts, 'tto' || value as id, '0' as parent_id, 'temporal_ordered_b' as Commandline),
                    struct(                                         timestamp as ts, 'ttt' || value as id, '0' as parent_id,         'temporal_a' as Commandline),
                    struct(                                         timestamp as ts,'ttto' || value as id, '0' as parent_id, 'temporal_ordered_c' as Commandline)
                )
            when value % 50000 = 0 then
                array(
                    struct(cast(cast(timestamp as long) - 0.002 as timestamp) as ts,  'aaa' || value as id,            '0' as parent_id,        'ancestor_a' as Commandline),
                    struct(cast(cast(timestamp as long) - 0.001 as timestamp) as ts,   'aa' || value as id, 'aaa' || value as parent_id,        'ancestor_b' as Commandline),
                    struct(cast(cast(timestamp as long) - 0.001 as timestamp) as ts,   'pp' || value as id,            '0' as parent_id, 'parent_a parent_b' as Commandline),
                    struct(                                         timestamp as ts,    'a' || value as id,  'aa' || value as parent_id,        'ancestor_x' as Commandline),
                    struct(                                         timestamp as ts,    'p' || value as id,  'pp' || value as parent_id,          'parent_x' as Commandline)
                )
            else 
                array(struct(
                    timestamp                              as ts,
                    cast(floor(rand() * 100000) as string) as id,
                    cast(floor(rand() * 100000) as string) as parent_id,
                    uuid()                                 as Commandline ))
            end as gen_events
        from
            rate_view
        )
    )