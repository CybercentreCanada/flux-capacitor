select
   host_id,
   e.col1 as id,
   e.col2 as parent_id,
   captured_folder_colname,
   Name,
   ImagePath,
   e.col3 as Commandline
from
(
    select
        *,
        explode(events) as e
    from (
        select
            'host1' as host_id,
            cast(value as string) as id,
            uuid() as captured_folder_colname,
            uuid() as Name,
            uuid() as ImagePath,
            case 
            when value % 10 = 0 then
                array(
                    struct('p' || value, '0', 'parent_a parent_b'),
                    struct('c' || value, 'p' || value, 'parent_x')
                )
            when value % 11 = 0 then
                array(
                    struct('pp' || value, '0', 'ancestor_a'),
                    struct('p' || value, 'pp' || value, 'ancestor_b'),
                    struct('c' || value, 'p' || value, 'ancestor_x')
                )
            when value % 12 = 0 then
                array(
                    struct('ttt' || value, '0', 'temporal_c'),
                    struct('tt' || value, '0', 'ancestor_b'),
                    struct('t' || value, '0', 'temporal_a')
                )
            when value % 13 = 0 then
                array(
                    struct('ttto' || value, '0', 'temporal_ordered_a'),
                    struct('tto' || value, '0', 'temporal_ordered_b'),
                    struct('to' || value, '0', 'temporal_ordered_c')
                )
            else 
                array(struct(
                    cast(floor(rand() * 100000) as string),
                    cast(floor(rand() * 100000) as string),
                    uuid()))
            end as events
        from
            (select id as value from range(1,100))
        )
    )