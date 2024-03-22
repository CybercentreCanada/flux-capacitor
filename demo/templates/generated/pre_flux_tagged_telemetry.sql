select
    *,
    sigma as sigma_pre_flux
from
(
select
    *,
    -- regroup each rule's tags in a map (ruleName -> Tags)
    map(
        'rule1',
        map(
            'cr1_selection', ImagePath ilike '%\\\\rundll32.exe',
            'cr1_filter_empty', Commandline is null,
            'cr1_filter', Commandline ilike '%.dll%' OR Commandline = '',
            'cr1_filter_localserver_fp', Commandline ilike '% -localserver %',
            'pr1_filter_iexplorer',
            (
                ImagePath ilike '%:\\\\Program Files\\\\Internet Explorer\\\\iexplore.exe'
                AND CommandLine ilike '%.cpl%'
            ),
            'pr1_filter_msiexec_syswow64',
            (
                ImagePath ilike '%:\\\\Windows\\\\SysWOW64\\\\msiexec.exe'
                AND CommandLine ilike 'C:\\\\Windows\\\\syswow64\\\\MsiExec.exe -Embedding%'
            ),
            'pr1_filter_msiexec_system32',
            (
                ImagePath ilike '%:\\\\Windows\\\\System32\\\\msiexec.exe'
                AND CommandLine ilike 'C:\\\\Windows\\\\system32\\\\MsiExec.exe -Embedding%'
            )
        ),
        'rule2',
        map(
            'cr2_filter_provider', Name = 'SystemTraceProvider-Process',
            'cr2_filter_git', ImagePath ilike '%\\\\git.exe',
            'pr2_selection', ImagePath ilike '%\\\\conhost.exe',
            'pr2_filter_git', CommandLine ilike '% show %'
        ),
        'rule3',
        map(
            'cr3_selection_other',
            (
                CommandLine ilike '%cmd.exe%'
                and CommandLine ilike '%/Q%'
                and CommandLine ilike '%/c%'
                and CommandLine ilike '%\\\\\\\\127.0.0.1\\\\%'
                and CommandLine ilike '%&1%'
            ),
            'cr3_selection_atexec', CommandLine rlike('svchost.exe -k netsvcs|taskeng.exe'),
            'pr3_selection_other', ImagePath rlike('.*(\\\\wmiprvse.exe|\\\\mmc.exe|\\\\explorer.exe|\\\\services.exe)'),
            'pr3_selection_atexec',
            (
                CommandLine ilike '%cmd.exe%'
                and CommandLine ilike '%/C%'
                and CommandLine ilike '%Windows\\\\Temp\\\\%'
                and CommandLine ilike '%&1%'
            )
         ),
        'integration_test_parent',
        map(
            'x', CommandLine ilike '%parent_x%',
            'a', CommandLine ilike '%parent_a%',
            'b', CommandLine ilike '%parent_b%'
        ),
        'integration_test_ancestor',
        map(
            'x', CommandLine ilike '%ancestor_x%',
            'a', CommandLine ilike '%ancestor_a%',
            'b', CommandLine ilike '%ancestor_b%'
        ),
        'integration_test_temporal',
        map(
            'a', CommandLine ilike '%temporal_a%',
            'b', CommandLine ilike '%temporal_b%',
            'c', CommandLine ilike '%temporal_c%'
        ),
        'integration_test_temporal_ordered',
        map(
            'a', CommandLine ilike '%temporal_ordered_a%',
            'b', CommandLine ilike '%temporal_ordered_b%',
            'c', CommandLine ilike '%temporal_ordered_c%'
        ),
        'integration_test_nested_fields',
        map(
            'child.tag', child.tag ilike 'tag',
            'parent.tag', parent.tag ilike 'tag'
        )
    ) as sigma
from
    process_telemetry_view 
)