select
    *,
    map( -- store each resulting rule into its corresponding rule's map
        'rule1', 
        -- rule 1 -> condition: selection and not 1 of filter*
        sigma.rule1.cr1_selection AND NOT (  
                                 sigma.rule1.cr1_filter
                              OR sigma.rule1.cr1_filter_empty
                              OR sigma.rule1.cr1_filter_localserver_fp
                              OR sigma.rule1.pr1_filter_iexplorer
                              OR sigma.rule1.pr1_filter_msiexec_syswow64
                              OR sigma.rule1.pr1_filter_msiexec_system32)
        ,
        'rule2', 
        -- rule 2 -> condition: selection and not 1 of filter_*
        sigma.rule2.pr2_selection AND NOT (sigma.rule2.r2_filter_provider 
                              OR sigma.rule2.pr2_filter_git)
        ,
        'rule3',
        -- rule 3 -> condition: 1 of selection_*
        (sigma.rule3.cr3_selection_other AND sigma.rule3.pr3_selection_other)
        OR
        (sigma.rule3.cr3_selection_atexec AND sigma.rule3.pr3_selection_atexec)
        ,
        'integration_test_parent',
        sigma.integration_test_parent.a 
        AND sigma.integration_test_parent.b 
        AND sigma.integration_test_parent.x
        ,
        'integration_test_ancestor',
        sigma.integration_test_ancestor.a 
        AND sigma.integration_test_ancestor.b 
        AND sigma.integration_test_ancestor.x
        ,
        'integration_test_temporal',
        sigma.integration_test_temporal.a 
        AND sigma.integration_test_temporal.b 
        AND sigma.integration_test_temporal.c
        ,
        'integration_test_temporal_ordered',
        sigma.integration_test_temporal_ordered.a 
        AND sigma.integration_test_temporal_ordered.b 
        AND sigma.integration_test_temporal_ordered.c
    ) as sigma_final
from
    global_temp.flux_capacitor_output