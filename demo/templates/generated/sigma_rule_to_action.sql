SELECT *
  FROM (VALUES ('rule1','parent')
             , ('rule2','parent')
             , ('rule3','parent')
             , ('integration_test_parent','parent')
             , ('integration_test_ancestor','ancestor')
             , ('integration_test_temporal','temporal')
             , ('integration_test_temporal_ordered','temporal')
       ) t1 (detection_rule_name, detection_action)