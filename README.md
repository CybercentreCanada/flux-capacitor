# The Flux Capacitor Aggregation Function

Now we need a means to update these tags over time. We need a way to acculate and access them later.

`flux_capacitor` is a custom aggregate functions that updates and retrieves detection flags according to a specification. The detections argument is a map of map as explained above.

```
flux_capacitor(detections, update_specification)
```

The update_specification specifies which tags are temporal and how they should be updated.

A temporal update specification consists of 4 attributes
1) `name` the name of the tag to update
2) `prefix_get` tags are stored in a bloom filter with a key of `name`. Optionally, you can add a prefix to this key. The prefix value is obtain from the value in the specified column name.
3) `prefix_put` in some cases like parent-child relationships the get/put key prefix values will be different.
4) `put_condition` by default any tag that is true will be stored in the bloom filter. However, you can place conditions under which the tag is stored. For example, if you need set a tag only if a previous tag was seen you can specify a `put_condition`.

```
eval(tag):
    if "this" tag is not evaluated yet
        if "this" tag has a put_condition
            we will refer to the tag specified in the put_condition as "that" tag
            if "this" equals "that"
                get "that" tag from the bloom
            else
                eval("that") tag
            merge retrieved value with the current value of "that" tag
            if "that" tag is true
                put "this" tag in the bloom (honoring the put_condition)
        else
            if "this" tag is true
                put "this" tag in the bloom
            else
                get "this" tag from the bloom    
    mark "this" tag as evaluated
    
for every rule
    for every tag
        eval(tag)

```


Example 1: ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d
We don't need to remember recon_cmd_d so we omit it from the update specification

recon_cmd_b is only stored when a previous recon_cmd_a was true

```json
{
    "rule_name": "rule1"
    "tags": [
        {
            "name": "recon_cmd_a",
            "prefix_get": "captured_folder_colname",
            "prefix_put": "captured_folder_colname"
        },
        {
            "name": "recon_cmd_b",
            "prefix_get": "captured_folder_colname",
            "prefix_put": "captured_folder_colname",
            "put_condition": "recon_cmd_a"
        },
        {
            "name": "recon_cmd_c",
            "prefix_get": "captured_folder_colname",
            "prefix_put": "captured_folder_colname",
            "put_condition": "recon_cmd_b"
        },
    ]
}

```

Example 2: un-ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c
We don't need to remember recon_cmd_c, so we omit it from the update specification

```json
{
    "rule_name": "rule2"
    "tags": [
        {
            "name": "recon_cmd_a",
            "prefix_get": "captured_folder_colname",
            "prefix_put": "captured_folder_colname"
        },
        {
            "name": "recon_cmd_b",
            "prefix_get": "captured_folder_colname",
            "prefix_put": "captured_folder_colname"
        },
    ]
}
```

Example 3: infinit ancestor

```json
{    
    "rule_name": "rule3"
    "tags": [
        {
            "name": "selection_parent",
            "prefix_get": "parent_process_id",
            "prefix_put": "process_id",
            "put_condition": "selection_parent"
        },
    ]
}
```

Example 4: just check parent
```json
{
    "rule_name": "rule4"
    "tags": [
        {
            "name": "selection_parent",
            "prefix_get": "parent_process_id",
            "prefix_put": "process_id"
        },
    ]
}
```

Example 5: un-ordered temporal rules, no prefix keys. Context is the entire machine.

```json
{
    "rule_name": "rule5"
    "tags": [
        {
            "name": "recon_cmd_a",
        },
        {
            "name": "recon_cmd_b",
        },
        {
            "name": "recon_cmd_c",
        },
    ]
}
```













### We will use these 3 Sigma rules as examples of rules using parent process attributes


### RULE 1: [rules/windows/process_creation/proc_creation_win_run_executable_invalid_extension](https://github.com/SigmaHQ/sigma/blob/1fcdeffadaa01d19bbbfec2691b72612199aef70/rules/windows/process_creation/proc_creation_win_run_executable_invalid_extension.yml)

```yaml
detection:
    selection:
        Image|endswith: '\rundll32.exe'
    filter_empty:
        CommandLine: null
    filter:
        - CommandLine|contains: '.dll'
        - CommandLine: ''
    filter_iexplorer:
        ParentImage|endswith: ':\Program Files\Internet Explorer\iexplore.exe'
        CommandLine|contains: '.cpl'
    filter_msiexec_syswow64:
        ParentImage|endswith: ':\Windows\SysWOW64\msiexec.exe'
        ParentCommandLine|startswith: 'C:\Windows\syswow64\MsiExec.exe -Embedding'
    filter_msiexec_system32:
        ParentImage|endswith: ':\Windows\System32\msiexec.exe'
        ParentCommandLine|startswith: 'C:\Windows\system32\MsiExec.exe -Embedding'
    filter_splunk_ufw:
        ParentImage|endswith: ':\Windows\System32\cmd.exe'
        ParentCommandLine|contains: ' C:\Program Files\SplunkUniversalForwarder\'
    filter_localserver_fp:
        CommandLine|contains: ' -localserver '
    condition: selection and not 1 of filter*
```


### RULE 2: [rules/windows/process_creation/proc_creation_win_susp_conhost](https://github.com/SigmaHQ/sigma/blob/5e1b91a6161afc5c31337caa23b142904c94329e/rules/windows/process_creation/proc_creation_win_susp_conhost.yml)

```yaml
detection:
    selection:
        ParentImage|endswith: '\conhost.exe'
    filter_provider:
        Provider_Name: 'SystemTraceProvider-Process'  # FPs with Aurora
    filter_git:
        # Example FP:
        #   ParentCommandLine: "C:\Program Files\Git\cmd\git.exe" show --textconv :path/to/file
        Image|endswith: '\git.exe'
        ParentCommandLine|contains: ' show '
    condition: selection and not 1 of filter_*
```

### RULE 3: [rules/windows/process_creation/proc_creation_win_impacket_lateralization](https://github.com/SigmaHQ/sigma/blob/33b370d49bd6aed85bd23827aa16a50bd06d691a/rules/windows/process_creation/proc_creation_win_impacket_lateralization.yml)

```yaml
detection:
    selection_other:
        ParentImage|endswith:
            - '\wmiprvse.exe'        # wmiexec
            - '\mmc.exe'        # dcomexec MMC
            - '\explorer.exe'        # dcomexec ShellBrowserWindow
            - '\services.exe'        # smbexec
        CommandLine|contains|all:
            - 'cmd.exe'
            - '/Q'
            - '/c'
            - '\\\\127.0.0.1\'
            - '&1'
    selection_atexec:
        ParentCommandLine|contains:
            - 'svchost.exe -k netsvcs' 
            - 'taskeng.exe'
            # cmd.exe /C tasklist /m > C:\Windows\Temp\bAJrYQtL.tmp 2>&1
        CommandLine|contains|all:
            - 'cmd.exe'
            - '/C'
            - 'Windows\Temp\'
            - '&1'
    condition: 1 of selection_*
```

