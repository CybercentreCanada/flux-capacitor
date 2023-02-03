# The Flux Capacitor Aggregation Function

Now we need a means to update these tags over time. We need a way to acculate and access them later.

`flux_capacitor` is a custom aggregate functions that updates and retrieves detection flags according to a specification. The detections argument is a map of map as explained above.



Example 1: ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d
We don't need to remember recon_cmd_d so we omit it from the update specification

recon_cmd_b is only stored when a previous recon_cmd_a was true

```yaml
rules:

    - rulename: rule6
      description: >
        ordered list of events by host and for particular context key
        for example the a regex applied to a file path could capture
        a folder name and store that value under captured_folder_colname
        The flux capacitor will propagate the tags seen for folder XYZ
        to all subsequent rows which have a captured_folder_colname of XYZ
      action: temporal
      ordered: true
      groupby:
        - captured_folder_colname
      tags:
        - name: recon_cmd_a
        - name: recon_cmd_b
        - name: recon_cmd_c


```

Example 2: un-ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c
We don't need to remember recon_cmd_c, so we omit it from the update specification


Example 3: infinit ancestor

```yaml
rules:
    
    - rulename: rule3
      description: propagate to all decendents
      action: ancestor
      tags:
        - name: ancestor_process_feature1
      parent: parent_id
      child: id


```

Example 4: just check parent
```yaml
rules:
    - rulename: rule4
      description: propagate to child only
      action: parent
      parent: parent_id
      child: id
      tags:
        - name: parent_process_feature1

```

Example 5: un-ordered temporal rules, no prefix keys. Context is the entire machine.

```yaml
rules:
    - rulename: rule1
      description: ordered list of events by host
      action: temporal
      ordered: true
      tags:
        - name: recon_cmd_a
        - name: recon_cmd_b
        - name: recon_cmd_c

```

```yaml
rules:

    - rulename: rule5
      description: un-ordered set of events by host
      action: temporal
      ordered: false
      tags:
        - name: recon_cmd_a
        - name: recon_cmd_b
        - name: recon_cmd_c

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

