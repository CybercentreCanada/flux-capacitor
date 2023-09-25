# Flux-Capacitor Spark map with group state

Sigma is a generic signature format that allows you to make detections in log events. Rules are easy to write and applicable to any type of log. Best of all, Sigma rules are abstract and not tied to any particular SIEM, making Sigma rules shareable.

Once a cyber security researcher or analyst develops detection method, they can use Sigma to describe and share their technique with others. Here’s a quote from Sigma HQ:

> Sigma is for log files what Snort is for network traffic and YARA is for files.

# Flux-Capacitor Spark map avec groupe d'état

Sigma est un format de signature generic qui permet de créer des détection dans des logs. Les regle Sigma sont facile à écrire et applicable a plusieurs type de log. Encore mieux, les règles Sigma sont abstraites et non rattachées à un SIEM particulier, ce qui en fait un format partageable.

Une fois que le chercheur en cyber security ou l'analyste développe un detection ils peuvent utiliser Sigma pour décrire la détection et partager leur technique avec d'autres. Voici une citation de Sigma HQ:

> Sigma is for log files what Snort is for network traffic and YARA is for files.

## Use Spark Streaming to run Detections

Spark Structured streaming can easily evaluate the SQL produced by the sigmac compiler. First we create a streaming dataframe by connecting to our favorite queuing mechanism (EventHubs, Kafka). In this example, we will readStream from an Iceberg table, where events are incrementally inserted into.

## Utiliser Spark Streaming to exécuter nos Détections

Spark Structured streaming peut facilement évaluer le SQL produit par le compilateur sigmac. Tout d'abord on crée un "streaming dataframe" en connectant a notre système de queue préféré (EventHubs, Kafka). Dans nos exemples on va utiliser `readStream` sur une table Iceberg ou nos événement son inserer incrémentalement.

## The Parent Process Challenge
Detecting anomalies in discrete events is relatively trivial. However, Sigma rules can correlate an event with previous ones. A classic example of this is found in Windows Security Logs (Event ID 4688). In this log source we find information about a process being created. A crucial piece of information in this log is the process that started this process. You can use these Process ID to determine what the program did while it ran etc.

## La difficulté avec le process parent

La détection d'anomalies dans des événements discrets est relativement triviale. Cependant, les règles Sigma peuvent corréler un événement avec des événements précédents. Un exemple classique de ceci se trouve dans les logs de sécurité Windows (ID d'événement 4688). Dans cette source de log, nous trouvons des informations sur un processus en cours de création. Une information cruciale dans ce journal est le processus qui a démarré ce processus. Vous pouvez utiliser ces ID de processus pour déterminer ce que le programme a fait pendant son exécution, etc.


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

### [temporal proximity](https://github.com/SigmaHQ/sigma-specification/blob/version_2/Sigma_meta_rules.md#temporal-proximity-temporal)


```yaml
action: correlation
type: temporal
rules:
    - recon_cmd_a
    - recon_cmd_b
    - recon_cmd_c
group-by:
    - ComputerName
    - User
timespan: 5m
ordered: false
```






# The Flux Capacitor Aggregation Function




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


```python

def flux_capacitor(input_df, spec, bloom_capacity, group_col):
    """Python function to invoke the scala code"""
    spark = SparkSession.builder.getOrCreate()
    flux_stateful_function = spark._sc._jvm.cccs.fluxcapacitor.FluxCapacitor.invoke
    jdf = flux_stateful_function(
            input_df._jdf, 
            group_col, 
            bloom_capacity, 
            spec)
    return DataFrame(jdf, spark)
```

```python

flux_update_spec = """
rules:
    - rulename: rule4
      description: propagate to child only
      action: parent
      parent: parent_id
      child: id
      tags:
        - name: parent_process_feature1
    - rulename: rule5
      description: un-ordered set of events by host
      action: temporal
      ordered: false
      tags:
        - name: recon_cmd_a
        - name: recon_cmd_b
        - name: recon_cmd_c
"""
df = (
        spark.readStream
        .format("kafka")
        .load()
    )

df = flux_capacitor(df, flux_update_spec, bloomsize, "group_key")

```









