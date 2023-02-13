/* Schema of process table

create table {{process_telemetry_table}} (
  timestamp timestamp,
  host_id string,
  id string,
  parent_id string,
  captured_folder_colname string,
  Name string,
  ImagePath string,
  Commandline string
)
using iceberg
*/


insert into {{process_telemetry_table}}
values
(TIMESTAMP '2022-12-30 00:00:01','1000','1','0','f','','','parent_a parent_b'),
(TIMESTAMP '2022-12-30 00:00:02','1000','2','1','f','','','parent_x'),

(TIMESTAMP '2022-12-30 00:00:03','1000','10','0','f','','','ancestor_a ancestor_b'),
(TIMESTAMP '2022-12-30 00:00:04','1000','20','10','f','','','ancestor_b'),
(TIMESTAMP '2022-12-30 00:00:05','1000','30','20','f','','','ancestor_x'),

(TIMESTAMP '2022-12-30 00:00:06','1000','70','0','f','','','temporal_c'),
(TIMESTAMP '2022-12-30 00:00:07','1000','80','0','f','','','temporal_b'),
(TIMESTAMP '2022-12-30 00:00:08','1000','90','0','f','','','temporal_a'),

(TIMESTAMP '2022-12-25 00:00:01','1000','11','0','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:02','1000','2','0','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:03','1000','12','11','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:04','1000','4','0','folderA','x','',''),
(TIMESTAMP '2022-12-25 00:00:05','1000','5','0','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:06','1000','100','10','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:07','1000','200','0','folderA','','',''),
(TIMESTAMP '2022-12-25 00:00:08','1000','201','200','x','','xyz\.exe','hum  hum'),
(TIMESTAMP '2022-12-25 00:00:09','1000','202','201','x','','',''),
(TIMESTAMP '2022-12-25 00:00:10','1000','203','202','x','','c:\\\\.exe','c:\\.exe'),
(TIMESTAMP '2022-12-25 00:00:11','1000','7','0','folderA','','','')