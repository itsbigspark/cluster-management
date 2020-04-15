show tables in bddlsold01n
drop table if exists bddlsold01n.sys_cm_purging

create database if not exists bddlsold01n;


drop table if exists bddlsold01n.sys_cm_purge_config;
CREATE TABLE IF NOT EXISTS  bddlsold01n.sys_cm_purge_config (db_name STRING, tbl_name STRING, retention_period INT, retain_month_end STRING, processing_group INT, active STRING);
INSERT INTO  bddlsold01n.sys_cm_purge_config VALUES ('bddlsold01n','test_table_sh',10,'true',1,'true');
INSERT INTO  bddlsold01n.sys_cm_purge_config VALUES ('bddlsold01n','test_table_sh2',10,'false',1,'true');


DROP TABLE IF EXISTS bddlsold01n.test_table_sh;
CREATE TABLE bddlsold01n.test_table_sh (value STRING, src_sys_id STRING, src_sys_inst_id STRING) PARTITIONED BY (edi_business_day string);
ALTER TABLE bddlsold01n.test_table_sh ADD PARTITION (edi_business_day='2020-01-29');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-02-28') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-02-28') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day='2020-02-29') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -11) as varchar(10))) VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -11) as varchar(10))) VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -10) as varchar(10))) VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -10) as varchar(10))) VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -9) as varchar(10))) VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -9) as varchar(10))) VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -2) as varchar(10))) VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -2) as varchar(10))) VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -1) as varchar(10))) VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01n.test_table_sh PARTITION (edi_business_day=cast(adddate(now(), -1) as varchar(10))) VALUES ('test1','ADB','UBR');


DROP TABLE IF EXISTS bddlsold01n.test_table_sh2;
CREATE TABLE bddlsold01n.test_table_sh2 (value STRING, src_sys_id STRING, src_sys_inst_id STRING) PARTITIONED BY (edi_business_day string);
insert overwrite table bddlsold01n.test_table_sh2 partition (edi_business_day) select * from bddlsold01n.test_table_sh;




invalidate metadata bddlsold01n.test_table_sh;
show partitions bddlsold01n.test_table_sh;




create external table if not exists bddlsold01n.sys_cm_purge_audit
(
id string
,db_name string
,tableName string
,partition_spec string
,originalLocation string
,trashLocation string
)


 invalidate metadata bddlsold01n.sys_cm_purge_audit;
 select * from bddlsold01n.sys_cm_purge_audit;
