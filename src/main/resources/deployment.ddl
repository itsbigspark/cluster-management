

create database if not exists bddlsold01d;


drop table if exists bddlsold01d.sys_cm_purging;
CREATE TABLE IF NOT EXISTS  bddlsold01d.sys_cm_purging (db_name STRING, tbl_name STRING, retention_period INT, retain_month_end STRING, processing_group INT, active STRING);
INSERT INTO  bddlsold01d.sys_cm_purging VALUES ('bddlsold01d','test_table_sh',10,'true',1,'true');
INSERT INTO  bddlsold01d.sys_cm_purging VALUES ('bddlsold01d','test_table_sh2',10,'false',1,'true');


select * from bddlsold01d.sys_cm_purging
DROP TABLE IF EXISTS bddlsold01d.test_table_sh;
CREATE TABLE bddlsold01d.test_table_sh (value STRING, src_sys_id STRING, src_sys_inst_id STRING) PARTITIONED BY (edi_business_day string);
ALTER TABLE bddlsold01d.test_table_sh ADD PARTITION (edi_business_day='2020-01-20');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh PARTITION (edi_business_day='2020-01-26') VALUES ('test1','ADB','UBN');

DROP TABLE IF EXISTS bddlsold01d.test_table_sh2;
CREATE TABLE bddlsold01d.test_table_sh2 (value STRING, src_sys_id STRING, src_sys_inst_id STRING) PARTITIONED BY (edi_business_day string);
ALTER TABLE bddlsold01d.test_table_sh2 ADD PARTITION (edi_business_day='2020-01-20');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN');
INSERT INTO bddlsold01d.test_table_sh2 PARTITION (edi_business_day='2020-01-26') VALUES ('test1','ADB','UBN');



invalidate metadata bdmsyssit01d.test_table_sh;
show partitions bdmsyssit01d.test_table_sh;

show partitions bdmsyssit01d.test_table_sh


invalidate metadata bdmsyssit01d.cm_audit;
select * from bdmsyssit01d.cm_audit;

alter table bddlsold01d.test_table_sh partition edi_business_day='2020-01-26'
