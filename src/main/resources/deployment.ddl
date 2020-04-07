create database if not exists bddlsold01d

drop table if exists bdmsyssit01d.sys_cm_housekeeping;
CREATE TABLE IF NOT EXISTS  bdmsyssit01d.sys_cm_housekeeping (db_name STRING, tbl_name STRING, retention_period INT, retain_month_end STRING, processing_group INT, active STRING);

INSERT INTO  bdmsyssit01d.sys_cm_housekeeping VALUES ('bdmsyssit01d','test_table_sh',10,'true',1,'true');

DROP TABLE IF EXISTS bdmsyssit01d.test_table_sh;
CREATE TABLE bdmsyssit01d.test_table_sh (value STRING, src_sys_id STRING, src_sys_inst_id STRING) PARTITIONED BY (edi_business_day string);
ALTER TABLE bdmsyssit01d.test_table_sh ADD PARTITION (edi_business_day='2020-01-20');

INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN');
INSERT INTO bdmsyssit01d.test_table_sh PARTITION (edi_business_day='2020-01-26') VALUES ('test1','ADB','UBN');