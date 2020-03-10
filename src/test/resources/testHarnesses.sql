spark.sql("CREATE DATABASE testDB")
spark.sql("DROP TABLE IF EXISTS testDB.testTable")
spark.sql("CREATE TABLE testDB.testTableSH (value STRING, source_sys_id STRING, source_sys_inst_id STRING) PARTITIONED BY (edi_business_day timestamp)")
spark.sql("ALTER TABLE testDB.testTableSH ADD PARTITION (edi_business_day='2020-01-20')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN')")
spark.sql("INSERT INTO testDB.testTableSH PARTITION (edi_business_day='2020-01-26') VALUES ('test1','ADB','UBN')")

spark.sql("CREATE TABLE testDB.testTableEAS (value STRING) PARTITIONED BY (edi_business_day timestamp, source_sys_id STRING, source_sys_inst_id STRING)")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-20',source_sys_id='ADB',source_sys_inst_id='UBR')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-21',source_sys_id='ADB',source_sys_inst_id='UBR')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-22',source_sys_id='ADB',source_sys_inst_id='UBR')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-23',source_sys_id='ADB',source_sys_inst_id='UBR')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-20',source_sys_id='ADB',source_sys_inst_id='UBN')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-21',source_sys_id='ADB',source_sys_inst_id='UBN')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-22',source_sys_id='ADB',source_sys_inst_id='UBN')")
spark.sql("ALTER TABLE testDB.testTableEAS ADD PARTITION (edi_business_day='2020-01-23',source_sys_id='ADB',source_sys_inst_id='UBN')")

spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-20',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-21',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-22',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-23',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-20',source_sys_id='ADB',source_sys_inst_id='UBN') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-21',source_sys_id='ADB',source_sys_inst_id='UBN') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-22',source_sys_id='ADB',source_sys_inst_id='UBN') VALUES ('test1')")
spark.sql("INSERT INTO testDB.testTableEAS PARTITION (edi_business_day='2020-01-23',source_sys_id='ADB',source_sys_inst_id='UBN') VALUES ('test1')")

df=spark.sql("SELECT DISTINCT SOURCE_SYS_ID, SOURCE_SYS_INST_ID, year(EDI_BUSINESS_DAY)||'-'||month(EDI_BUSINESS_DAY)||'-'||max(day(EDI_BUSINESS_DAY)) as MONTH_END FROM testDB.testTableSH GROUP BY SOURCE_SYS_ID, SOURCE_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)")
df.createOrReplaceTempView("monthEnds")
df2=spark.sql("SELECT DISTINCT t.SOURCE_SYS_ID, t.SOURCE_SYS_INST_ID, t.EDI_BUSINESS_DAY FROM testDB.testTable t JOIN monthEnds t2 on t.EDI_BUSINESS_DAY = t2.YEAR||'-'||t2.MONTH||'-'||t2.DAY ")

spark.sql("CREATE TABLE default.data_retention_configuration (DATABASE STRING, TABLE STRING, RETENTION_PERIOD int, RETAIN_MONTH_END STRING, group INT)")
spark.sql("INSERT INTO default.data_retention_configuration VALUES ('default','testTableSH',100,'false',1),('default','testTableEAS',100,'false',2)")

