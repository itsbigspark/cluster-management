package com.bigspark.cloudera.management.common.model;

public class EDIPublishMessage {

  String feedName;
  String dbName;
  String tableName;
  String businessDate;
  String sourceSysID;
  String instanceID;

  public EDIPublishMessage(String feedName, String dbName, String tableName, String businessDate,
      String sourceSysID, String instanceID) {
    this.feedName = feedName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.businessDate = businessDate;
    this.sourceSysID = sourceSysID;
    this.instanceID = instanceID;
  }

  @Override
  public String toString() {
    return "Message{" +
        "feedName='" + feedName + '\'' +
        ", dbName='" + dbName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", businessDate='" + businessDate + '\'' +
        ", sourceSysID='" + sourceSysID + '\'' +
        ", instanceID='" + instanceID + '\'' +
        '}';
  }
}
