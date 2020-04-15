package com.bigspark.cloudera.management.helpers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;

public class SparkJobDto {

  private BigInteger jobId; //" : 0,
  private String name; //" : ; //"showString at NativeMethodAccessorImpl.java:0; //",
  private String submissionTime; //" : ; //"2019-09-01T09:00:34.575GMT; //",
  private String completionTime; //" : ; //"2019-09-01T09:00:49.030GMT; //",
  private BigInteger[] stageIds; //" : [ 0 ],
  private BigInteger jobGroup; //" : ; //"1; //",
  private String status; //" : ; //"SUCCEEDED; //",
  private BigInteger numTasks; //" : 269,
  private BigInteger numActiveTasks; //" : 0,
  private BigInteger numCompletedTasks; //" : 269,
  private BigInteger numSkippedTasks; //" : 0,
  private BigInteger numFailedTasks; //" : 0,
  private BigInteger numKilledTasks; //" : 0,
  private BigInteger numCompletedIndices; //" : 269,
  private BigInteger numActiveStages; //" : 0,
  private BigInteger numCompletedStages; //" : 1,
  private BigInteger numSkippedStages; //" : 0,
  private BigInteger numFailedStages; //" : 0
  @JsonIgnore
  private String[] killedTasksSummary; //" : { }


  public SparkJobDto() {
  }

  public SparkJobDto(BigInteger jobId, String name, String submissionTime, String completionTime,
      BigInteger[] stageIds, BigInteger jobGroup, String status, BigInteger numTasks,
      BigInteger numActiveTasks, BigInteger numCompletedTasks, BigInteger numSkippedTasks,
      BigInteger numFailedTasks, BigInteger numKilledTasks, BigInteger numCompletedIndices,
      BigInteger numActiveStages, BigInteger numCompletedStages, BigInteger numSkippedStages,
      BigInteger numFailedStages, String[] killedTasksSummary) {
    this.jobId = jobId;
    this.name = name;
    this.submissionTime = submissionTime;
    this.completionTime = completionTime;
    this.stageIds = stageIds;
    this.jobGroup = jobGroup;
    this.status = status;
    this.numTasks = numTasks;
    this.numActiveTasks = numActiveTasks;
    this.numCompletedTasks = numCompletedTasks;
    this.numSkippedTasks = numSkippedTasks;
    this.numFailedTasks = numFailedTasks;
    this.numKilledTasks = numKilledTasks;
    this.numCompletedIndices = numCompletedIndices;
    this.numActiveStages = numActiveStages;
    this.numCompletedStages = numCompletedStages;
    this.numSkippedStages = numSkippedStages;
    this.numFailedStages = numFailedStages;
    this.killedTasksSummary = killedTasksSummary;
  }

  public BigInteger getJobId() {
    return this.jobId;
  }

  public void setJobId(BigInteger jobId) {
    this.jobId = jobId;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSubmissionTime() {
    return this.submissionTime;
  }

  public void setSubmissionTime(String submissionTime) {
    this.submissionTime = submissionTime;
  }

  public String getCompletionTime() {
    return this.completionTime;
  }

  public void setCompletionTime(String completionTime) {
    this.completionTime = completionTime;
  }

  public BigInteger[] getStageIds() {
    return this.stageIds;
  }

  public void setStageIds(BigInteger[] stageIds) {
    this.stageIds = stageIds;
  }

  public BigInteger getJobGroup() {
    return this.jobGroup;
  }

  public void setJobGroup(BigInteger jobGroup) {
    this.jobGroup = jobGroup;
  }

  public String getStatus() {
    return this.status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public BigInteger getNumTasks() {
    return this.numTasks;
  }

  public void setNumTasks(BigInteger numTasks) {
    this.numTasks = numTasks;
  }

  public BigInteger getNumActiveTasks() {
    return this.numActiveTasks;
  }

  public void setNumActiveTasks(BigInteger numActiveTasks) {
    this.numActiveTasks = numActiveTasks;
  }

  public BigInteger getNumCompletedTasks() {
    return this.numCompletedTasks;
  }

  public void setNumCompletedTasks(BigInteger numCompletedTasks) {
    this.numCompletedTasks = numCompletedTasks;
  }

  public BigInteger getNumSkippedTasks() {
    return this.numSkippedTasks;
  }

  public void setNumSkippedTasks(BigInteger numSkippedTasks) {
    this.numSkippedTasks = numSkippedTasks;
  }

  public BigInteger getNumFailedTasks() {
    return this.numFailedTasks;
  }

  public void setNumFailedTasks(BigInteger numFailedTasks) {
    this.numFailedTasks = numFailedTasks;
  }

  public BigInteger getNumKilledTasks() {
    return this.numKilledTasks;
  }

  public void setNumKilledTasks(BigInteger numKilledTasks) {
    this.numKilledTasks = numKilledTasks;
  }

  public BigInteger getNumCompletedIndices() {
    return this.numCompletedIndices;
  }

  public void setNumCompletedIndices(BigInteger numCompletedIndices) {
    this.numCompletedIndices = numCompletedIndices;
  }

  public BigInteger getNumActiveStages() {
    return this.numActiveStages;
  }

  public void setNumActiveStages(BigInteger numActiveStages) {
    this.numActiveStages = numActiveStages;
  }

  public BigInteger getNumCompletedStages() {
    return this.numCompletedStages;
  }

  public void setNumCompletedStages(BigInteger numCompletedStages) {
    this.numCompletedStages = numCompletedStages;
  }

  public BigInteger getNumSkippedStages() {
    return this.numSkippedStages;
  }

  public void setNumSkippedStages(BigInteger numSkippedStages) {
    this.numSkippedStages = numSkippedStages;
  }

  public BigInteger getNumFailedStages() {
    return this.numFailedStages;
  }

  public void setNumFailedStages(BigInteger numFailedStages) {
    this.numFailedStages = numFailedStages;
  }

  public String[] getKilledTasksSummary() {
    return this.killedTasksSummary;
  }

  public void setKilledTasksSummary(String[] killedTasksSummary) {
    this.killedTasksSummary = killedTasksSummary;
  }

  public SparkJobDto jobId(BigInteger jobId) {
    this.jobId = jobId;
    return this;
  }

  public SparkJobDto name(String name) {
    this.name = name;
    return this;
  }

  public SparkJobDto submissionTime(String submissionTime) {
    this.submissionTime = submissionTime;
    return this;
  }

  public SparkJobDto completionTime(String completionTime) {
    this.completionTime = completionTime;
    return this;
  }

  public SparkJobDto stageIds(BigInteger[] stageIds) {
    this.stageIds = stageIds;
    return this;
  }

  public SparkJobDto jobGroup(BigInteger jobGroup) {
    this.jobGroup = jobGroup;
    return this;
  }

  public SparkJobDto status(String status) {
    this.status = status;
    return this;
  }

  public SparkJobDto numTasks(BigInteger numTasks) {
    this.numTasks = numTasks;
    return this;
  }

  public SparkJobDto numActiveTasks(BigInteger numActiveTasks) {
    this.numActiveTasks = numActiveTasks;
    return this;
  }

  public SparkJobDto numCompletedTasks(BigInteger numCompletedTasks) {
    this.numCompletedTasks = numCompletedTasks;
    return this;
  }

  public SparkJobDto numSkippedTasks(BigInteger numSkippedTasks) {
    this.numSkippedTasks = numSkippedTasks;
    return this;
  }

  public SparkJobDto numFailedTasks(BigInteger numFailedTasks) {
    this.numFailedTasks = numFailedTasks;
    return this;
  }

  public SparkJobDto numKilledTasks(BigInteger numKilledTasks) {
    this.numKilledTasks = numKilledTasks;
    return this;
  }

  public SparkJobDto numCompletedIndices(BigInteger numCompletedIndices) {
    this.numCompletedIndices = numCompletedIndices;
    return this;
  }

  public SparkJobDto numActiveStages(BigInteger numActiveStages) {
    this.numActiveStages = numActiveStages;
    return this;
  }

  public SparkJobDto numCompletedStages(BigInteger numCompletedStages) {
    this.numCompletedStages = numCompletedStages;
    return this;
  }

  public SparkJobDto numSkippedStages(BigInteger numSkippedStages) {
    this.numSkippedStages = numSkippedStages;
    return this;
  }

  public SparkJobDto numFailedStages(BigInteger numFailedStages) {
    this.numFailedStages = numFailedStages;
    return this;
  }

  public SparkJobDto killedTasksSummary(String[] killedTasksSummary) {
    this.killedTasksSummary = killedTasksSummary;
    return this;
  }

  @Override
  public String toString() {
    return "{" +
        " jobId='" + getJobId() + "'" +
        ", name='" + getName() + "'" +
        ", submissionTime='" + getSubmissionTime() + "'" +
        ", completionTime='" + getCompletionTime() + "'" +
        ", stageIds='" + getStageIds() + "'" +
        ", jobGroup='" + getJobGroup() + "'" +
        ", status='" + getStatus() + "'" +
        ", numTasks='" + getNumTasks() + "'" +
        ", numActiveTasks='" + getNumActiveTasks() + "'" +
        ", numCompletedTasks='" + getNumCompletedTasks() + "'" +
        ", numSkippedTasks='" + getNumSkippedTasks() + "'" +
        ", numFailedTasks='" + getNumFailedTasks() + "'" +
        ", numKilledTasks='" + getNumKilledTasks() + "'" +
        ", numCompletedIndices='" + getNumCompletedIndices() + "'" +
        ", numActiveStages='" + getNumActiveStages() + "'" +
        ", numCompletedStages='" + getNumCompletedStages() + "'" +
        ", numSkippedStages='" + getNumSkippedStages() + "'" +
        ", numFailedStages='" + getNumFailedStages() + "'" +
        ", killedTasksSummary='" + getKilledTasksSummary() + "'" +
        "}";
  }

  public static SparkJobDto[] getSparkJobs(String json) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    //objectMapper.configure(DeserializationFeature., true);
    SparkJobDto[] jobs = objectMapper.readValue(json, SparkJobDto[].class);
    return jobs;
  }

}
