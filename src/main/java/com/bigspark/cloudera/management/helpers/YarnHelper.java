package com.bigspark.cloudera.management.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.util.TimeZone;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * Class to Help manage spark-yarn jobs - Will kill idle jobs
 * @author Rich Hay
 */
public class YarnHelper {
    private static final Log LOGGER = LogFactory.getLog(YarnHelper.class);
    private static int minsToKill = 120;
    /**
     * Entry point to manage Livy Spark apps on Yarn
     * @param args - Arguments to be parsed
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        manageLivySparkApps(args);
    }

    /**
     * Method to manage Livy Spark Applications
     * Currently Deletes Sessions idle for > 120 minutes
     * @param args
     * @throws Exception
     */
    private static void manageLivySparkApps(String[] args) throws Exception {
        LOGGER.info("Starting to manage Livy-Spark applications");
        try (YarnClient yarn = getYarnClient()) {
            yarn.start();
            for (ApplicationReport report : yarn.getApplications(EnumSet.of(YarnApplicationState.RUNNING))) {
                if (isApplicationLivySpark(report)) {
                    LOGGER.info(String.format("Found Livy-Spark application '%s' for evaluation",
                            report.getApplicationId()));
                    if (isKillIdleApplication_NEW(report)) {
                        try {
                            LOGGER.info(String.format(
                                    "Killing Livy-Spark application '%s'",
                                    report.getApplicationId().toString()));
                            yarn.killApplication(report.getApplicationId());
                        } catch (Exception e) {
                            LOGGER.error(String.format(
                                    "Failed to kill Livy-Spark application '%s' due to unexpected Exception",
                                    report.getApplicationId()), e);
                        }
                    } else {
                        LOGGER.debug(String.format(
                                "Skipping Livy-Spark application '%s'",
                                report.getApplicationId().toString()));
                    }
                }
            }
            yarn.stop();
        }
        LOGGER.info("Finished managing Livy-Spark applications");
    }

    /**
     * Method to get a Yarn Client instance
     * Note it is expected that the required configs are on the class path
     * @return Yarn Client
     */
    private static YarnClient getYarnClient() {
        Configuration hconf = new Configuration();
        YarnConfiguration conf = new YarnConfiguration(hconf);
        YarnClient yarn = YarnClient.createYarnClient();
        yarn.init(conf);
        return yarn;
    }

    /**
     * Method to determin whether an application is a Spark/Livy application
     * @param report
     * @return
     */
    private static Boolean isApplicationLivySpark(ApplicationReport report) {
        return report.getApplicationType().equals("SPARK") && report.getName().startsWith("livy");
    }


    // http://ecpvm003484.server.rbsgrp.net:8088/proxy/application_1564911637635_38262/api/v1/applications/application_1564911637635_38262/jobs
    // https://spark.apache.org/docs/latest/monitoring.html
    // http://ecpvm003481.server.rbsgrp.net:18089/

    /**
     * Method to determine whether to kill an application or not
     * An application will be killed that meets the following critera
     * 1. Most recent job completed > 120 mins ago
     * 2. Has no jobs, and was started > 120 mins ago
     * @param report The Yarn Application
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private static Boolean isKillIdleApplication_NEW(ApplicationReport report)
            throws IOException, InterruptedException {
        Boolean killApplication = false;
       // ApplicationResourceUsageReport usage = report.getApplicationResourceUsageReport();
        String url = report.getTrackingUrl();
        String applicationId = report.getApplicationId().toString();
        LOGGER.info(String.format("Tracking URL: %s", url));
        //JsonNode jobs = getJobs(String.format("%sapi/v1/applications/%s/jobs", url, applicationId));
        SparkJobDto[] jobs = getJobs(String.format("%sapi/v1/applications/%s/jobs", url, applicationId));
        LocalDateTime now = LocalDateTime.now(TimeZone.getTimeZone("GMT").toZoneId());
        //if (jobs.size() > 0) {
        if (jobs != null && jobs.length > 0) {
            // JsonNode latestJob = jobs[chi
            //JsonNode latestJob = jobs.get(0);
            SparkJobDto latestJob = jobs[0];
            if (!latestJob.getStatus().equals("RUNNING")) {
                DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(TimeZone.getTimeZone("GMT").toZoneId());
                String completionTime = latestJob.getCompletionTime();
                LocalDateTime jobCompletionTime = LocalDateTime.parse(completionTime.replace("GMT",""), formatter);
                killApplication = Duration.between(jobCompletionTime, now).toMinutes() >= minsToKill;
                if(killApplication) {
                    LOGGER.info(String.format("App id '%s' has a latest job completion time of '%s'.  Time now is '%s'.  This is > %d minutes, so will kill", applicationId, jobCompletionTime, now, minsToKill));
                } else {
                    LOGGER.info(String.format("App id '%s' has a latest job completion time of '%s'.  Time now is '%s'.  This is <= %d minutes, so will not kill", applicationId, jobCompletionTime, now, minsToKill));
                }
            } else {
                LOGGER.info(String.format("Application Id %s has currently Running Jobs.  Ignoring.", applicationId));
            }
           /*
            if (latestJob.has("status") && !latestJob.get("status").asText().equals("RUNNING")) {
                if (latestJob.has("completionTime")) {
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(TimeZone.getTimeZone("GMT").toZoneId());
                    String completionTime = latestJob.get("completionTime").asText();
                    LocalDateTime jobCompletionTime = LocalDateTime.parse(completionTime.replace("GMT",""), formatter);
                    killApplication = Duration.between(jobCompletionTime, now).toMinutes() >= minsToKill;
                    if(killApplication) {
                        LOGGER.info(String.format("App id '%s' has a latest job completion time of '%s'.  Time now is '%s'.  This is > %d minutes, so will kill", applicationId, jobCompletionTime, now, minsToKill));
                    } else {
                        LOGGER.info(String.format("App id '%s' has a latest job completion time of '%s'.  Time now is '%s'.  This is <= %d minutes, so will not kill", applicationId, jobCompletionTime, now, minsToKill));
                    }
                } else {
                    LOGGER.warn(String.format("Application Id %s has no completionTime.", applicationId));
                }
            } else {
                LOGGER.info(String.format("Application Id %s has currently Running Jobs.  Ignoring.", applicationId));
            }
            */

        } else {
            LocalDateTime appStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(report.getStartTime()), TimeZone.getDefault().toZoneId());
            killApplication = Duration.between(appStart, LocalDateTime.now()).toMinutes() >= minsToKill && report.getApplicationResourceUsageReport().getNumUsedContainers() == 1;
            if(killApplication) {
                LOGGER.info(String.format("App id '%s' has no jobs.  App Start Time is '%s'. Time now is '%s'.  This is > %d minutes, so will kill", applicationId, appStart, now, minsToKill));
            } else {
                LOGGER.info(String.format("App id '%s' has no jobs.  App Start Time is '%s'. Time now is '%s'.  This is <= %d minutes, so will not kill", applicationId, appStart, now, minsToKill));
            }
        }
        return killApplication;
    }

    /**
     * Method to get Jobs info for an Appication from the Spark UI API
     * @param fullUrl The URL of the running Spark app
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private static SparkJobDto[] getJobs(String fullUrl) throws IOException, InterruptedException {
        URL url = new URL(fullUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        // Send post request
        con.setDoOutput(true);
        int responseCode = con.getResponseCode();
        if (responseCode == 200) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine + "\r\n");
                }
                in.close();
                LOGGER.debug(response.toString());
                //JsonNode rootNode = new ObjectMapper().readTree(response.toString());
               // return rootNode;
               return SparkJobDto.getSparkJobs(response.toString());
            }
        } else {
            //TODO: GEt Error reponse if available and log
            LOGGER.error("Error getting info");
        }
        return null;
    }

}
