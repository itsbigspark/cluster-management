package dev.bigspark.cloudera.management.jobs.offload;


public class OffloadRunner {

  public static void main(String[] args) throws Exception {
    OffloadController offloadController = new OffloadController();
    offloadController.execute(1);
  }
}
