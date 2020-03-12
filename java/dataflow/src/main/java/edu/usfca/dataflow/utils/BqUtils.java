package edu.usfca.dataflow.utils;

import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import edu.usfca.dataflow.Main;

public class BqUtils {
  // TODO: To change this to your project so you can test things locally.
  private static final BigQuery DEFAULT_BIGQUERY =
      BigQueryOptions.newBuilder().setProjectId(Main.GCP_PROJECT).build().getService();

  public static BigQuery getService() {
    return DEFAULT_BIGQUERY;
  }

  public static TableResult getQueryResult(String query) {
    final JobId jobId = JobId.of(UUID.randomUUID().toString());
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
    try {
      return BqUtils.getService().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).getQueryResults();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }
}
