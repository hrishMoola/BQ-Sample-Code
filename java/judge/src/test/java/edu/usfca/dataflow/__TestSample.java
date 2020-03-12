package edu.usfca.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableSet;

public class __TestSample {
  // TODO: If you are debugging your code, and this "time limit" causes a problem in your local environment,
  // feel free to comment out these two lines. This is enforced only for "grading system" purposes.
  // Note that intended solutions will not need more than 5 seconds (and typically all queries finish within 1-2
  // seconds).
  @Rule
  public Timeout timeout = Timeout.millis(15000);

  // TODO: Change the project/dataset/table names to yours, if you want to run unit tests locally to verify your query results.
  // As you do not have access to the "beer-spear" GCP project, you won't be able to use the following as-is.
  final String table1A = "`beer-spear.Lab07.d02_small`";
  final String table1B = "`beer-spear.Lab07.d03_medium`";
  final String table2A = "`beer-spear.Lab07.d03_medium`";
  final String table2B = "`beer-spear.Lab07.d04_large`";
  final String table3A = "`beer-spear.Lab07.d03_medium`";
  final String table3B = "`beer-spear.Lab07.d04_large`";

  // TODO: For your local tests, change this to your GCP project "ID" (not name).
  static final BigQuery JUDGE_SERVICE = BigQueryOptions.newBuilder().setProjectId("your-gcp-project-ID").build().getService();


  public static TableResult __getQueryResult(String query) {
    final JobId jobId = JobId.of(UUID.randomUUID().toString());
    // Note: For all queries for this lab, they shouldn't really process more than 2.5MB per query.
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).setMaximumBytesBilled(10485760L).build();
    try {
      return JUDGE_SERVICE.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).getQueryResults();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  // TODO: If you don't pass this unit test, you'll need to follow the instructions to resolve your technical settings.
  // It will still work in the grading system (using instructor's GCP project & credentials),
  // but you will be much more efficient for future labs/projects if this works in your local machine.
  @Test
  public void testSampleQuery() throws IOException {
    // Notice how the path is different (since this unit test is currently in "java/judge" directory).
    File file = new File("../dataflow/resources/sample00.sql");
    System.out.println(file.getAbsolutePath()); // This is to check if $file object correctly reflects the path.
    String query = new String(Files.readAllBytes(file.toPath()));
    System.out.println(query); // This should print a few lines.

    TableResult result = __getQueryResult(query);
    assertEquals(1, result.getTotalRows());
    for (FieldValueList fvl : result.iterateAll()) {
      assertEquals(5552452L, fvl.get("num_rows").getLongValue());
      assertEquals("dummy", fvl.get("col_str").getStringValue());
      assertTrue(fvl.get("col_boolean").getBooleanValue());
      assertTrue(fvl.get("col_int").isNull());
    }
  }

  // --------
  final Set<String> expected1A = new ImmutableSet.Builder<String>().addAll(Arrays.asList("app.짜장면\t12\t178704.065",
      "app.냉면\t12\t168830.079", "id486686\t10\t146271.546", "app.라면\t10\t31864.227")).build();

  final Set<String> expected1B = new ImmutableSet.Builder<String>().addAll(Arrays.asList("app.비빔면\t10261.622",
      "app.짜장면\t10147.190", "id686486\t10580.808", "커피\t10880.078", "id686\t11276.108")).build();
  // --------
  final Set<String> expected2A = new ImmutableSet.Builder<String>()
      .addAll(Arrays.asList("android\tbceea369-xxxx-mxxx-nxxx-xxxxxxxxxxxx\t4\t1484.6",
          "android\ta08a7c3e-xxxx-mxxx-nxxx-xxxxxxxxxxxx\t5\t1222.8", // BQ will give you
          "android\taaa7da49-xxxx-mxxx-nxxx-xxxxxxxxxxxx\t5\t1276.4",
          "android\tf2f6b8f4-xxxx-mxxx-nxxx-xxxxxxxxxxxx\t4\t1448.0"))
      .build();

  final Set<String> expected2B =
      new ImmutableSet.Builder<String>().addAll(Arrays.asList("android\t0f8fc435-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "android\t15dbbf23-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\t1f5fc1c4-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "android\t4d307963-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\t629a8a66-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "android\t8c9574d5-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\tb641fe18-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "android\tc543cb3a-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\tc5a187d1-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "android\tdad9ae86-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\te39ca9ad-xxxx-mxxx-nxxx-xxxxxxxxxxxx",
          "ios\t00000000-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "ios\t429fc575-xxxx-mxxx-nxxx-xxxxxxxxxxxx")).build();
  // --------
  final Set<String> expected3A = new ImmutableSet.Builder<String>().addAll(
      Arrays.asList("android\t30eedd18-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\t369eb63e-xxxx-mxxx-nxxx-xxxxxxxxxxxx"))
      .build();

  final Set<String> expected3B = new ImmutableSet.Builder<String>().addAll(
      Arrays.asList("android\t542e67b8-xxxx-mxxx-nxxx-xxxxxxxxxxxx", "android\tda1e9fa5-xxxx-mxxx-nxxx-xxxxxxxxxxxx"))
      .build();

  interface __helperInterface {
    String format(FieldValueList fvl);
  }

  // For Task 1-A, grading system will round the values to 3 decimal places.
  static __helperInterface formatter1A = (FieldValueList fvl) -> String.format("%s\t%d\t%.3f",
      fvl.get(0).getStringValue(), fvl.get(1).getLongValue(), fvl.get(2).getDoubleValue());

  // For Task 1-B, grading system will round the values to 3 decimal places.
  static __helperInterface formatter1B =
      (FieldValueList fvl) -> String.format("%s\t%.3f", fvl.get(0).getStringValue(), fvl.get(1).getDoubleValue());

  // For Task 2-A, grading system will round the values to 1 decimal place.
  static __helperInterface formatter2A = (FieldValueList fvl) -> String.format("%s\t%s\t%d\t%.1f",
      fvl.get(0).getStringValue(), fvl.get(1).getStringValue(), fvl.get(2).getLongValue(), fvl.get(3).getDoubleValue());

  static __helperInterface formatter3 = (FieldValueList fvl) -> String.format("%s\t%s",
      fvl.get(0).getStringValue().toLowerCase(), fvl.get(1).getStringValue().toLowerCase());

  public void __helper(final String filePath, final String table, final Set<String> expected, __helperInterface func)
      throws IOException {
    File file = new File(filePath);
    String query = new String(Files.readAllBytes(file.toPath()));

    query = query.replaceAll("`.*`", String.format("%s", table));
    System.out.println(query);

    TableResult result = __getQueryResult(query);
    assertEquals(expected.size(), result.getTotalRows());
    Set<String> actual = new HashSet<>();
    for (FieldValueList fvl : result.iterateAll()) {
      actual.add(func.format(fvl));
    }
    assertEquals(expected, actual);
  }

  @Test
  public void __shareable__1A() throws IOException {
    __helper("../dataflow/resources/task-1A.sql", table1A, expected1A, formatter1A);
  }

  @Test
  public void __shareable__1B() throws IOException {
    __helper("../dataflow/resources/task-1B.sql", table1B, expected1B, formatter1B);
  }

  @Test
  public void __shareable__2A() throws IOException {
    __helper("../dataflow/resources/task-2A.sql", table2A, expected2A, formatter2A);
  }

  @Test
  public void __shareable__2B() throws IOException {
    __helper("../dataflow/resources/task-2B.sql", table2B, expected2B, formatter3);
  }

  @Test
  public void __shareable__3A() throws IOException {
    __helper("../dataflow/resources/task-3A.sql", table3A, expected3A, formatter3);
  }

  @Test
  public void __shareable__3B() throws IOException {
    __helper("../dataflow/resources/task-3B.sql", table3B, expected3B, formatter3);
  }
}
