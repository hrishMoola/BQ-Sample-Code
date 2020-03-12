package edu.usfca.dataflow;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

import edu.usfca.dataflow.utils.BqUtils;

/**
 * Follow the instructions on github.
 */
public class Main {
  // TODO: Make sure you change USER_EMAIL below to your @dons email address.
  final private static String USER_EMAIL = "a";
  // TODO: Make sure you change GCP_PROJECT to your project id.
  final public static String GCP_PROJECT = "b";

  static String getUserEmail() {
    return USER_EMAIL;
  }

  // This code is the same as Lab06.
  public static void main(String[] args) throws IOException {
    File file = new File("dataflow/resources/sample00.sql");
    System.out.println(file.getAbsolutePath()); // This is to check if $file object correctly reflects the path.
    String query = new String(Files.readAllBytes(file.toPath()));
    System.out.println(query); // This should print a few lines.

    // Here's one way to run queries from your Java program.
    // Note that this utilizes your "default" GCP credentials (stored in the machine that runs this program).
    // This has nothing to do with Beam SDK. We'll learn about Beam-BigQuery integration later.
    TableResult result = BqUtils.getQueryResult(query);
    for (FieldValueList fvl : result.iterateAll()) {
      // TODO: The following line will throw a NullPointerException because "fvl.get("col_int")" would return null.
      // If your returned values could be null, then you need to handle that gracefully.
      // System.out.format("num_rows = %d, col_str = %s, col_boolean = %s, col_int = %d",
      // fvl.get("num_rows").getLongValue(), fvl.get("col_str").getStringValue(),
      // fvl.get("col_boolean").getBooleanValue(), fvl.get("col_int").getLongValue());

      System.out.format("num_rows = %d, col_str = %s, col_boolean = %s, col_int = ?",
          fvl.get("num_rows").getLongValue(), fvl.get("col_str").getStringValue(),
          fvl.get("col_boolean").getBooleanValue());
      // Expected output: "num_rows = 5552452, col_str = dummy, col_boolean = true, col_int = ?"
    }
  }
}
