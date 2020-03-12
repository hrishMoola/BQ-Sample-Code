package edu.usfca.dataflow.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.usfca.dataflow.Main;

public class __TestBqUtils {
  @Test
  public void testService() {
    assertEquals(Main.GCP_PROJECT, BqUtils.getService().getOptions().getProjectId());
  }
}
