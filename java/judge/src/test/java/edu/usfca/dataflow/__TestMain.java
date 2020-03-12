package edu.usfca.dataflow;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

// NOTE: You should not modify this file, as the grading system will use this file as-is (and additional hidden tests)
// in order to grade your submission.
public class __TestMain {
  @Test
  public void testGetUserEmail() {
    assertNotEquals("", Main.getUserEmail());
    assertNotEquals("", Main.GCP_PROJECT);
  }
}
