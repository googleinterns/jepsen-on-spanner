package com.google.jepsenonspanner.client;

import org.junit.jupiter.api.Test;

class ExecutorTest {

  @Test
  void testInit() {
    String instanceId = "test-instance";
    String dbId = "example-db";
    System.out.println("HERE WHAT");
    System.out.flush();
    System.setProperty("SPANNER_EMULATOR_HOST", "localhost:9010");
    Executor client = new Executor(instanceId, dbId, 0);
  }
}