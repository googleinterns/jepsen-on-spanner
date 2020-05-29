package com.google.jepsenonspanner.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SpannerClientTest {

  @Test
  void testInit() {
    String instanceId = "test-instance";
    String dbId = "example-db";
    System.out.println("HERE WHAT");
    System.out.flush();
    System.setProperty("SPANNER_EMULATOR_HOST", "localhost:9010");
    SpannerClient client = new SpannerClient(instanceId, dbId);
  }
}