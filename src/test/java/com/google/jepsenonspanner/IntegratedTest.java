package com.google.jepsenonspanner;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LinearizabilityLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LoadGenerator;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.verifier.BankVerifier;
import com.google.jepsenonspanner.verifier.KnossosVerifier;
import com.google.jepsenonspanner.verifier.Verifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IntegratedTest {
  Executor executor;

  @Test
  void testGenerateLoad() {
    executor = new Executor("jepsen-on-spanner-with-gke", "jepsen", "example-db", 0, /*init=*/true);
    executor.createTables();
    HashMap<String, Long> initialValues = new HashMap(Map.of("x", 0, "y", 0));
//    for (int i = 0; i < 5; i++) {
//      initialValues.put(String.valueOf(i), 20L);
//    }
    executor.initKeyValues(initialValues);
    LoadGenerator gen = LinearizabilityLoadGenerator.createGeneratorFromConfig("test-config.json");
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      System.out.printf("Generated op %s", op.toString());
      op.getExecutionPlan().accept(executor);
    }
    executor.extractHistory();
    Verifier v = new KnossosVerifier();
    v.verify("history.edn", initialValues);
  }

  @AfterEach
  void cleanUp() {
    executor.cleanUp();
    executor.close();
  }
}