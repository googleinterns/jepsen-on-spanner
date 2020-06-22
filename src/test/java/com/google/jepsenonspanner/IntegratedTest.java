package com.google.jepsenonspanner;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LoadGenerator;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.verifier.BankVerifier;
import com.google.jepsenonspanner.verifier.Verifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

public class IntegratedTest {
  Executor executor;

  @Test
  void testGenerateLoad() {
    executor = new Executor("jepsen-on-spanner-with-gke", "jepsen", "example-db", 0, /*init=*/true);
    executor.createTables();
    HashMap<String, Long> initialValues = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      initialValues.put(String.valueOf(i), 20L);
    }
    executor.initKeyValues(initialValues);
    LoadGenerator gen = new BankLoadGenerator(50, 100, 5, 1);
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      System.out.printf("Generated op %s", op.toString());
      op.getExecutionPlan().accept(executor);
    }
    executor.extractHistory();
    Verifier v = new BankVerifier();
    v.verify("history.edn", initialValues);
  }

  @AfterEach
  void cleanUp() {
    executor.cleanUp();
    executor.close();
  }
}