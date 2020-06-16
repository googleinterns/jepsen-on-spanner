package com.google.jepsenonspanner;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LoadGenerator;
import com.google.jepsenonspanner.operation.Operation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

public class IntegratedTest {
  Executor executor;

  @Test
  void testGenerateLoad() {
    executor = new Executor("test-instance", "example-db", 0);
    HashMap<String, Long> initialValues = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      initialValues.put(String.valueOf(i), 20L);
    }
    executor.initKeyValues(initialValues);
    LoadGenerator gen = new BankLoadGenerator(50, 100, 5, 1);
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      System.out.println(op.toString());
      op.getExecutionPlan().accept(executor);
    }
    executor.extractHistory();
  }

  @AfterEach
  void cleanUp() {
//    executor.getClient().write(Arrays.asList(Mutation.delete(Executor.TESTING_TABLE_NAME, KeySet.all()),
//            Mutation.delete(Executor.HISTORY_TABLE_NAME, KeySet.all())));
    executor.cleanUp();
  }
}
