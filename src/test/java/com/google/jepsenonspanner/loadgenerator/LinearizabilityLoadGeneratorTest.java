package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinearizabilityLoadGeneratorTest {
  private static final int OP_LIMIT = 10;
  private static final int VALUE_LIMIT = 5;
  private static final List<String> KEYS = Arrays.asList("x", "y");

  @Test
  void nextOperation() {
  }

  void checkReads(boolean allowMultiKeys) {
    LoadGenerator gen = new LinearizabilityLoadGenerator(OP_LIMIT, VALUE_LIMIT,
            KEYS.toArray(new String[0]), /*allowMultiKeys=*/false, /*read=*/1, /*write=*/0,
            /*transaction=*/0, /*cas=*/0);
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      assertTrue(op instanceof ReadTransaction);
      ReadTransaction read = (ReadTransaction) op;
      assertEquals(0, read.getStaleness());
      List<String> keys = read.getKeys();
      assertEquals(1, keys.size());
      assertTrue(KEYS.containsAll(keys));
    }
  }

  @Test
  void checkReadOnly() {
    checkReads(/*allowMultiKeys=*/false);
  }

  @Test
  void checkMultiKeyReadOnly() {
    checkReads(/*allowMultiKeys=*/true);
  }
}