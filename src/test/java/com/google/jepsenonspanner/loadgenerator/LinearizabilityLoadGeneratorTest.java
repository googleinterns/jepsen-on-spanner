package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import com.google.jepsenonspanner.operation.ReadWriteTransaction;
import com.google.jepsenonspanner.operation.TransactionalAction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinearizabilityLoadGeneratorTest {
  private static final int OP_LIMIT = 10;
  private static final int VALUE_LIMIT = 5;
  private static final List<String> KEYS = Arrays.asList("x", "y");

  void checkReads(boolean allowMultiKeys) {
    LoadGenerator gen = new LinearizabilityLoadGenerator(OP_LIMIT, VALUE_LIMIT,
            KEYS.toArray(new String[0]), allowMultiKeys, new LinearizabilityLoadGenerator.Config(/*read=*/1, /*write=*/0,
            /*transaction=*/0, /*cas=*/0));
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      assertTrue(op instanceof ReadTransaction);
      checkSingleRead((ReadTransaction) op, allowMultiKeys);
    }
  }

  void checkSingleRead(ReadTransaction op, boolean allowMultiKeys) {
    assertEquals(op.getStaleness(), 0);
    List<String> keys = op.getKeys();
    if (!allowMultiKeys) {
      assertEquals(1, keys.size());
    } else {
      assertTrue(keys.size() <= KEYS.size());
    }
    assertTrue(KEYS.containsAll(keys));
  }

  @Test
  void testReadOnly() {
    checkReads(/*allowMultiKeys=*/false);
  }

  @Test
  void testMultiKeyReadOnly() {
    checkReads(/*allowMultiKeys=*/true);
  }

  void checkWriteOnly(boolean allowMultiKeys) {
    LoadGenerator gen = new LinearizabilityLoadGenerator(OP_LIMIT, VALUE_LIMIT,
            KEYS.toArray(new String[0]), allowMultiKeys, new LinearizabilityLoadGenerator.Config(/*read=*/0, /*write=*/1,
            /*transaction=*/0, /*cas=*/0));
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      assertTrue(op instanceof ReadWriteTransaction);
      checkSingleWrite((ReadWriteTransaction) op, allowMultiKeys);
    }
  }

  void checkSingleWrite(ReadWriteTransaction op, boolean allowMultiKeys) {
    List<TransactionalAction> actions = op.getSpannerActions();
    if (!allowMultiKeys) {
      assertEquals(1, actions.size());
    } else {
      assertTrue(actions.size() <= KEYS.size());
    }
    for (TransactionalAction action : actions) {
      assertFalse(action.isRead());
      assertTrue(KEYS.contains(action.getKey()));
      assertTrue(1 <= action.getValue() && action.getValue() <= VALUE_LIMIT);
      assertNull(action.getDependentAction());
    }
  }

  @Test
  void testWriteOnly() {
    checkWriteOnly(/*allowMultiKeys=*/false);
  }

  @Test
  void testMultiKeyWriteOnly() {
    checkWriteOnly(/*allowMultiKeys=*/true);
  }

  void checkTransactions(boolean allowMultiKeys) {
    LoadGenerator gen = new LinearizabilityLoadGenerator(OP_LIMIT, VALUE_LIMIT,
            KEYS.toArray(new String[0]), allowMultiKeys, new LinearizabilityLoadGenerator.Config(/*read=*/0, /*write=*/0,
            /*transaction=*/1, /*cas=*/0));
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      assertTrue(op instanceof ReadWriteTransaction);
      checkSingleTransaction((ReadWriteTransaction) op, allowMultiKeys);
    }
  }

  void checkSingleTransaction(ReadWriteTransaction op, boolean allowMultiKeys) {
    List<TransactionalAction> actions = op.getSpannerActions();
    for (TransactionalAction action : actions) {
      assertTrue(KEYS.contains(action.getKey()));
      assertNull(action.getDependentAction());
      if (!action.isRead()) {
        assertTrue(1 <= action.getValue() && action.getValue() <= VALUE_LIMIT);
      }
    }
  }

  @Test
  void testTransactionOnly() {
    checkTransactions(/*allowMultiKeys=*/true);
  }

  @Test
  void testDeterministic() {
    int seed = new Random().nextInt();
    List<LinearizabilityLoadGenerator> generators = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      generators.add(new LinearizabilityLoadGenerator(seed, OP_LIMIT, VALUE_LIMIT,
              KEYS.toArray(new String[0]), /*allowMultiKeys=*/true,
              new LinearizabilityLoadGenerator.Config(/*read=*/1, /*write=*/1, /*transaction=*/1,
                      /*cas=*/0)));
    }

    for (int i = 0; i < OP_LIMIT; i++) {
      assertTrue(generators.get(0).hasLoad());
      Operation op = generators.get(0).nextOperation();

      // All operations generated should be the same within this iteration
      for (int j = 1; j < generators.size(); j++) {
        Operation newOp = generators.get(j).nextOperation();
        assertEquals(newOp, op);
      }
    }
  }
}