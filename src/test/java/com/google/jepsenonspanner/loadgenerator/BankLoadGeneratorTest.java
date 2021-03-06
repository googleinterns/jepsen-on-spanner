package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import com.google.jepsenonspanner.operation.ReadWriteTransaction;
import com.google.jepsenonspanner.operation.TransactionalAction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BankLoadGeneratorTest {

  private static final int OP_LIMIT = 10;
  private static final int MAX_BALANCE = 10;
  private static final int ACCT_NUM = 3;
  private static final int MAX_TIME_PAST = 5 * 60 * 1000;

  @Test
  void testHasLoad() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM);
    for (int i = 0; i < OP_LIMIT; i++) {
      assertTrue(gen.hasLoad());
      gen.nextOperation();
    }
    assertFalse(gen.hasLoad());
  }

  void checkReads(BankLoadGenerator.Config config, boolean stale, boolean bounded) {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM, config);
    while (gen.hasLoad()) {
      Operation ops = gen.nextOperation();
      assertTrue(ops instanceof ReadTransaction);
      ReadTransaction txn = (ReadTransaction) ops;
      assertEquals(txn.getStaleness() != 0, stale);
      assertTrue(txn.getStaleness() <= MAX_TIME_PAST);
      assertEquals(txn.getBounded(), bounded);
      List<String> keys = txn.getKeys();
      assertEquals(keys.size(), ACCT_NUM);
      for (String key : keys) {
        assertTrue(Integer.parseInt(key) < ACCT_NUM);
        assertTrue(Integer.parseInt(key) >= 0);
      }
    }
  }

  @Test
  void testStrongReads() {
    checkReads(new BankLoadGenerator.Config(/*strongRead=*/1, 0, 0, 0), /*stale=*/false,
            /*bounded=*/false);
  }

  @Test
  void testBoundedStaleReads() {
    checkReads(new BankLoadGenerator.Config(0, /*boundedStaleRead=*/1, 0, 0), /*stale=*/true,
            /*bounded=*/true);
  }

  @Test
  void testExactStaleReads() {
    checkReads(new BankLoadGenerator.Config(0, 0, /*exactStaleRead=*/1, 0), /*stale=*/true,
            /*bounded=*/false);
  }

  void checkTransactionalRead(TransactionalAction action) {
    assertEquals(action.getValue(), 0);
    int acct = Integer.parseInt(action.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
  }

  void checkTransfer(TransactionalAction action) {
    assertNotNull(action);
    assertFalse(action.isRead());
    int acct = Integer.parseInt(action.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
    long transferAmt = action.getValue();
    assertTrue(transferAmt <= MAX_BALANCE && transferAmt > 0);
  }

  @Test
  void testTransfer() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM,
            new BankLoadGenerator.Config(0, 0, 0, /*transfer=*/1));
    while (gen.hasLoad()) {
      Operation operation = gen.nextOperation();
      assertTrue(operation instanceof ReadWriteTransaction);
      ReadWriteTransaction txn = (ReadWriteTransaction) operation;
      List<TransactionalAction> actions = txn.getSpannerActions();
      assertEquals(actions.size(), 2); // transfer between 2 accounts

      for (TransactionalAction action : actions) {
        checkTransactionalRead(action);
        checkTransfer(action.getDependentAction());
      }

      // additionally, check functions of each transfer
      TransactionalAction subtractAction = actions.get(0).getDependentAction();
      long transferAmt = subtractAction.getValue();
      assertTrue(subtractAction.decideProceed(transferAmt + 1));
      assertTrue(subtractAction.decideProceed(subtractAction.getValue()));
      assertFalse(subtractAction.decideProceed(transferAmt - 1));
      subtractAction.findDependentValue(transferAmt + 1);
      assertEquals(subtractAction.getValue(), 1);

      TransactionalAction addAction = actions.get(1).getDependentAction();
      long transferAmt2 = addAction.getValue();
      assertEquals(transferAmt, transferAmt2);
      // decide function will always return true, even if invalid; up to client to fail this txn
      assertTrue(addAction.decideProceed(transferAmt - 1));
      addAction.findDependentValue(addAction.getValue());
      assertEquals(addAction.getValue(), transferAmt2 * 2);
    }
  }

  @Test
  void testDeterministic() {
    int seed = new Random().nextInt();
    List<BankLoadGenerator> generators = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      generators.add(new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM, seed));
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
