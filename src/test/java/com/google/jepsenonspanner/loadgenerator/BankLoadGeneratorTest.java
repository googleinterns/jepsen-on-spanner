package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.StaleOperation;
import com.google.jepsenonspanner.operation.TransactionalOperation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BankLoadGeneratorTest {

  private static final int OP_LIMIT = 20;
  private static final int MAX_BALANCE = 10;
  private static final int ACCT_NUM = 3;
  private static final int MAX_TIME_PAST = 5 * 60 * 1000;

  @Test
  void hasLoad() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM);
    for (int i = 0; i < OP_LIMIT; i++) {
      assertTrue(gen.hasLoad());
      gen.nextOperation();
    }
    assertFalse(gen.hasLoad());
  }

  @Test
  void nextOperation() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM);
    while (gen.hasLoad()) {
      List<? extends Operation> ops = gen.nextOperation();
      assertFalse(ops.isEmpty());
      if (ops.get(0) instanceof StaleOperation) {
        checkStaleOps((List<StaleOperation>) ops);
      } else {
        checkTransactionalOps((List<TransactionalOperation>) ops);
      }
    }
  }

  void checkStaleOps(List<StaleOperation> ops) {
    assertEquals(ops.size(), ACCT_NUM);
    int timePast = ops.get(0).getStaleness();
    for (StaleOperation op : ops) {
      checkRead(op);
      assertEquals(op.getStaleness(), timePast);
    }
  }

  void checkTransactionalOps(List<TransactionalOperation> ops) {
    if (ops.get(0).getDependentOp() == null) {
      // strong read across accounts
      assertEquals(ops.size(), ACCT_NUM);
      for (TransactionalOperation op : ops) {
        checkRead(op);
      }
    } else {
      // transfer
      assertEquals(ops.size(), 2); // between 2 accounts
      for (TransactionalOperation op : ops) {
        checkRead(op);

        // check dependent write ops
        checkTransfer(op.getDependentOp());
      }

      // additionally, check functions of each transfer
      TransactionalOperation subtractOp = ops.get(0).getDependentOp();
      int transferAmt = subtractOp.getValue();
      assertTrue(subtractOp.decideProceed(transferAmt + 1));
      assertTrue(subtractOp.decideProceed(subtractOp.getValue()));
      assertFalse(subtractOp.decideProceed(transferAmt - 1));
      subtractOp.findDependentValue(transferAmt + 1);
      assertEquals(subtractOp.getValue(), 1);

      TransactionalOperation addOp = ops.get(1).getDependentOp();
      int transferAmt2 = addOp.getValue();
      assertEquals(transferAmt, transferAmt2);
      // decide function will always return true, even if invalid; up to client to fail this txn
      assertTrue(addOp.decideProceed(transferAmt - 1));
      addOp.findDependentValue(addOp.getValue());
      assertEquals(addOp.getValue(), transferAmt2 * 2);
    }
  }

  void checkRead(Operation op) {
    assertEquals(op.getValue(), 0);
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
  }

  void checkTransfer(TransactionalOperation op) {
    assertTrue(op != null);
    assertFalse(op.isRead());
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
    int transferAmt = op.getValue();
    assertTrue(transferAmt <= MAX_BALANCE && transferAmt > 0);
  }

}
