package com.google.jepsenonspanner.loadgenerator;

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
      List<Operation> ops = gen.nextOperation();
      assertFalse(ops.isEmpty());
      // cannot use type info here, because transfer starts with a read as well
      if (ops.get(0).getDependentOp() == null) {
        // a read
        assertEquals(ops.size(), ACCT_NUM);
        int timePast = ops.get(0).getMillisecondsPast();
        for (Operation op : ops) {
          checkRead(op, timePast);
        }
      } else {
        // transfer between 2 accounts
        assertEquals(ops.size(), 2);
        for (Operation op : ops) {
          checkRead(op, 0);

          // now check the dependent transfer ops
          checkTransfer(op.getDependentOp());
        }

        // additionally, check functions of each transfer
        Operation subtractOp = ops.get(0).getDependentOp();
        int transferAmt = subtractOp.getValue();
        assertTrue(subtractOp.decideProceed(transferAmt + 1));
        assertTrue(subtractOp.decideProceed(subtractOp.getValue()));
        assertFalse(subtractOp.decideProceed(transferAmt - 1));
        subtractOp.findDependentValue(transferAmt + 1);
        assertEquals(subtractOp.getValue(), 1);

        Operation addOp = ops.get(1).getDependentOp();
        int transferAmt2 = addOp.getValue();
        assertEquals(transferAmt, transferAmt2);
        // decide function will always return true, even if invalid; up to client to fail this txn
        assertTrue(addOp.decideProceed(transferAmt - 1));
        addOp.findDependentValue(addOp.getValue());
        assertEquals(addOp.getValue(), transferAmt2 * 2);
      }
    }
  }

  void checkRead(Operation op, int timePast) {
    assertEquals(op.getOp(), Operation.OpType.READ);
    assertEquals(op.getMillisecondsPast(), timePast);
    assertEquals(op.getValue(), 0);
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
  }

  void checkTransfer(Operation op) {
    assertTrue(op != null);
    assertEquals(op.getOp(), Operation.OpType.WRITE);
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
    int transferAmt = op.getValue();
    assertTrue(transferAmt <= MAX_BALANCE && transferAmt > 0);
  }

}
