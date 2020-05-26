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
  private static final int MAX_TIME_PAST = 3000000;

  @Test
  void hasLoad() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM);
    for (int i = 0; i < OP_LIMIT; i++) {
      assertTrue(gen.hasLoad());
      gen.nextOperation();
    }
    assertFalse(gen.hasLoad());
  }

  void testRead(Operation op, int timePast) {
    assertEquals(op.getOp(), Operation.OpType.READ);
    assertEquals(op.getMillisecondsPast(), timePast);
    assertEquals(op.getValue(), null);
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
  }

  void testTransfer(Operation op) {
    assertTrue(op != null);
    assertEquals(op.getOp(), Operation.OpType.WRITE);
    int acct = Integer.parseInt(op.getKey());
    assertTrue(acct >= 0 && acct < ACCT_NUM);
    int transferAmt = Integer.parseInt(op.getValue());
    assertTrue(transferAmt <= MAX_BALANCE && transferAmt > 0);
  }

  @Test
  void nextOperation() {
    BankLoadGenerator gen = new BankLoadGenerator(OP_LIMIT, MAX_BALANCE, ACCT_NUM);
    while (gen.hasLoad()) {
      List<Operation> ops = gen.nextOperation();
      if (ops.get(0).getDependentOp() == null) {
        // a read
        assertEquals(ops.size(), ACCT_NUM);
        int timePast = ops.get(0).getMillisecondsPast();
        for (Operation op : ops) {
          testRead(op, timePast);
        }
      } else {
        // transfer between 2 accounts
        assertEquals(ops.size(), 2);
        for (Operation op : ops) {
          testRead(op, 0);

          // now check the dependent transfer ops
          testTransfer(op.getDependentOp());
        }

        // additionally, check functions of each transfer
        Operation subtractOp = ops.get(0).getDependentOp();
        int transferAmt = Integer.parseInt(subtractOp.getValue());
        assertTrue(subtractOp.decideProceed(Integer.toString(transferAmt + 1)));
        assertTrue(subtractOp.decideProceed(subtractOp.getValue()));
        assertFalse(subtractOp.decideProceed(Integer.toString(transferAmt - 1)));
        subtractOp.findDependentValue(Integer.toString(transferAmt + 1));
        assertEquals(subtractOp.getValue(), "1");

        Operation addOp = ops.get(1).getDependentOp();
        int transferAmt2 = Integer.parseInt(addOp.getValue());
        assertEquals(transferAmt, transferAmt2);
        // decide function will always return true, even if invalid; up to client to fail this txn
        assertTrue(addOp.decideProceed(Integer.toString(transferAmt - 1)));
        addOp.findDependentValue(addOp.getValue());
        assertEquals(addOp.getValue(), Integer.toString(transferAmt2 * 2));
      }
    }
  }
}