package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.operation.TransactionalOperation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TransactionalOperationTest {
  private static final String KEY = "KEY";
  private static final int VALUE = 1;
  private static final int ZERO = 0;

  private static final TransactionalOperation STRONG_READ = new TransactionalOperation(KEY, 0,
          TransactionalOperation.READ);
  private static final TransactionalOperation DEPENDENT_WRITE = new TransactionalOperation(KEY,
          VALUE, TransactionalOperation.WRITE, (s1, s2) -> s1 + s2, (s1, s2) -> s1 > 0 && s2 > 0);

  @Test
  void testToString() {
    assertEquals(STRONG_READ.toString(), String.format("Strong Read %s %d, dependent = [ %s ]",
            STRONG_READ.getKey(), STRONG_READ.getValue(), STRONG_READ.getDependentOp()));
    assertEquals(DEPENDENT_WRITE.toString(), String.format("Write %s %d, dependent = [ %s ]",
            DEPENDENT_WRITE.getKey(), DEPENDENT_WRITE.getValue(),
            DEPENDENT_WRITE.getDependentOp()));
  }

  @Test
  void decideProceed() {
    assertTrue(DEPENDENT_WRITE.decideProceed(VALUE));
    assertFalse(DEPENDENT_WRITE.decideProceed(ZERO));
  }

  @Test
  void findDependentValue() {
    DEPENDENT_WRITE.findDependentValue(VALUE);
    assertEquals(DEPENDENT_WRITE.getValue(), VALUE + VALUE);
  }

  @Test
  void setDependentOp() {
    STRONG_READ.setDependentOp(DEPENDENT_WRITE);
    assertEquals(DEPENDENT_WRITE.toString(), STRONG_READ.getDependentOp().toString());
    STRONG_READ.setDependentOp(null);
  }
}