package com.google.jepsenonspanner.operation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TransactionalActionTest {
  private static final String KEY = "KEY";
  private static final int VALUE = 1;
  private static final int ZERO = 0;

  private static final TransactionalAction STRONG_READ = TransactionalAction.createTransactionalRead(KEY);
  private static final TransactionalAction DEPENDENT_WRITE =
          TransactionalAction.createDependentTransactionalWrite(KEY, s1 -> s1 + VALUE, s1 -> s1 > 0);

  @Test
  void testToString() {
    assertEquals(STRONG_READ.toString(), String.format("Strong Read %s %d, dependent = [ %s ]",
            STRONG_READ.getKey(), STRONG_READ.getValue(), STRONG_READ.getDependentAction()));
    assertEquals(DEPENDENT_WRITE.toString(), String.format("Write %s %d, dependent = [ %s ]",
            DEPENDENT_WRITE.getKey(), DEPENDENT_WRITE.getValue(),
            DEPENDENT_WRITE.getDependentAction()));
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
    STRONG_READ.setDependentAction(DEPENDENT_WRITE);
    assertEquals(DEPENDENT_WRITE.toString(), STRONG_READ.getDependentAction().toString());
    STRONG_READ.setDependentAction(null);
  }
}
