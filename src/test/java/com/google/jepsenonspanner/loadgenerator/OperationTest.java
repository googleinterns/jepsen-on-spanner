package com.google.jepsenonspanner.loadgenerator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OperationTest {
  private static final String KEY = "KEY";
  private static final String VALUE = "VALUE";
  private static final String EMPTY_STRING = "";

  private static final Operation STRONG_READ = new Operation(Operation.OpType.READ, KEY, null);
  private static final Operation STALE_READ = new Operation(Operation.OpType.READ, KEY, VALUE, 500);
  private static final Operation DEPENDENT_WRITE = new Operation(Operation.OpType.WRITE, KEY,
          VALUE, (s1, s2) -> s1 + " " + s2, (s1, s2) -> !s1.isEmpty() && !s2.isEmpty());

  @Test
  void testToString() {
    assertEquals(STRONG_READ.toString(), String.format("%s %s %s %d [ %s ]", STRONG_READ.getOp(),
            STRONG_READ.getKey(), STRONG_READ.getValue(), STRONG_READ.getMillisecondsPast(),
            STRONG_READ.getDependentOp()));
    assertEquals(STALE_READ.toString(), String.format("%s %s %s %d [ %s ]", STALE_READ.getOp(),
            STALE_READ.getKey(), STALE_READ.getValue(), STALE_READ.getMillisecondsPast(),
            STALE_READ.getDependentOp()));
    assertEquals(DEPENDENT_WRITE.toString(), String.format("%s %s %s %d [ %s ]",
            DEPENDENT_WRITE.getOp(), DEPENDENT_WRITE.getKey(), DEPENDENT_WRITE.getValue(),
            DEPENDENT_WRITE.getMillisecondsPast(), DEPENDENT_WRITE.getDependentOp()));
  }

  @Test
  void decideProceed() {
    assertTrue(DEPENDENT_WRITE.decideProceed(VALUE));
    assertFalse(DEPENDENT_WRITE.decideProceed(EMPTY_STRING));
  }

  @Test
  void findDependentValue() {
    DEPENDENT_WRITE.findDependentValue(VALUE);
    assertEquals(DEPENDENT_WRITE.getValue(), VALUE + " " + VALUE);
  }

  @Test
  void setDependentOp() {
    STRONG_READ.setDependentOp(DEPENDENT_WRITE);
    assertEquals(DEPENDENT_WRITE.toString(), STRONG_READ.getDependentOp().toString());
    STRONG_READ.setDependentOp(null);
  }
}