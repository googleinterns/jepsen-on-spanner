package com.google.jepsenonspanner.operation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class StaleOperationTest {
  private static final String KEY = "KEY";
  private static final int VALUE = 1;
  private static final int STALENESS = 500;

  private static final StaleOperation EXACT_STALE_READ = new StaleOperation(KEY, VALUE, false,
          STALENESS);
  private static final StaleOperation BOUNDED_STALE_READ = new StaleOperation(KEY, VALUE, false,
          STALENESS);

  @Test
  void testToString() {
    assertEquals(EXACT_STALE_READ.toString(), String.format("Exact Stale Read %s %d, staleness = " +
                    "%d ms", EXACT_STALE_READ.getKey(), EXACT_STALE_READ.getValue(),
            EXACT_STALE_READ.getStaleness()));
    assertEquals(BOUNDED_STALE_READ.toString(), String.format("Bounded Stale Read %s %d, " +
                    "staleness = %d ms", BOUNDED_STALE_READ.getKey(), BOUNDED_STALE_READ.getValue(),
            BOUNDED_STALE_READ.getStaleness()));
  }

  @Test
  void testIsBounded() {
    assertFalse(EXACT_STALE_READ.isBounded());
    assertTrue(BOUNDED_STALE_READ.isBounded());
  }

  @Test
  void testGetStaleness() {
    assertEquals(EXACT_STALE_READ.getStaleness(), STALENESS);
    assertEquals(BOUNDED_STALE_READ.getStaleness(), STALENESS);
  }
}
