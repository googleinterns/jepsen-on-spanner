package com.google.jepsenonspanner.operation;

public class StaleOperation extends Operation {

  private boolean bounded;
  private int staleness; // in milliseconds

  public StaleOperation(String key, int value, boolean bounded, int staleness) {
    super(key, value);
    this.bounded = bounded;
    this.staleness = staleness;
  }

  public boolean isBounded() {
    return bounded;
  }

  public int getStaleness() {
    return staleness;
  }

  @Override
  public String toString() {
    return String.format("%s Stale Read %s, staleness = %d ms", bounded ? "Bounded" :
            "Exact", super.toString(), staleness);
  }
}
