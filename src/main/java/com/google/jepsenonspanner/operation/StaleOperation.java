package com.google.jepsenonspanner.operation;

public class StaleOperation extends Operation {

  private boolean bounded;
  private int staleness; // in milliseconds

  public StaleOperation(String key, long value) {
    super(key, value);
  }

  public boolean isBounded() {
    return bounded;
  }

  public int getStaleness() {
    return staleness;
  }

  @Override
  public String toString() {
    return String.format("Stale Read %s", super.toString());
  }
}
