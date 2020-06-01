package com.google.jepsenonspanner.operation;

public class StaleOperation extends Operation {

  public StaleOperation(String key, long value) {
    super(key, value);
  }

  public StaleOperation(String key) {
    this(key, /*value=*/0);
  }

  @Override
  public String toString() {
    return String.format("Stale Read %s", super.toString());
  }
}
