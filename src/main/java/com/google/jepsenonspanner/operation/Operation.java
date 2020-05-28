package com.google.jepsenonspanner.operation;

import java.util.Arrays;

/**
 * Operation encapsulates an operation produced by a generator that needs to be executed by the
 * client wrapper. It boils down to three types of basic interaction with the spanner instance:
 * strong read, stale read and write. All user-defined loads build on top of this class in the
 * form of a list of operations that will be performed in the same transaction, or a series of
 * stale reads.
 * <p></p>
 * An operation may have dependents whose value will be filled in by the return value of this
 * operation. The client should discover such dependents and call the two member functions to
 * fill in the value. In the context of this file, a dependent operation is one that depends on
 * the return value of a previous operation.
 */
public abstract class Operation {

  private String key;
  private int value;

  public Operation(String key, int value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  /**
   * Return a string representation of this operation
   */
  @Override
  public String toString() {
    return String.join(" ", Arrays.asList(key, Integer.toString(value)));
  }
}
