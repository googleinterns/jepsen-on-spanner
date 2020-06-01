package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.OperationList;

import java.util.List;

/**
 * A load generator generates testing load for the client to interact with the Spanner instance.
 * Each generator must implement this interface
 *
 */
public abstract class LoadGenerator {

  protected int opLimit = 0;

  public LoadGenerator(int opLimit) {
    this.opLimit = opLimit;
  }

  /**
   * Returns the next operations for the client wrapper to execute
   */
  public abstract OperationList nextOperation();

  /**
   * Returns if the generator has more loads
   */
  public boolean hasLoad() {
    return opLimit > 0;
  }
}
