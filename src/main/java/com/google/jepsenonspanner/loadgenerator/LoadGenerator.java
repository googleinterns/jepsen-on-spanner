package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.operation.OperationList;

import java.util.HashMap;

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
   * Initialize any key-value pairs in the database as necessary
   */
  public void initKVs(HashMap<String, Long> initialData, Executor executor) {
    executor.initKeyValues(initialData);
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
