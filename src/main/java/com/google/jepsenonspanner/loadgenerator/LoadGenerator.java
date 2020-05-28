package com.google.jepsenonspanner.loadgenerator;

import java.util.List;

/**
 * A load generator generates testing load for the client to interact with the Spanner instance.
 * Each generator must implement this interface
 *
 */
public interface LoadGenerator {

  /**
   * Returns the next operations for the client wrapper to execute
   */
  public List<Operation> nextOperation();

  /**
   * Returns if the generator has more loads
   */
  public boolean hasLoad();
}
