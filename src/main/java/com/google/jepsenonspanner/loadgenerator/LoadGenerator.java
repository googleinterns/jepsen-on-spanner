package com.google.jepsenonspanner.loadgenerator;

import java.util.List;

/**
 * A load generator generates testing load for the client to interact with the Spanner instance.
 * Each generator must implement this interface
 *
 */
public interface LoadGenerator {
  public List<Operation> nextOperation();
  public boolean hasLoad();
}
