package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Executor;

import java.util.List;
import java.util.function.Consumer;

/**
 * An Operation encapsulates a unit of load that should be executed atomically by the
 * executor. The user will supply a load name and a representation to be recorded in the history
 * table; they will then be interpreted by the verifier.
 */
public abstract class Operation {

  private String loadName;
  private List<String> recordRepresentation;

  public Operation(String loadName, List<String> recordRepresentation) {
    this.loadName = loadName;
    this.recordRepresentation = recordRepresentation;
  }

  /**
   * Returns a function that takes an Executor as the argument and executes this operation. This
   * is to highlight the design that an Operation is intended as a "data" class, and the
   * execution should not be done "by" it, but instead "on" it.
   * Example:
   * LoadGenerator gen = ...;
   * Operation ops = gen.nextOperation();
   * Consumer<Executor> func = ops.getExecutionPlan();
   * func.accept(exec);
   */
  public abstract Consumer<Executor> getExecutionPlan();

  public String getLoadName() {
    return loadName;
  }

  public List<String> getRecordRepresentation() {
    return recordRepresentation;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (!(other instanceof Operation))
      return false;

    Operation otherOps = (Operation) other;
    return this.loadName.equals(otherOps.loadName)
            && this.recordRepresentation.equals(otherOps.recordRepresentation);
  }

  @Override
  public String toString() {
    return loadName + " " + recordRepresentation;
  }
}
