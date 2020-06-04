package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Executor;

import java.util.List;
import java.util.function.Consumer;

/**
 * An OperationList encapsulates a unit of load that should be executed atomically by the
 * executor. The user will supply a load name and a representation to be recorded in the history
 * table; they will then be interpreted by the verifier.
 */
public abstract class OperationList {

  private String loadName;
  private List<String> recordRepresentation;

  public OperationList(String loadName, List<String> recordRepresentation) {
    this.loadName = loadName;
    this.recordRepresentation = recordRepresentation;
  }

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
    if (!(other instanceof OperationList))
      return false;

    OperationList otherOps = (OperationList) other;
    return this.loadName.equals(otherOps.loadName)
            && this.recordRepresentation.equals(otherOps.recordRepresentation);
  }
}
