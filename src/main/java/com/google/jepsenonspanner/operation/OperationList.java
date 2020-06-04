package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Executor;

import java.util.List;

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

  public abstract void executeOps(Executor client);

  public String getLoadName() {
    return loadName;
  }

  public List<String> getRecordRepresentation() {
    return recordRepresentation;
  }
}
