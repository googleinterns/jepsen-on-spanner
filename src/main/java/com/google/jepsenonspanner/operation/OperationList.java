package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Executor;

import java.util.List;

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
