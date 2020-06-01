package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Executor;

public interface OperationList {
  public void executeOps(Executor client);

//  public void record(Recorder recorder);
}
