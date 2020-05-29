package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.SpannerClient;

public interface OperationList {
  public void executeOps(SpannerClient client);

  public void record(Recorder recorder);
}
