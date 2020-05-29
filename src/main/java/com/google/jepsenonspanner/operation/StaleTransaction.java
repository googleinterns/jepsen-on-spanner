package com.google.jepsenonspanner.operation;

import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.SpannerClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StaleTransaction implements OperationList {

  private List<StaleOperation> ops;
  private int staleness;
  private boolean bounded;

  public StaleTransaction(List<StaleOperation> ops, int staleness, boolean bounded) {
    this.ops = ops;
    this.staleness = staleness;
    this.bounded = bounded;
  }

  @Override
  public void executeOps(SpannerClient client) {
    List<String> keys = new ArrayList<>();
    for (StaleOperation op : ops) {
      keys.add(op.getKey());
    }
    HashMap<String, Long> result = client.readKeys(keys, staleness, bounded);

  }

  @Override
  public void record(Recorder recorder) {

  }
}
