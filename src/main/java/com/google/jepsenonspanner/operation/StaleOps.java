package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.SpannerClient;
import io.opencensus.trace.Span;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StaleOps implements OperationList {

  private List<StaleOperation> ops;
  private int staleness;
  private boolean bounded;
  private boolean failed;

  public StaleOps(List<StaleOperation> ops, int staleness, boolean bounded) {
    this.ops = ops;
    this.staleness = staleness;
    this.bounded = bounded;
    this.failed = false;
  }

  @Override
  public void executeOps(SpannerClient client) {
    List<String> keys = new ArrayList<>();
    for (StaleOperation op : ops) {
      keys.add(op.getKey());
    }
    try {
      Pair<HashMap<String, Long>, Timestamp> result = client.readKeys(keys, staleness, bounded);
      HashMap<String, Long> keyValues = result.getLeft();
      Timestamp readTimeStamp = result.getRight();
    } catch (SpannerException e) {
      this.failed = true;
    }
  }

  public void recordInvoke(SpannerClient client, List<String> keys) throws RuntimeException {
    try {
      client.recordInvoke(keys);
    } catch (SpannerException e) {
      throw new RuntimeException("RECORDER ERROR");
    }
  }

  public void recordComplete(SpannerClient client, HashMap<String, Long> keyValues,
                             Timestamp readTimeStamp) throws RuntimeException {
    try {
      client.recordComplete(keyValues, readTimeStamp);
    } catch (SpannerException e) {
      throw new RuntimeException("RECORDER ERROR");
    }
  }
}
