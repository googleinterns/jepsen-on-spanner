package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import com.google.jepsenonspanner.client.Executor;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadTransaction extends OperationList {

  private List<String> keys;
  private int staleness;
  private boolean bounded;

  public ReadTransaction(String loadName, List<String> recordRepresentation, List<String> keys,
                         int staleness, boolean bounded) {
    super(loadName, recordRepresentation);
    this.keys = keys;
    this.staleness = staleness;
    this.bounded = bounded;
  }

  public static ReadTransaction createStrongRead(String loadName, List<String> keys) {
    return new ReadTransaction(loadName, null, keys, /*staleness=*/0, /*bounded
    =*/false);
  }

  public static ReadTransaction createBoundedStaleRead(String loadName, List<String> keys,
                                                       int staleness) {
    return new ReadTransaction(loadName, null, keys, staleness, /*bounded=*/true);
  }

  public static ReadTransaction createExactStaleRead(String loadName, List<String> keys,
                                                     int staleness) {
    return new ReadTransaction(loadName, null, keys, staleness, /*bounded=*/false);
  }

  @Override
  public void executeOps(Executor client) {
    try {
      client.recordInvoke(getLoadName(), getRecordRepresentation(), staleness);
      Pair<HashMap<String, Long>, Timestamp> result = client.readKeys(keys, staleness, bounded);
      HashMap<String, Long> keyValues = result.getLeft();
      Timestamp readTimeStamp = result.getRight();
      List<String> recordRepresentation = new ArrayList<>();
      for (Map.Entry<String, Long> kv : keyValues.entrySet()) {
        recordRepresentation.add(String.format("%s %d", kv.getKey(), kv.getValue()));
      }
      client.recordComplete(getLoadName(), recordRepresentation, readTimeStamp);
    } catch (SpannerException e) {
      // TODO: figure out how to differentiate between fail and info
      client.recordFail(getLoadName(), getRecordRepresentation());
    }
  }
}
