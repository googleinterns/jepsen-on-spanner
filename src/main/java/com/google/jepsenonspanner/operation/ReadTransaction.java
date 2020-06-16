package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Executor;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * ReadTransaction class encapsulates a read-only transaction that can have a certain staleness,
 * whether bounded or exact.
 */
public class ReadTransaction extends Operation {

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

  /**
   * Creates a strong read. Note that the representation is initialized as the same as the keys.
   * This is because there will be no values read initially, so only keys are present in the
   * representations in the history table; they will be updated once the read returns.
   * @param loadName
   * @param keys
   */
  public static ReadTransaction createStrongRead(String loadName, List<String> keys) {
    return new ReadTransaction(loadName, keys, keys, /*staleness=*/0, /*bounded
    =*/false);
  }

  public static ReadTransaction createBoundedStaleRead(String loadName, List<String> keys,
                                                       int staleness) {
    return new ReadTransaction(loadName, keys, keys, staleness, /*bounded=*/true);
  }

  public static ReadTransaction createExactStaleRead(String loadName, List<String> keys,
                                                     int staleness) {
    return new ReadTransaction(loadName, keys, keys, staleness, /*bounded=*/false);
  }

  /**
   * The execution plan of a read-only transaction will:
   * - Write an "invoke" entry into the history table
   * - Read the results at the given staleness
   * - Write an "ok" entry into the history table and update the timestamp of the "invoke" entry
   * - If a RuntimeException is thrown, it will write a "fail" entry
   * - If a SpannerException is thrown, it will write an "info" entry
   */
  @Override
  public Consumer<Executor> getExecutionPlan() {
    return executor -> {
      Timestamp recordTimestamp = executor.recordInvoke(getLoadName(), getRecordRepresentation(),
              staleness);
      try {
        Pair<HashMap<String, Long>, Timestamp> result = executor.readKeys(keys, staleness, bounded);
        HashMap<String, Long> keyValues = result.getLeft();
        Timestamp readTimeStamp = result.getRight();

        // Update the representation to reflect the values read
        List<String> recordRepresentation = new ArrayList<>();
        for (Map.Entry<String, Long> kv : keyValues.entrySet()) {
          recordRepresentation.add(String.format("%s %d", kv.getKey(), kv.getValue()));
        }
        executor.recordComplete(getLoadName(), recordRepresentation, readTimeStamp,
                recordTimestamp);
      } catch (SpannerException e) {
        executor.recordInfo(getLoadName(), getRecordRepresentation());
      } catch (OperationException e) {
        if (staleness == 0) {
          executor.recordFail(getLoadName(), getRecordRepresentation());
        } else {
          executor.recordFail(getLoadName(), getRecordRepresentation(), recordTimestamp);
        }
      }
    };
  }

  @Override
  public String toString() {
    return super.toString() + " " + staleness + " " + (bounded ? "bounded" : "exact");
  }

  /** ALL TESTING FUNCTIONS BELOW */
  @VisibleForTesting
  public List<String> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  @VisibleForTesting
  public int getStaleness() {
    return staleness;
  }

  @VisibleForTesting
  public boolean getBounded() {
    return bounded;
  }
}
