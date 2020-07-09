package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.jepsenonspanner.operation.OpRepresentation;
import com.google.jepsenonspanner.operation.OperationException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Record.FAIL_STR;
import static com.google.jepsenonspanner.client.Record.INFO_STR;
import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutorTest {

  private static final String PROJECT_ID = "jepsen-on-spanner-with-gke";
  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "example-db";
  private static int PID = 0;
  static Executor executor = new Executor(PROJECT_ID, INSTANCE_ID, DATABASE_ID, PID, /*init=*/true);
  private static String LOAD_NAME = "transfer";
  private List<String> keys;
  private List<OpRepresentation> representations;

  @BeforeAll
  static void setUp() {
    executor.createTables();
  }

  @BeforeEach
  void setUpRepresentation() {
    keys = new ArrayList<>();
    representations = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      keys.add(String.valueOf(i));
      representations.add(OpRepresentation.createOtherRepresentation(String.valueOf(i)));
    }
  }

  @AfterEach
  void cleanup() {
    executor.cleanUp();
  }

  @Test
  void testInitKeyValues() {
    HashMap<String, Long> initKVs = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      initKVs.put(Integer.toString(i), (long) i);
    }

    executor.initKeyValues(initKVs);
    Pair<HashMap<String, Long>, Timestamp> result =
            executor.readKeys(new ArrayList<>(initKVs.keySet()), /*staleness=*/0,
            /*bounded=*/false);
    HashMap<String, Long> kvResult = result.getLeft();
    assertEquals(kvResult, initKVs);
  }

  public void checkSingleRecord(Struct row, String opType, String load, List<OpRepresentation> representation,
                        long processID, Timestamp timestamp) {
    assertEquals(row.getString(Executor.RECORD_TYPE_COLUMN_NAME), opType);
    assertEquals(row.getString(Executor.OP_NAME_COLUMN_NAME), load);
    assertEquals(row.getStringList(Executor.VALUE_COLUMN_NAME),
            representation.stream().map(OpRepresentation::toString).collect(Collectors.toList()));
    assertEquals(row.getLong(Executor.PID_COLUMN_NAME), processID);
    if (timestamp != null)
      assertEquals(row.getTimestamp(Executor.TIME_COLUMN_NAME), timestamp);
  }

  public void checkSingleRecord(Struct row, String opType, Timestamp timestamp) {
    checkSingleRecord(row, opType, LOAD_NAME, representations, PID, timestamp);
  }

  public void checkSingleRecord(Struct row, String opType) {
    checkSingleRecord(row, opType, /*timestamp-*/null);
  }

  ResultSet retrieveAllRecords() {
    return executor.getClient().singleUse().read(Executor.HISTORY_TABLE_NAME,
            KeySet.all(),
            Arrays.asList(Executor.TIME_COLUMN_NAME, Executor.RECORD_TYPE_COLUMN_NAME,
                    Executor.OP_NAME_COLUMN_NAME, Executor.VALUE_COLUMN_NAME,
                    Executor.PID_COLUMN_NAME));
  }

  void checkFailOrInfo(String opType) {
    if (opType.equals(FAIL_STR.getName())) {
      executor.recordFail(LOAD_NAME, representations);
    } else {
      executor.recordInfo(LOAD_NAME, representations);
    }

    try (ResultSet resultSet = retrieveAllRecords()) {
      while (resultSet.next()) {
        checkSingleRecord(resultSet.getCurrentRowAsStruct(), opType);
      }
    }
  }

  @Test
  void testRecordInvoke() {
    int staleness = 100000;
    Timestamp staleRecordTimestamp = executor.recordInvoke(LOAD_NAME, representations, staleness);
    Timestamp recordTimestamp = executor.recordInvoke(LOAD_NAME, representations);

    try (ResultSet resultSet = retrieveAllRecords()) {
      List<Timestamp> timestamps = new ArrayList<>();
      while (resultSet.next()) {
        checkSingleRecord(resultSet.getCurrentRowAsStruct(),
                INVOKE_STR.getName()); // Do not test for timestamp here
        timestamps.add(resultSet.getTimestamp(Executor.TIME_COLUMN_NAME));
      }
      assertEquals(timestamps.size(), 2);
      assertTrue(timestamps.get(1).toSqlTimestamp().getTime() -
              timestamps.get(0).toSqlTimestamp().getTime() > staleness);
      assertEquals(timestamps.get(0), staleRecordTimestamp);
      assertEquals(timestamps.get(1), recordTimestamp);
    }
  }

  void checkRecordCompleteWithStaleness(int staleness) {
    long commitTimestampInMilliseconds = 10000000;
    Timestamp invokeTimestamp = staleness == 0 ?
            executor.recordInvoke(LOAD_NAME, representations) :
            executor.recordInvoke(LOAD_NAME, representations, staleness);
    Timestamp commitTimestamp = Timestamp.ofTimeMicroseconds(commitTimestampInMilliseconds);
    executor.recordComplete(LOAD_NAME, representations, commitTimestamp, invokeTimestamp);

    try (ResultSet resultSet = retrieveAllRecords()) {
      resultSet.next();
      checkSingleRecord(resultSet.getCurrentRowAsStruct(), INVOKE_STR.getName(), commitTimestamp);
      resultSet.next();
      checkSingleRecord(resultSet.getCurrentRowAsStruct(), OK_STR.getName(), commitTimestamp);
    }
  }

  @Test
  void testRecordComplete() {
    checkRecordCompleteWithStaleness(0);
  }

  @Test
  void testStaleRecordComplete() {
    checkRecordCompleteWithStaleness(10000);
  }

  @Test
  void testRecordFail() {
    checkFailOrInfo(FAIL_STR.getName());
  }

  @Test
  void testRecordInfo() {
    checkFailOrInfo(INFO_STR.getName());
  }

  @Test
  void testRunTxn() {
    HashMap<String, Long> kvs = new HashMap<>();
    for (String key : keys) {
      kvs.put(key, Long.valueOf(key));
    }
    executor.initKeyValues(kvs);

    // IDE suggest I could replace this with lambda, but I think this looks clearer?
    executor.runTxn(new Executor.TransactionFunction() {
      @Override
      public void run(TransactionContext transaction) {
        for (String key : keys) {
          long value = executor.executeTransactionalRead(key, transaction);
          assertEquals(value, Long.valueOf(key).longValue());
          executor.executeTransactionalWrite(key, value * value, transaction);
          long valueSquared = executor.executeTransactionalRead(key, transaction);
          assertEquals(valueSquared, value * value);
        }
      }
    });

    HashMap<String, Long> result = executor.readKeys(keys, 0, false).getLeft();
    for (Map.Entry<String, Long> kv : result.entrySet()) {
      long keyAsLong = Long.parseLong(kv.getKey());
      assertEquals(kv.getValue().longValue(), keyAsLong * keyAsLong);
    }
  }

  @Test
  void testAbortTxn() throws Throwable {
    HashMap<String, Long> kvs = new HashMap<>();
    for (String key : keys) {
      kvs.put(key, Long.valueOf(key));
    }
    executor.initKeyValues(kvs);

    try {
      executor.runTxn(new Executor.TransactionFunction() {
        @Override
        public void run(TransactionContext transaction) {
          for (String key : keys) {
            executor.executeTransactionalWrite(key, -1, transaction);
            if (Integer.parseInt(key) > 8)
              throw new OperationException("Testing");
          }
        }
      });
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.UNKNOWN && e.getCause() instanceof OperationException) {
        HashMap<String, Long> result = executor.readKeys(keys, 0, false).getLeft();
        // current transaction aborted, should not observe any change
        assertEquals(result, kvs);
      } else {
        throw e.getCause();
      }
    }
  }

  @Test
  void testInvalidReads() {
    List<String> nonExistKeys = Collections.singletonList("NON_EXIST");
    Exception e = assertThrows(OperationException.class, () -> {
      executor.readKeys(nonExistKeys, 0, false);
    });
    assertTrue(e.getMessage().contains(String.format("Non-existent key found in read of %s",
            nonExistKeys)));
  }

  @Test
  void testExtractHistory() {
    Timestamp timestamp = executor.recordInvoke(LOAD_NAME, representations);
    executor.recordComplete(LOAD_NAME, representations, Timestamp.ofTimeMicroseconds(100000), timestamp);
    executor.extractHistory();
  }

  @Test
  void testExtractRealTimeHistory() {
    Timestamp timestamp = executor.recordInvoke(LOAD_NAME, representations);
    executor.recordComplete(LOAD_NAME, representations, Timestamp.ofTimeMicroseconds(100000), timestamp);
    executor.extractHistoryWithTimestamp();
  }
}