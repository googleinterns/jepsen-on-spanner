package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutorTest {

  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "example-db";
  private static int PID = 0;
  static Executor client = new Executor(INSTANCE_ID, DATABASE_ID, PID);
  private static String LOAD_NAME = "transfer";
  private List<String> representation;

  @BeforeEach
  void setUpRepresentation() {
    representation = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      representation.add(String.valueOf(i));
    }
  }

  @AfterEach
  void cleanup() {
    client.getClient().write(Arrays.asList(Mutation.delete(Executor.TESTING_TABLE_NAME, KeySet.all()),
            Mutation.delete(Executor.HISTORY_TABLE_NAME, KeySet.all())));
  }

  @Test
  void testInitKeyValues() {
    HashMap<String, Long> initKVs = new HashMap<>();
    for (int i = 0; i < 4; i++) {
      initKVs.put(Integer.toString(i), (long) i);
    }

    client.initKeyValues(initKVs);
    Pair<HashMap<String, Long>, Timestamp> result =
            client.readKeys(new ArrayList<>(initKVs.keySet()), /*staleness=*/0,
            /*bounded=*/false);
    HashMap<String, Long> kvResult = result.getLeft();
    assertEquals(kvResult, initKVs);
  }

  void checkSingleRecord(Struct row, String opType, String load, List<String> representation,
                        long processID, Timestamp timestamp) {
    assertEquals(row.getString(Executor.OPTYPE_COLUMN_NAME), opType);
    assertEquals(row.getString(Executor.LOAD_COLUMN_NAME), load);
    assertEquals(row.getStringList(Executor.VALUE_COLUMN_NAME), representation);
    assertEquals(row.getLong(Executor.PID_COLUMN_NAME), processID);
    if (timestamp != null)
      assertEquals(row.getTimestamp(Executor.TIME_COLUMN_NAME), timestamp);
  }

  void checkSingleRecord(Struct row, String opType, Timestamp timestamp) {
    checkSingleRecord(row, opType, LOAD_NAME, representation, PID, timestamp);
  }

  void checkSingleRecord(Struct row, String opType) {
    checkSingleRecord(row, opType, /*timestamp-*/null);
  }

  ResultSet retrieveAllRecords() {
    return client.getClient().singleUse().read(Executor.HISTORY_TABLE_NAME,
            KeySet.all(),
            Arrays.asList(Executor.TIME_COLUMN_NAME, Executor.OPTYPE_COLUMN_NAME,
                    Executor.LOAD_COLUMN_NAME, Executor.VALUE_COLUMN_NAME,
                    Executor.PID_COLUMN_NAME));
  }

  void checkFailOrInfo(String opType) {
    if (opType == Executor.FAIL)
      client.recordFail(LOAD_NAME, representation);
    else
      client.recordInfo(LOAD_NAME, representation);

    try (ResultSet resultSet = retrieveAllRecords()) {
      while (resultSet.next()) {
        checkSingleRecord(resultSet.getCurrentRowAsStruct(), opType);
      }
    }
  }

  @Test
  void testRecordInvoke() {
    int staleness = 100000;
    Timestamp staleRecordTimestamp = client.recordInvoke(LOAD_NAME, representation, staleness);
    Timestamp recordTimestamp = client.recordInvoke(LOAD_NAME, representation);

    try (ResultSet resultSet = retrieveAllRecords()) {
      List<Timestamp> timestamps = new ArrayList<>();
      while (resultSet.next()) {
        checkSingleRecord(resultSet.getCurrentRowAsStruct(),
                Executor.INVOKE); // Do not test for timestamp here
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
            client.recordInvoke(LOAD_NAME, representation) :
            client.recordInvoke(LOAD_NAME, representation, staleness);
    Timestamp commitTimestamp = Timestamp.ofTimeMicroseconds(commitTimestampInMilliseconds);
    client.recordComplete(LOAD_NAME, representation, commitTimestamp, invokeTimestamp);

    try (ResultSet resultSet = retrieveAllRecords()) {
      resultSet.next();
      checkSingleRecord(resultSet.getCurrentRowAsStruct(), Executor.INVOKE, commitTimestamp);
      resultSet.next();
      checkSingleRecord(resultSet.getCurrentRowAsStruct(), Executor.OK, commitTimestamp);
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
    checkFailOrInfo(Executor.FAIL);
  }

  @Test
  void testRecordInfo() {
    checkFailOrInfo(Executor.INFO);
  }

  @Test
  void testRunTxn() {
    HashMap<String, Long> kvs = new HashMap<>();
    for (String key : representation) {
      kvs.put(key, Long.valueOf(key));
    }
    client.initKeyValues(kvs);

    // IDE suggest I could replace this with lambda, but I think this looks clearer?
    client.runTxn(new Executor.TransactionFunction() {
      @Override
      public void run() {
        for (String key : representation) {
          long value = client.executeTransactionalRead(key);
          assertEquals(value, Long.valueOf(key).longValue());
          client.executeTransactionalWrite(key, value * value);
          long valueSquared = client.executeTransactionalRead(key);
          assertEquals(valueSquared, value * value);
        }
      }
    });

    HashMap<String, Long> result = client.readKeys(representation, 0, false).getLeft();
    for (Map.Entry<String, Long> kv : result.entrySet()) {
      long keyAsLong = Long.parseLong(kv.getKey());
      assertEquals(kv.getValue().longValue(), keyAsLong * keyAsLong);
    }
  }

  @Test
  void testAbortTxn() throws Throwable {
    HashMap<String, Long> kvs = new HashMap<>();
    for (String key : representation) {
      kvs.put(key, Long.valueOf(key));
    }
    client.initKeyValues(kvs);

    try {
      client.runTxn(new Executor.TransactionFunction() {
        @Override
        public void run() {
          for (String key : representation) {
            client.executeTransactionalWrite(key, -1);
            if (Integer.parseInt(key) > 8)
              throw new RuntimeException("Testing");
          }
        }
      });
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.UNKNOWN) {
        HashMap<String, Long> result = client.readKeys(representation, 0, false).getLeft();
        assertEquals(result, kvs);
      } else {
        throw e.getCause();
      }
    }
  }
}