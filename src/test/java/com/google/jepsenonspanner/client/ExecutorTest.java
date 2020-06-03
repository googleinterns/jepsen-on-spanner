package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ExecutorTest {

  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "example-db";
  private static int PID = 0;
  static Executor client = new Executor(INSTANCE_ID, DATABASE_ID, PID);

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

  @Test
  void testRecordInvoke() {
    String loadName = "transfer";
    List<String> representation = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      representation.add(String.valueOf(i));
    }
    client.recordInvoke(loadName, representation, 0);
    client.recordInvoke(loadName, representation, 10000000);

    ResultSet resultSet = client.getClient().singleUse().read(Executor.HISTORY_TABLE_NAME,
            KeySet.all(),
            Arrays.asList(Executor.TIME_COLUMN_NAME, Executor.OPTYPE_COLUMN_NAME,
                    Executor.LOAD_COLUMN_NAME, Executor.VALUE_COLUMN_NAME,
                    Executor.PID_COLUMN_NAME));
    while (resultSet.next()) {
      assertEquals(resultSet.getString(Executor.OPTYPE_COLUMN_NAME), Executor.INVOKE);
      assertEquals(resultSet.getString(Executor.LOAD_COLUMN_NAME), loadName);
      assertEquals(resultSet.getStringList(Executor.VALUE_COLUMN_NAME), representation);
      assertEquals(resultSet.getLong(Executor.PID_COLUMN_NAME), PID);
      System.out.println(resultSet.getTimestamp(Executor.TIME_COLUMN_NAME));
    }
  }
}