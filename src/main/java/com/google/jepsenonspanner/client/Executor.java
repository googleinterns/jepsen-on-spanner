package com.google.jepsenonspanner.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Executor {

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;
  private DatabaseId databaseId;
  private int processID;
  private TransactionContext currentActiveTransaction;

  static final String TESTING_TABLE_NAME = "Testing";
  static final String HISTORY_TABLE_NAME = "History";
  static final String KEY_COLUMN_NAME = "Key";
  static final String VALUE_COLUMN_NAME = "Value";
  static final String OPTYPE_COLUMN_NAME = "OpType";
  static final String TIME_COLUMN_NAME = "Time";
  static final String PID_COLUMN_NAME = "ProcessID";
  static final String LOAD_COLUMN_NAME = "Load";
  static final String INVOKE = "invoke";
  static final String OK = "ok";
  static final String FAIL = "fail";
  static final String INFO = "info";

  private static final Type Record = Type.struct(Arrays.asList(
          Type.StructField.of(OPTYPE_COLUMN_NAME, Type.bool()),
          Type.StructField.of(KEY_COLUMN_NAME, Type.string()),
          Type.StructField.of(VALUE_COLUMN_NAME, Type.int64())
  ));

  public interface TransactionFunction {
    void run();
  }

  public Executor(String instanceId, String dbId, int processID) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    this.databaseId = DatabaseId.of(options.getProjectId(), instanceId, dbId);
    this.client = spanner.getDatabaseClient(databaseId);
    this.adminClient = spanner.getDatabaseAdminClient();
    this.processID = processID;

    // create the initial tables for history
    OperationFuture<Database, CreateDatabaseMetadata> op =
            adminClient.createDatabase(instanceId, dbId, Arrays.asList(
                    "CREATE TABLE " + HISTORY_TABLE_NAME + " (\n" +
                            "    Time       TIMESTAMP NOT NULL\n" +
                            "    OPTIONS (allow_commit_timestamp = true),\n" +
                            "    OpType     STRING(6) NOT NULL,\n" +
                            "    Load   STRING(MAX) NOT NULL,\n" +
                            "    Value      ARRAY<STRING(MAX)>,\n" +
                            "    ProcessID  INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(Time, ProcessID, OpType, Load)",
                    "CREATE TABLE " + TESTING_TABLE_NAME + " (\n" +
                            "    Key   STRING(MAX) NOT NULL,\n" +
                            "    Value INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(Key)\n"));

    try {
      Database db = op.get();
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      SpannerException se = (SpannerException) e.getCause();
      if (se.getErrorCode() != ErrorCode.ALREADY_EXISTS) {
        throw se;
      }
    } catch (InterruptedException e) {
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public Pair<HashMap<String, Long>, Timestamp> readKeys(List<String> keys, int staleness,
                                                   boolean bounded) throws SpannerException {
    HashMap<String, Long> result = new HashMap<>();
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String key : keys) {
      keySetBuilder.addKey(Key.of(key));
    }

    Timestamp readTimeStamp;
    try (ReadOnlyTransaction txn = staleness == 0 ?
            client.singleUseReadOnlyTransaction() : client.singleUseReadOnlyTransaction(bounded ?
            TimestampBound.ofMaxStaleness(staleness, TimeUnit.MILLISECONDS) :
            TimestampBound.ofExactStaleness(staleness, TimeUnit.MILLISECONDS))) {
      ResultSet resultSet = txn.read(TESTING_TABLE_NAME, keySetBuilder.build(),
              Arrays.asList(KEY_COLUMN_NAME, VALUE_COLUMN_NAME));
      while (resultSet.next()) {
        result.put(resultSet.getString(KEY_COLUMN_NAME), resultSet.getLong(VALUE_COLUMN_NAME));
      }
      readTimeStamp = txn.getReadTimestamp();
    }
    return Pair.of(result, readTimeStamp);
  }

  public Timestamp runTxn(TransactionFunction transactionToRun) {
    TransactionRunner transactionRunner = client.readWriteTransaction();
    transactionRunner.run(new TransactionRunner.TransactionCallable<Void>() {
      @Nullable
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        currentActiveTransaction = transaction;
        transactionToRun.run();
        currentActiveTransaction = null;
        return null;
      }
    });
    return transactionRunner.getCommitTimestamp();
  }

  public long executeTransactionalRead(String key) throws RuntimeException {
    if (currentActiveTransaction == null)
      throw new RuntimeException("This method should be used in a ReadWriteTransaction");
    try (ResultSet resultSet = currentActiveTransaction.executeQuery(
            Statement.of(String.format("SELECT %s FROM %s WHERE %s = \"%s\"", VALUE_COLUMN_NAME,
                    TESTING_TABLE_NAME, KEY_COLUMN_NAME, key)))) {
      resultSet.next();
      return resultSet.getLong(VALUE_COLUMN_NAME);
    }
  }

  public void executeTransactionalWrite(String key, long value) {
    if (currentActiveTransaction == null)
      throw new RuntimeException("This method should be used in a ReadWriteTransaction");
    currentActiveTransaction.executeUpdate(
            Statement.of(String.format("UPDATE %s SET %s = %s WHERE %s = \"%s\"",
                    TESTING_TABLE_NAME, VALUE_COLUMN_NAME, value, KEY_COLUMN_NAME, key)));
  }

  public Timestamp recordInvoke(String loadName, List<String> representation, int staleness) {
    return recordList(loadName, representation, INVOKE, staleness);
  }

  public Timestamp recordInvoke(String loadName, List<String> representation) {
    return recordInvoke(loadName, representation, /*staleness=*/0);
  }

  public void recordComplete(String loadName, List<String> recordRepresentation,
                             Timestamp commitTimestamp, Timestamp invokeTimestamp) {
    try {
      client.write(Arrays.asList(
              Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                      .set(TIME_COLUMN_NAME).to(commitTimestamp)
                      .set(OPTYPE_COLUMN_NAME).to(OK)
                      .set(LOAD_COLUMN_NAME).to(loadName)
                      .set(VALUE_COLUMN_NAME).toStringArray(recordRepresentation)
                      .set(PID_COLUMN_NAME).to(processID).build(),
              Mutation.delete(HISTORY_TABLE_NAME, Key.of(invokeTimestamp, processID, INVOKE,
                      loadName)),
              Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                      .set(TIME_COLUMN_NAME).to(commitTimestamp)
                      .set(OPTYPE_COLUMN_NAME).to(INVOKE)
                      .set(LOAD_COLUMN_NAME).to(loadName)
                      .set(VALUE_COLUMN_NAME).toStringArray(recordRepresentation)
                      .set(PID_COLUMN_NAME).to(processID).build()));
    } catch (SpannerException e) {
      throw new RuntimeException("RECORDER ERROR");
    }
  }

  public void recordFail(String loadName, List<String> representation) {
    recordList(loadName, representation, FAIL, /*staleness=*/0);
  }

  public void recordInfo(String loadName, List<String> representation) {
    recordList(loadName, representation, INFO, /*staleness=*/0);
  }

  private Timestamp recordList(String loadName, List<String> representation, String opType,
                          int staleness) throws RuntimeException {
    try {
       Timestamp commitTimestamp =
               client.write(Collections.singletonList(Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                  .set(TIME_COLUMN_NAME).to(Value.COMMIT_TIMESTAMP)
                  .set(OPTYPE_COLUMN_NAME).to(opType)
                  .set(LOAD_COLUMN_NAME).to(loadName)
                  .set(VALUE_COLUMN_NAME).toStringArray(representation)
                  .set(PID_COLUMN_NAME).to(processID).build()));
      if (staleness != 0) {
        Timestamp staleTimestamp =
                Timestamp.ofTimeMicroseconds((commitTimestamp.toSqlTimestamp().getTime() - staleness) * 1000);
        client.write(Arrays.asList(
                Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                        .set(TIME_COLUMN_NAME).to(staleTimestamp)
                        .set(OPTYPE_COLUMN_NAME).to(opType)
                        .set(LOAD_COLUMN_NAME).to(loadName)
                        .set(VALUE_COLUMN_NAME).toStringArray(representation)
                        .set(PID_COLUMN_NAME).to(processID).build(),
                Mutation.delete(HISTORY_TABLE_NAME,
                        Key.of(commitTimestamp, processID, opType, loadName))));
        return staleTimestamp;
      }
      return commitTimestamp;
    } catch (SpannerException e) {
      System.out.println(staleness);
      System.out.println(e.getCause());
      e.printStackTrace();
      throw new RuntimeException("RECORDER ERROR");
    }
  }

  public void initKeyValues(HashMap<String, Long> initialKVs) {
    List<Mutation> mutations = new ArrayList<>();
    for (Map.Entry<String, Long> kv : initialKVs.entrySet()) {
      mutations.add(Mutation.newInsertBuilder(TESTING_TABLE_NAME)
              .set(KEY_COLUMN_NAME).to(kv.getKey())
              .set(VALUE_COLUMN_NAME).to(kv.getValue()).build());
    }
    try {
      client.write(mutations);
    } catch (SpannerException e) {
      System.out.println(e.getMessage());
      throw new RuntimeException("RECORDER ERROR");
    }
  }

  DatabaseClient getClient() { return client; }
}
