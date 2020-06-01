package com.google.jepsenonspanner.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
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
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.StaleOperation;
import com.google.jepsenonspanner.operation.TransactionalOperation;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SpannerClient {

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;
  private DatabaseId databaseId;
  private int processID;

  private static final String TESTING_TABLE_NAME = "Testing";
  private static final String HISTORY_TABLE_NAME = "History";
  private static final String KEY_COLUMN_NAME = "Key";
  private static final String VALUE_COLUMN_NAME = "Value";
  private static final String OPTYPE_COLUMN_NAME = "OpType";
  private static final String TIME_COLUMN_NAME = "Time";
  private static final String PID_COLUMN_NAME = "ProcessID";
  private static final String INVOKE = "invoke";

  private static final Type Record = Type.struct(Arrays.asList(
          Type.StructField.of(OPTYPE_COLUMN_NAME, Type.bool()),
          Type.StructField.of(KEY_COLUMN_NAME, Type.string()),
          Type.StructField.of(VALUE_COLUMN_NAME, Type.int64())
  ));

  public SpannerClient(String instanceId, String dbId, int processID) {
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
                            "    Value      ARRAY<STRUCT<BOOL, STRING(MAX), INT64>> NOT NULL,\n" +
                            "    ProcessID  INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(Time, OpType)",
                    "CREATE TABLE " + TESTING_TABLE_NAME + " (\n" +
                            "    Key   STRING(MAX) NOT NULL,\n" +
                            "    Value STRING(MAX) NOT NULL,\n" +
                            ") PRIMARY KEY(Key)\n"));

    System.out.println("Here 3.5");
    try {
      Database db = op.get();
      System.out.println("Here 4");
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      // TODO: find a way to identify repeated create table and ignore that error
      System.out.println("Here 5");
      System.out.println(e.getMessage());
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public DatabaseClient getDbClient() { return client; }

  public Pair<HashMap<String, Long>, Timestamp> readKeys(List<String> keys, int staleness,
                                                   boolean bounded) throws SpannerException {
    HashMap<String, Long> result = new HashMap<>();
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String key : keys) {
      keySetBuilder.addKey(Key.of(key));
    }

    Timestamp readTimeStamp = null;
    try (ReadOnlyTransaction txn = client.singleUseReadOnlyTransaction(bounded ?
            TimestampBound.ofMaxStaleness(staleness, TimeUnit.MILLISECONDS) :
            TimestampBound.ofExactStaleness(staleness, TimeUnit.MILLISECONDS))) {
      ResultSet resultSet = txn.read(TESTING_TABLE_NAME, keySetBuilder.build(),
              Arrays.asList(KEY_COLUMN_NAME, VALUE_COLUMN_NAME));
      readTimeStamp = txn.getReadTimestamp();
      while (resultSet.next()) {
        result.put(resultSet.getString(KEY_COLUMN_NAME), resultSet.getLong(VALUE_COLUMN_NAME));
      }
    }
    return Pair.of(result, readTimeStamp);
  }

  public void runTxn(Consumer<TransactionContext> transactionToRun) {
    client.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
      @Nullable
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transactionToRun.accept(transaction);
        return null;
      }
    });
  }

  public KeySet getReadKeySet(List<? extends Operation> ops) {
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (Operation op : ops) {
      keySetBuilder.addKey(Key.of(op.getKey()));
    }
    return keySetBuilder.build();
  }

  public long executeTransactionalRead(TransactionalOperation op, TransactionContext transaction) {
    Struct row = transaction.readRow(TESTING_TABLE_NAME, Key.of(op.getKey()),
            Arrays.asList(KEY_COLUMN_NAME, VALUE_COLUMN_NAME));
    return row.getLong(VALUE_COLUMN_NAME);
  }

  public void executeTransactionalWrite(TransactionalOperation op, TransactionContext transaction) {
    transaction.buffer(Mutation.newUpdateBuilder(TESTING_TABLE_NAME)
            .set(KEY_COLUMN_NAME).to(op.getKey()).set(VALUE_COLUMN_NAME).to(op.getValue()).build());
  }

  public void recordInvoke(List<String> keys) {
    List<Struct> recordOps = new ArrayList<>();
    for (String key : keys) {
      recordOps.add(Struct.newBuilder()
              .set(OPTYPE_COLUMN_NAME).to(true).set(KEY_COLUMN_NAME).to(key).build());
    }
    client.write(Arrays.asList(Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                    .set(TIME_COLUMN_NAME).to(Value.COMMIT_TIMESTAMP)
                    .set(OPTYPE_COLUMN_NAME).to(INVOKE)
                    .set(VALUE_COLUMN_NAME).toStructArray(Record, recordOps)
                    .set(PID_COLUMN_NAME).to(processID).build()));
  }

  public void recordComplete(HashMap<String, Long> keyValues, Timestamp timestamp) {
    List<Struct> recordOps = new ArrayList<>();
    for (Map.Entry<String, Long> kv : keyValues.entrySet()) {
      recordOps.add(Struct.newBuilder()
              .set(OPTYPE_COLUMN_NAME).to(true)
              .set(KEY_COLUMN_NAME).to(kv.getKey())
              .set(VALUE_COLUMN_NAME).to(kv.getValue()).build());
    } 
    client.write(Arrays.asList(Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
            .set(TIME_COLUMN_NAME).to(timestamp)
            .set(OPTYPE_COLUMN_NAME).to(INVOKE)
            .set(VALUE_COLUMN_NAME).toStructArray(Record, recordOps)
            .set(PID_COLUMN_NAME).to(processID).build()));
  }
}
