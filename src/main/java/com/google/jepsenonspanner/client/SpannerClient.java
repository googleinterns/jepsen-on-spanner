package com.google.jepsenonspanner.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Type;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.StaleOperation;
import com.google.jepsenonspanner.operation.TransactionalOperation;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SpannerClient {

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;
  private DatabaseId databaseId;

  public static final String TESTING_TABLE_NAME = "Testing";
  public static final String HISTORY_TABLE_NAME = "History";
  public static final String KEY_COLUMN_NAME = "Key";
  public static final String VALUE_COLUMN_NAME = "Value";

  private static final Type Record = Type.struct(Arrays.asList(
          Type.StructField.of("OpType", Type.bool()),
          Type.StructField.of("Key", Type.string()),
          Type.StructField.of("Value", Type.string())
  ));

  public SpannerClient(String instanceId, String dbId) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    System.out.println("Here 1");
    Spanner spanner = options.getService();
    System.out.println("Here 2");
    databaseId = DatabaseId.of(options.getProjectId(), instanceId, dbId);
    System.out.println("Here 3");
    client = spanner.getDatabaseClient(databaseId);
    adminClient = spanner.getDatabaseAdminClient();

    // create the initial tables for history
    OperationFuture<Database, CreateDatabaseMetadata> op =
            adminClient.createDatabase(instanceId, dbId, Arrays.asList(
                    "CREATE TABLE " + HISTORY_TABLE_NAME + " (\n" +
                            "    Time       TIMESTAMP NOT NULL\n" +
                            "    OPTIONS (allow_commit_timestamp = true),\n" +
                            "    OpType     STRING(6) NOT NULL,\n" +
                            "    Value      ARRAY<STRING(MAX)> NOT NULL,\n" +
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

  public HashMap<String, Long> readKeys(List<String> keys, int staleness, boolean bounded) {
    HashMap<String, Long> result = new HashMap<>();
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String key : keys) {
      keySetBuilder.addKey(Key.of(key));
    }
    try (ResultSet resultSet = client.singleUse(bounded ?
            TimestampBound.ofMaxStaleness(staleness, TimeUnit.MILLISECONDS) :
            TimestampBound.ofExactStaleness(staleness, TimeUnit.MILLISECONDS))
            .read(TESTING_TABLE_NAME, keySetBuilder.build(), Arrays.asList(KEY_COLUMN_NAME,
                    VALUE_COLUMN_NAME))) {
      while (resultSet.next()) {
        result.put(resultSet.getString(KEY_COLUMN_NAME), resultSet.getLong(VALUE_COLUMN_NAME));
      }
    }
    return result;
  }

//  public void executeOp(List<? extends Operation> ops, Recorder recorder) {
//    if (ops.isEmpty()) {
//      throw new RuntimeException("Empty operation to execute");
//    }
//    Operation firstOp = ops.get(0);
//    if (firstOp instanceof StaleOperation) {
//      StaleOperation firstStaleOp = (StaleOperation) firstOp;
//      List<StaleOperation> result = new ArrayList<>();
//      try (ResultSet resultSet = client.singleUse(firstStaleOp.isBounded() ?
//              TimestampBound.ofMaxStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS) :
//              TimestampBound.ofExactStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS))
//              .read(TESTING_TABLE_NAME, getReadKeySet(ops), Arrays.asList(KEY_COLUMN_NAME,
//                      VALUE_COLUMN_NAME))) {
//        while (resultSet.next()) {
//          result.add(new StaleOperation(resultSet.getString(0), (int) resultSet.getLong(1),
//                  firstStaleOp.isBounded(), firstStaleOp.getStaleness()));
//        }
//      }
//      recorder.recordStaleOps(result, client);
//    } else {
//      // this is a transaction
//      //8
//      client.readWriteTransaction().run(
//        new TransactionRunner.TransactionCallable<Void>() {
//          @Nullable
//          @Override
//          public Void run(TransactionContext transaction) throws Exception {
//            List<TransactionalOperation> operations = (List<TransactionalOperation>) ops;
//            for (TransactionalOperation op : operations) {
//              long dependentValue = -1;
//              for (; op != null; op = op.getDependentOp()) {
//                if (!op.decideProceed(dependentValue)) {
//                  return null;
//                  // abort the whole transaction if anything is determined as unable to proceed
//                }
//                op.findDependentValue(dependentValue);
//                if (op.isRead()) {
//                  dependentValue = executeTransactionalRead(op, transaction);
//                } else {
//                  executeTransactionalWrite(op, transaction);
//                }
//              }
//            }
//            return null;
//          }
//        }
//      );
//    }
//  }

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
}
