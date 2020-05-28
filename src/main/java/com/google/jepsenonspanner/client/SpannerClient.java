package com.google.jepsenonspanner.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Type;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.StaleOperation;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SpannerClient {

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;
  private DatabaseId databaseId;

  private static final String TESTING_TABLE_NAME = "Testing";
  private static final String HISTORY_TABLE_NAME = "History";
  private static final String KEY_COLUMN_NAME = "Key";
  private static final String VALUE_COLUMN_NAME = "Value";

  private static final Type Record = Type.struct(Arrays.asList(
          Type.StructField.of("OpType", Type.bool()),
          Type.StructField.of("Key", Type.string()),
          Type.StructField.of("Value", Type.string())
  ));

  public SpannerClient(String instanceId, String dbId) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    databaseId = DatabaseId.of(options.getProjectId(), instanceId, dbId);
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

    try {
      Database db = op.get();
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      // TODO: find a way to identify repeated create table and ignore that error
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public void executeOp(List<? extends Operation> ops) {
    if (ops.isEmpty()) {
      throw new RuntimeException("Empty operation to execute");
    }
    Operation firstOp = ops.get(0);
    if (firstOp instanceof StaleOperation) {
      StaleOperation firstStaleOp = (StaleOperation) firstOp;
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      for (Operation operation : ops) {
        StaleOperation op = (StaleOperation) operation;
        keySetBuilder.addKey(Key.of(op.getKey()));
      }
      List<StaleOperation> result = new ArrayList<>();
      try (ResultSet resultSet = client.singleUse(firstStaleOp.isBounded() ?
              TimestampBound.ofMaxStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS) :
              TimestampBound.ofExactStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS))
              .read(TESTING_TABLE_NAME, keySetBuilder.build(), Arrays.asList(KEY_COLUMN_NAME,
                      VALUE_COLUMN_NAME))) {
        while (resultSet.next()) {
          result.add(new StaleOperation(resultSet.getString(0), (int) resultSet.getLong(1),
                  firstStaleOp.isBounded(), firstStaleOp.getStaleness()));
        }
      }
    } else {
      // if one is stale read, whole list should be stale reads
      //
      client.readWriteTransaction().run(
        new TransactionRunner.TransactionCallable<Void>() {
          @Nullable
          @Override
          public Void run(TransactionContext transaction) throws Exception {
            return null;
          }
        }
      )
    }
  }
}
