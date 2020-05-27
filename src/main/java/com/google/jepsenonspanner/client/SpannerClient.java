package com.google.jepsenonspanner.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.jepsenonspanner.loadgenerator.Operation;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class SpannerClient {

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;
  private DatabaseId databaseId;

  public SpannerClient(String instanceId, String dbId) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    databaseId = DatabaseId.of(options.getProjectId(), instanceId, dbId);
    client = spanner.getDatabaseClient(databaseId);
    adminClient = spanner.getDatabaseAdminClient();

    // create the initial tables for history
    OperationFuture<Database, CreateDatabaseMetadata> op =
            adminClient.createDatabase(instanceId, dbId, Arrays.asList(
                    "CREATE TABLE History (\n" +
                            "    Time       TIMESTAMP NOT NULL\n" +
                            "    OPTIONS (allow_commit_timestamp = true),\n" +
                            "    OpType     STRING(6) NOT NULL,\n" +
                            "    Value      ARRAY<STRING(MAX)> NOT NULL,\n" +
                            "    ProcessID  INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(Time, OpType)",
                    "CREATE TABLE Testing (\n" +
                            "    Key   STRING(MAX) NOT NULL,\n" +
                            "    Value STRING(MAX) NOT NULL,\n" +
                            ") PRIMARY KEY(Key)\n"));

    try {
      Database db = op.get();
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public void executeOp(Operation op) {
    
  }
}
