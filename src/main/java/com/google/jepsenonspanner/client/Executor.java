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

/**
 * Executor class encapsulates the details of a maintaining a client and facilitating its
 * communication with the Spanner instance. It provides interfaces for the operations /
 * transactions to execute read / writes, read a list of keys and recording various history logs.
 */
public class Executor {

  private DatabaseClient client; // maintain connection with the Spanner instance
  private int processID; // each executor will be assigned a unique ID
  private TransactionContext currentActiveTransaction; // This will be initialized only when
  // running transactions; see executeTransactionalRead/Write for more details

  public static final String TESTING_TABLE_NAME = "Testing";
  public static final String HISTORY_TABLE_NAME = "History";
  public static final String KEY_COLUMN_NAME = "Key";
  public static final String VALUE_COLUMN_NAME = "Value";
  public static final String OPTYPE_COLUMN_NAME = "OpType";
  public static final String TIME_COLUMN_NAME = "Time";
  public static final String PID_COLUMN_NAME = "ProcessID";
  public static final String LOAD_COLUMN_NAME = "Load";
  public static final String INVOKE = "invoke";
  public static final String OK = "ok";
  public static final String FAIL = "fail";
  public static final String INFO = "info";

  /**
   * Functional interface that will be implemented by user of Executor.runTxn. This function will
   * be run inside a readWriteTransaction. An alternative would be to use the Java native
   * Runnable interface, but considering that will create confusion as it is usually associated
   * with Thread, we create one that is unique to the Executor class.
   */
  public interface TransactionFunction {
    void run();
  }

  public Executor(String instanceId, String dbId, int processID) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    DatabaseId databaseId = DatabaseId.of(options.getProjectId(), instanceId, dbId);
    this.client = spanner.getDatabaseClient(databaseId);
    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
    this.processID = processID;

    // create the initial tables for history
    OperationFuture<Database, CreateDatabaseMetadata> op =
            adminClient.createDatabase(instanceId, dbId, Arrays.asList(
                    "CREATE TABLE " + HISTORY_TABLE_NAME + " (\n" +
                            "    " + TIME_COLUMN_NAME + "   TIMESTAMP NOT NULL\n" +
                            "    OPTIONS (allow_commit_timestamp = true),\n" +
                            "    " + OPTYPE_COLUMN_NAME + " STRING(6) NOT NULL,\n" +
                            "    " + LOAD_COLUMN_NAME + "   STRING(MAX) NOT NULL,\n" +
                            "    " + VALUE_COLUMN_NAME + "  ARRAY<STRING(MAX)>,\n" +
                            "    " + PID_COLUMN_NAME + "    INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(" + TIME_COLUMN_NAME + ", " + PID_COLUMN_NAME + ", " +
                            OPTYPE_COLUMN_NAME + ", " + LOAD_COLUMN_NAME + ")",
                    "CREATE TABLE " + TESTING_TABLE_NAME + " (\n" +
                            "    " + KEY_COLUMN_NAME + "   STRING(MAX) NOT NULL,\n" +
                            "    " + VALUE_COLUMN_NAME + " INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(" + KEY_COLUMN_NAME + ")\n"));

    try {
      op.get();
    } catch (ExecutionException e) {
      SpannerException se = (SpannerException) e.getCause();
      // If error code is ALREADY_EXISTS, other executors have created the initial tables already
      if (se.getErrorCode() != ErrorCode.ALREADY_EXISTS) {
        // otherwise throw error
        throw se;
      }
    } catch (InterruptedException e) {
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  /**
   * Given a list of string as keys, return a pair where the first element is the key-value
   * mapping read and the second element is the read timestamp. The staleness and bounded
   * parameters are used to achieve Exact Stale reads and Bounded Stale reads.
   * @param keys
   * @param staleness
   * @param bounded
   * @throws SpannerException
   */
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

  /**
   * Given a function of how to run the transaction, run the transaction.
   * Returns the commit timestamp of the transaction.
   * @param transactionToRun
   */
  public Timestamp runTxn(TransactionFunction transactionToRun) {
    TransactionRunner transactionRunner = client.readWriteTransaction();
    transactionRunner.run(new TransactionRunner.TransactionCallable<Void>() {
      @Nullable
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        // only when a transaction is active will the variable be non-null
        currentActiveTransaction = transaction;
        transactionToRun.run();
        currentActiveTransaction = null; // reset back to null
        return null;
      }
    });
    return transactionRunner.getCommitTimestamp();
  }

  /**
   * Given a key, returns the result of a transactional read. This method must be used only within
   * the user-defined transaction function.
   * @param key
   */
  public long executeTransactionalRead(String key) throws RuntimeException {
    if (currentActiveTransaction == null)
      throw new RuntimeException("This method should be used in a ReadWriteTransaction");

    // Using SQL interface so that all previous writes will be reflected in subsequent reads in
    // the same transaction; this is not the case for Mutation interface
    try (ResultSet resultSet = currentActiveTransaction.executeQuery(
            Statement.of(String.format("SELECT %s FROM %s WHERE %s = \"%s\"", VALUE_COLUMN_NAME,
                    TESTING_TABLE_NAME, KEY_COLUMN_NAME, key)))) {
      resultSet.next();
      return resultSet.getLong(VALUE_COLUMN_NAME);
    }
  }

  /**
   * Given a key and a value, write the key-value pair into the database. See above
   * executeTransactionalRead
   * @param key
   * @param value
   */
  public void executeTransactionalWrite(String key, long value) {
    if (currentActiveTransaction == null)
      throw new RuntimeException("This method should be used in a ReadWriteTransaction");
    currentActiveTransaction.executeUpdate(
            Statement.of(String.format("UPDATE %s SET %s = %s WHERE %s = \"%s\"",
                    TESTING_TABLE_NAME, VALUE_COLUMN_NAME, value, KEY_COLUMN_NAME, key)));
  }

  /**
   * Given a load name, a load value representation and staleness, record the invoke history in
   * the history table. Returns the commit timestamp of this record.
   * @param loadName
   * @param representation
   * @param staleness
   */
  public Timestamp recordInvoke(String loadName, List<String> representation, int staleness) {
    return recordList(loadName, representation, INVOKE, staleness);
  }

  /**
   * Overloaded recordInvoke for non-stale operations / transactions.
   * @param loadName
   * @param representation
   * @return
   */
  public Timestamp recordInvoke(String loadName, List<String> representation) {
    return recordInvoke(loadName, representation, /*staleness=*/0);
  }

  /**
   * Given a load name, a load value representation, a commit timestamp and an invoke timestamp,
   * record the "ok" history and update the timestamp of "invoke" history.
   * @param loadName
   * @param recordRepresentation
   * @param commitTimestamp
   * @param invokeTimestamp
   */
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
                      loadName)), // delete the old invoke record first, since key cannot be updated
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

  /**
   * Record a fail history.
   * @param loadName
   * @param representation
   */
  public void recordFail(String loadName, List<String> representation) {
    recordList(loadName, representation, FAIL, /*staleness=*/0);
  }

  /**
   * Record an info history.
   * @param loadName
   * @param representation
   */
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
        // If this is a stale read, we need to go back and subtract that staleness from the
        // commit timestamp
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

  /**
   * Given a key-vale mapping, insert it into the database. This function is intended to be used
   * in initialization.
   * @param initialKVs
   */
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

  /** For testing; returns the client under the hood. */
  public DatabaseClient getClient() { return client; }
}
