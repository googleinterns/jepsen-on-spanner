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
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.operation.OperationException;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import org.apache.commons.lang3.tuple.Pair;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.printer.Printers;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
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

  // maintain connection with the Spanner instance
  private DatabaseClient client;

  // each executor will be assigned a unique ID
  private int processID;

  public static final String TESTING_TABLE_NAME = "Testing";
  public static final String HISTORY_TABLE_NAME = "History";
  public static final String KEY_COLUMN_NAME = "Key";
  public static final String VALUE_COLUMN_NAME = "Value";
  public static final String RECORD_TYPE_COLUMN_NAME = "OpType";
  public static final String TIME_COLUMN_NAME = "Time";
  public static final String PID_COLUMN_NAME = "ProcessID";
  public static final String OP_NAME_COLUMN_NAME = "Load";
  public static final String INVOKE = "invoke";
  public static final String OK = "ok";
  public static final String FAIL = "fail";
  public static final String INFO = "info";
  public static final String RECORD_FILENAME = "history.edn";
  public static final String RECORDER_ERROR = "RECORDER ERROR";

  /**
   * Functional interface that will be implemented by user of Executor.runTxn. This function will
   * be run inside a readWriteTransaction. An alternative would be to use the Java native
   * Runnable interface, but considering that will create confusion as it is usually associated
   * with Thread, we create one that is unique to the Executor class.
   *
   * Naming distinction:
   * - Record type refers to the type of record written into history table i.e. "invoke" or "ok"
   * - Op refers to the load generated and defined by the generator
   * - Spanner Action refers to the basic reads and writes that an Op is consisted of
   */
  public interface TransactionFunction {
    void run(TransactionContext transaction);
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
                            "    " + RECORD_TYPE_COLUMN_NAME + " STRING(6) NOT NULL,\n" +
                            "    " + OP_NAME_COLUMN_NAME + "   STRING(MAX) NOT NULL,\n" +
                            "    " + VALUE_COLUMN_NAME + "  ARRAY<STRING(MAX)>,\n" +
                            "    " + PID_COLUMN_NAME + "    INT64 NOT NULL,\n" +
                            ") PRIMARY KEY(" + TIME_COLUMN_NAME + ", " + PID_COLUMN_NAME + ", " +
                            RECORD_TYPE_COLUMN_NAME + ", " + OP_NAME_COLUMN_NAME + ")",
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
   * Given a list of string as keys, returns a pair where the first element is the key-value
   * mapping read and the second element is the read timestamp. The staleness and bounded
   * parameters are used to achieve Exact Stale reads and Bounded Stale reads. If there is a
   * non-existent key, throw a OperationException.
   */
  public Pair<HashMap<String, Long>, Timestamp> readKeys(List<String> keys, int staleness,
                                                   boolean bounded) throws OperationException {
    HashMap<String, Long> result = new HashMap<>();
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String key : keys) {
      keySetBuilder.addKey(Key.of(key));
    }

    ReadOnlyTransaction txn;
    if (staleness == 0) {
      txn = client.singleUseReadOnlyTransaction();
    } else if (bounded) {
      txn = client.singleUseReadOnlyTransaction(TimestampBound.ofMaxStaleness(staleness,
              TimeUnit.MILLISECONDS));
    } else {
      txn = client.singleUseReadOnlyTransaction(TimestampBound.ofExactStaleness(staleness,
              TimeUnit.MILLISECONDS));
    }
    try (ResultSet resultSet = txn.read(TESTING_TABLE_NAME, keySetBuilder.build(),
            Arrays.asList(KEY_COLUMN_NAME, VALUE_COLUMN_NAME))) {
      while (resultSet.next()) {
        result.put(resultSet.getString(KEY_COLUMN_NAME), resultSet.getLong(VALUE_COLUMN_NAME));
      }
      if (result.size() != keys.size()) {
        throw new OperationException(String.format("Non-existent key found in read of %s", keys));
      }
      System.out.println(result.size());
    }
    return Pair.of(result, txn.getReadTimestamp());
  }

  /**
   * Runs the given transactionToRun within a transaction.
   * Returns the commit timestamp of the transaction.
   */
  public Timestamp runTxn(TransactionFunction transactionToRun) {
    TransactionRunner transactionRunner = client.readWriteTransaction();
    transactionRunner.run(new TransactionRunner.TransactionCallable<Void>() {
      @Nullable
      @Override
      public Void run(TransactionContext transaction) throws Exception {
        transactionToRun.run(transaction);
        return null;
      }
    });
    return transactionRunner.getCommitTimestamp();
  }

  /**
   * Given a key, returns the result of a transactional read. This method must be used only within
   * the user-defined transaction function. If there is a non-existent key, throw a
   * OperationException.
   */
  public long executeTransactionalRead(String key, TransactionContext transaction) throws OperationException {
    // Using SQL interface so that all previous writes will be reflected in subsequent reads in
    // the same transaction; this is not the case for Mutation interface
    try (ResultSet resultSet = transaction.executeQuery(
            Statement.of(String.format("SELECT %s FROM %s WHERE %s = \"%s\"", VALUE_COLUMN_NAME,
                    TESTING_TABLE_NAME, KEY_COLUMN_NAME, key)))) {
      if (!resultSet.next()) {
        throw new OperationException(String.format("Key %s not found on transactional read", key));
      }
      return resultSet.getLong(VALUE_COLUMN_NAME);
    }
  }

  /**
   * Given a key and a value, write the key-value pair into the database. See above
   * executeTransactionalRead. If there is a non-existent key, throw a OperationException.
   */
  public void executeTransactionalWrite(String key, long value, TransactionContext transaction) throws OperationException {
    long rowsModified = transaction.executeUpdate(
            Statement.of(String.format("UPDATE %s SET %s = %s WHERE %s = \"%s\"",
                    TESTING_TABLE_NAME, VALUE_COLUMN_NAME, value, KEY_COLUMN_NAME, key)));
    if (rowsModified != 1) {
      throw new OperationException(String.format("Key %s not found on transactional write", key));
    }
  }

  /**
   * Records an "invoke" history [opName, representation, staleness] into the history table. If
   * the record is stale, return its stale read timestamp; otherwise returns the commit timestamp
   * of this record.
   */
  public Timestamp recordInvoke(String opName, List<String> representation, int staleness) {
    return writeRecord(opName, representation, INVOKE, staleness);
  }

  /**
   * Overloaded recordInvoke for non-stale operations / transactions.
   * Returns the commit timestamp of this record.
   */
  public Timestamp recordInvoke(String opName, List<String> representation) {
    return recordInvoke(opName, representation, /*staleness=*/0);
  }

  /**
   * Given a load name, a load value representation, a commit timestamp and an invoke timestamp,
   * record the "ok" history and update the timestamp of "invoke" history.
   */
  public void recordComplete(String opName, List<String> recordRepresentation,
                             Timestamp commitTimestamp, Timestamp invokeTimestamp) {
    try {
      client.write(Arrays.asList(
              Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                      .set(TIME_COLUMN_NAME).to(commitTimestamp)
                      .set(RECORD_TYPE_COLUMN_NAME).to(OK)
                      .set(OP_NAME_COLUMN_NAME).to(opName)
                      .set(VALUE_COLUMN_NAME).toStringArray(recordRepresentation)
                      .set(PID_COLUMN_NAME).to(processID).build(),
              Mutation.delete(HISTORY_TABLE_NAME, Key.of(invokeTimestamp, processID, INVOKE,
                      opName)), // delete the old invoke record first, since key cannot be updated
              Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                      .set(TIME_COLUMN_NAME).to(commitTimestamp)
                      .set(RECORD_TYPE_COLUMN_NAME).to(INVOKE)
                      .set(OP_NAME_COLUMN_NAME).to(opName)
                      .set(VALUE_COLUMN_NAME).toStringArray(recordRepresentation)
                      .set(PID_COLUMN_NAME).to(processID).build()));
    } catch (SpannerException e) {
      throw new RuntimeException(RECORDER_ERROR);
    }
  }

  /**
   * Records a fail history.
   */
  public void recordFail(String opName, List<String> representation) {
    writeRecord(opName, representation, FAIL, /*staleness=*/0);
  }

  /**
   * Records an info history.
   */
  public void recordInfo(String opName, List<String> representation) {
    writeRecord(opName, representation, INFO, /*staleness=*/0);
  }


  /**
   * Helper function that inserts a history [opName, representation, staleness] into the
   * history table with the given recordType (can be one of "invoke", "ok", "fail" or "info").
   * Optional staleness will subtract staleness amount of milliseconds from the commit timestamp.
   */
  private Timestamp writeRecord(String opName, List<String> representation, String recordType,
                                int staleness) throws RuntimeException {
    try {
       Timestamp commitTimestamp =
               client.write(Collections.singletonList(Mutation.newInsertBuilder(HISTORY_TABLE_NAME)
                  .set(TIME_COLUMN_NAME).to(Value.COMMIT_TIMESTAMP)
                  .set(RECORD_TYPE_COLUMN_NAME).to(recordType)
                  .set(OP_NAME_COLUMN_NAME).to(opName)
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
                        .set(RECORD_TYPE_COLUMN_NAME).to(recordType)
                        .set(OP_NAME_COLUMN_NAME).to(opName)
                        .set(VALUE_COLUMN_NAME).toStringArray(representation)
                        .set(PID_COLUMN_NAME).to(processID).build(),
                Mutation.delete(HISTORY_TABLE_NAME,
                        Key.of(commitTimestamp, processID, recordType, opName))));
        return staleTimestamp;
      }
      return commitTimestamp;
    } catch (SpannerException e) {
      System.out.println(staleness);
      System.out.println(e.getCause());
      e.printStackTrace();
      throw new RuntimeException(RECORDER_ERROR);
    }
  }

  /**
   * Given a key-value mapping, insert it into the database. This function is intended to be used
   * in initialization.
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
      throw new RuntimeException(RECORDER_ERROR);
    }
  }

  /**
   * Converts a row of History table into a Map that will be written into the edn file.
   */
  private Map<Keyword, Object> convertToMap(Struct row) {
    Map<Keyword, Object> record = new HashMap<>();
    record.put(Keyword.newKeyword("type"), Keyword.newKeyword(row.getString(RECORD_TYPE_COLUMN_NAME)));
    record.put(Keyword.newKeyword("f"), Keyword.newKeyword(row.getString(OP_NAME_COLUMN_NAME)));
    List<String> representation = row.getStringList(VALUE_COLUMN_NAME);
    String repr = Printers.printString(Printers.defaultPrinterProtocol(), representation);
    record.put(Keyword.newKeyword("value"), representation);
    record.put(Keyword.newKeyword("process"), processID);
    return record;
  }

  /**
   * Extracts all history records and save it on a local edn file.
   */
  public void extractHistory() {
    try (ResultSet resultSet = client.singleUse().read(HISTORY_TABLE_NAME, KeySet.all(),
            Arrays.asList(RECORD_TYPE_COLUMN_NAME, OP_NAME_COLUMN_NAME, VALUE_COLUMN_NAME,
                    PID_COLUMN_NAME));
         FileWriter recordWriter = new FileWriter(RECORD_FILENAME)) {
      List<Map<Keyword, Object>> records = new ArrayList<>();
      while (resultSet.next()) {
        Map<Keyword, Object> record = convertToMap(resultSet.getCurrentRowAsStruct());
        records.add(record);
      }
      recordWriter.write(Printers.printString(Printers.prettyPrinterProtocol(), records));
    } catch (IOException e) {
      throw new RuntimeException(RECORDER_ERROR);
    }
  }

  /** For testing; returns the client under the hood. */
  @VisibleForTesting
  public DatabaseClient getClient() { return client; }
}