package com.google.jepsenonspanner.verifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Record;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import us.bpsm.edn.Keyword;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;
import static com.google.jepsenonspanner.client.Record.FAIL_STR;

/**
 * A BankVerifier is part of the Bank Benchmark and checks that the balances read are consistent
 * with the results of previous transfers. The sum of all accounts should be consistent.
 * Specifically, the verifiers checks:
 * - Whether a read reflects all previous successful transactions
 * - Whether a successful transaction will result in negative balances
 * - Whether a failed transaction should have failed due to insufficient balance
 * - Whether a read reflects a failed transaction (it should not)
 */
public class BankVerifier implements Verifier {
  // Workloads specific to the bank benchmark
  private static Keyword READ = Keyword.newKeyword(BankLoadGenerator.READ_LOAD_NAME.substring(1));
  private static Keyword TRANSFER =
          Keyword.newKeyword(BankLoadGenerator.TRANSFER_LOAD_NAME.substring(1));

  // Keeps track of all possible states an invoked operation may observe. Key is a string that
  // uniquely identifies an operation that is invoked but has not been completed. Value is the
  // list of all possible states the operation may observe.
  // TODO: Think about ways to improve the memory complexity of this algorithm
  private HashMap<String, LinkedList<HashMap<String, Long>>> concurrentTxnStates = new HashMap<>();

  @Override
  public boolean verify(Map<String, Long> state, String... filePath) {
    try {
      FileReader fs = new FileReader(new File(filePath[0]));
      return verify(fs, state);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(INVALID_FILE);
    }
  }

  @VisibleForTesting
  boolean verify(Readable input, Map<String, Long> initialState) {
    HashMap<String, Long> state = new HashMap<>(initialState);
    List<Record> records = Verifier.parseRecords(input);

    try {
      for (Record record : records) {
        String currRecordId = recordUniqueId(record);
        if (record.getType().equals(INVOKE_STR)) {
          // Add the initial possible state, which is a reference to the currently latest state.
          concurrentTxnStates.put(currRecordId, new LinkedList<>(Collections.singleton(state)));
        } else {
          if (record.getType().equals(OK_STR)) {
            checkOk(record);
          } else if (record.getType().equals(FAIL_STR)) {
            checkFail(record);
          }
          // This operation is considered complete, and the map should stop tracking its states
          concurrentTxnStates.remove(currRecordId);
        }
      }
    } catch (VerifierException e) {
      System.out.println("Current possible state maps is: " + concurrentTxnStates.toString());
      System.out.println(INVALID_INFO + e.getMessage());
      return false;
    }

    System.out.println(VALID_INFO);
    return true;
  }

  /**
   * Convert a record to a uniquely identified string.
   * e.g. a transfer of 10 from account 0 to 1 by thread 1 is expressed as "1 transfer [0 1 10]"
   */
  private String recordUniqueId(Record record) {
    long processId = record.getpID();
    Keyword opName = record.getLoad();
    List<List<Object>> value = record.getRawRepresentation();
    if (opName.equals(READ)) {
      // convert the read representation to only include keys
      value =
              value.stream().map(s -> Collections.singletonList(s.get(0))).collect(Collectors.toList());
    }
    return String.format("%d %s %s", processId, opName.getName(), value.toString());
  }

  /**
   * Given an "ok" record and the current state of the database according to previous records,
   * determine if this record is valid. Throws a VerifierException if it is invalid.
   */
  private void checkOk(Record record) throws VerifierException {
    Keyword opName = record.getLoad();
    List<List<Object>> value = record.getRawRepresentation();
    String currRecordId = recordUniqueId(record);
    if (opName.equals(READ)) {
      checkOkRead(value, recordUniqueId(record));
    } else if (opName.equals(TRANSFER)) {
      checkOkTransfer(value, currRecordId);
    } else {
      // Invalid operation name
      throw new VerifierException(opName.getName(), value);
    }
  }

  /**
   * Checks balances read from each bank account and verifies against the current state of the
   * database. Throws a VerifierException if it is invalid.
   * @param value a list of values read in the format of [["0" 20] ["1" 15]] i.e. read balance of
   *             account "0" = 20, account "1" = 15
   * @param recordId a String that uniquely identifies the current operation
   */
  private void checkOkRead(List<List<Object>> value, String recordId) throws VerifierException {
    List<HashMap<String, Long>> possibleStates = concurrentTxnStates.get(recordId);
    if (possibleStates == null) {
      throw new VerifierException(READ.getName(), value);
    }
    Map<String, Long> currentState = new HashMap<>();
    for (List<Object> representation : value) {
      String key = (String) representation.get(0);
      long balance = (long) representation.get(1);
      if (balance < 0) {
        throw new VerifierException(READ.getName(), value);
      }
      currentState.put(key, balance);
    }

    // The previous record must be a "invoke read", thus there should only be one possible state.
    // This possible state must be the same as the parsed current state.
    if (possibleStates.size() != 1 || !possibleStates.contains(currentState)) {
      throw new VerifierException(READ.getName(), value);
    }
  }

  /**
   * Updates the state of the database given a successful transfer between 2 accounts.
   * @param value a list of values read in the format of [["0" "1" 15]] i.e. transfer 15 from
   *              account "0" to account "1"
   * @param recordId a String that uniquely identifies the current operation
   */
  private void checkOkTransfer(List<List<Object>> value, String recordId) throws VerifierException {
    List<Object> transferParams = value.get(0);
    String fromAcct = (String) transferParams.get(0);
    String toAcct = (String) transferParams.get(1);
    long amount = (long) transferParams.get(2);
    LinkedList<HashMap<String, Long>> possibleStatesForThisRecord =
            concurrentTxnStates.get(recordId);
    if (possibleStatesForThisRecord.size() != 1) {
      // An ok transfer must only have one possible previous state, since the record proceeding
      // this must be a "invoke transfer"
      throw new VerifierException(TRANSFER.getName(), value);
    }
    // Update the latest state, or the only possible state. Since the first element in each list
    // of possible states is the same reference to this latest state, we do not need to modify
    // them again
    HashMap<String, Long> latestState = possibleStatesForThisRecord.peekFirst();
    // Previous state will only store keys that have changed values
    HashMap<String, Long> prevState = new HashMap<>();
    long fromAcctBalance = latestState.get(fromAcct);
    if (fromAcctBalance < amount) {
      throw new VerifierException(TRANSFER.getName(), value);
    }
    long toAcctBalance = latestState.get(toAcct);
    latestState.put(fromAcct, fromAcctBalance - amount);
    latestState.put(toAcct, toAcctBalance + amount);
    prevState.put(fromAcct, fromAcctBalance);
    prevState.put(toAcct, toAcctBalance);

    // Add the previous state to all possible states in the map
    for (LinkedList<HashMap<String, Long>> possibleStates : concurrentTxnStates.values()) {
      // The possible states now look like latestState@t10, delta@t9, ..., delta@t2, delta@t1
      possibleStates.add(1, prevState);
    }
  }

  /**
   * Given a "fail" record and the current state of the database according to previous records,
   * determine if this record is valid. Throws a VerifierException if it is invalid.
   */
  private void checkFail(Record record) throws VerifierException {
    Keyword opName = record.getLoad();
    List<List<Object>> value = record.getRawRepresentation();
    if (opName.equals(TRANSFER)) {
      checkFailTransfer(value, recordUniqueId(record));
    } else {
      // a read operation should not produce a fail record
      throw new VerifierException(opName.getName(), value);
    }
  }

  /**
   * Checks if a failed transfer between 2 accounts should actually fail according to current
   * state of the database.
   * @param value a list of values read in the format of [["0" "1" 15]] i.e. transfer 15 from
   *              account "0" to account "1"
   * @param recordId a String that uniquely identifies the current operation
   */
  private void checkFailTransfer(List<List<Object>> value, String recordId) throws VerifierException {
    List<Object> transferParams = value.get(0);
    String fromAcct = (String) transferParams.get(0);
    long amount = (long) transferParams.get(2);
    List<HashMap<String, Long>> possibleStates = concurrentTxnStates.get(recordId);
    if (possibleStates == null) {
      throw new VerifierException(TRANSFER.getName(), value);
    }

    boolean valid = false;
    for (HashMap<String, Long> state : possibleStates) {
      if (state.containsKey(fromAcct) && state.get(fromAcct) < amount) {
        // If in any of the possible states, there exists one value that would fail the transfer,
        // the history is valid
        valid = true;
      }
    }
    if (!valid) {
      throw new VerifierException(TRANSFER.getName(), value);
    }
  }
}
