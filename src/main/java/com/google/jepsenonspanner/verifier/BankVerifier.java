package com.google.jepsenonspanner.verifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BankVerifier implements Verifier {
  // Workloads specific to the bank benchmark
  private static Keyword READ = Keyword.newKeyword(BankLoadGenerator.READ_LOAD_NAME);
  private static Keyword TRANSFER = Keyword.newKeyword(BankLoadGenerator.TRANSFER_LOAD_NAME);

  @Override
  public boolean verify(String path, Map<String, Long> state) {
    try {
      FileReader fs = new FileReader(new File(path));
      return verify(fs, state);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Invalid file");
    }
  }

  @VisibleForTesting
  boolean verify(Readable input, Map<String, Long> state) {
    Parseable pbr = Parsers.newParseable(input);
    Parser parser = Parsers.newParser(Parsers.defaultConfiguration());

    // parses the edn file to Java data structure
    List<Map<Keyword, Object>> records = (List<Map<Keyword, Object>>) parser.nextValue(pbr);

    try {
      for (Map<Keyword, Object> record : records) {
        if (record.get(TYPE) == OK) {
          checkOk(record, state);
        } else if (record.get(TYPE) == FAIL) {
          checkFail(record, state);
        }
      }
    } catch (VerifierException e) {
      System.out.println("Invalid operation found at " + e.getMessage());
      return false;
    }

    System.out.println("Valid!");
    return true;
  }

  /**
   * Given an "ok" record and the current state of the database according to previous records,
   * determine if this record is valid. Throws a VerifierException if it is invalid.
   */
  private void checkOk(Map<Keyword, Object> record, Map<String, Long> state)
          throws VerifierException {
    Keyword opName = (Keyword) record.get(OP_NAME);
    List<String> value = (List<String>) record.get(VALUE);
    if (opName.equals(READ)) {
      checkOkRead(value, state);
    } else if (opName.equals(TRANSFER)) {
      checkOkTransfer(value, state);
    }
  }

  /**
   * Checks balances read from each bank account and verifies against the current state of the
   * database. Throws a VerifierException if it is invalid.
   * @param value a list of values read in the format of [["0" 20] ["1" 15]] i.e. read balance of
   *             account "0" = 20, account "1" = 15
   * @param state a map of current state given all previous records
   */
  private void checkOkRead(List<String> value, Map<String, Long> state) throws VerifierException {
    Map<String, Long> currentState = new HashMap<>();
    for (String representation : value) {
      String[] keyValues = representation.split(" ");
      currentState.put(keyValues[0], Long.parseLong(keyValues[1]));
    }
    if (!currentState.equals(state)) {
      throw new VerifierException(READ.getName(), value);
    }
  }

  /**
   * Updates the state of the database given a successful transfer between 2 accounts.
   * @param value a list of values read in the format of [["0" "1" 15]] i.e. transfer 15 from
   *              account "0" to account "1"
   * @param state a map of current state given all previous records
   */
  private void checkOkTransfer(List<String> value, Map<String, Long> state) {
    String[] transferParams = value.get(0).split(" ");
    String fromAcct = transferParams[0];
    String toAcct = transferParams[1];
    long amount = Long.parseLong(transferParams[2]);
    state.put(fromAcct, state.get(fromAcct) - amount);
    state.put(toAcct, state.get(toAcct) + amount);
  }

  /**
   * Given a "fail" record and the current state of the database according to previous records,
   * determine if this record is valid. Throws a VerifierException if it is invalid.
   */
  private void checkFail(Map<Keyword, Object> record, Map<String, Long> state) throws VerifierException {
    Keyword opName = (Keyword) record.get(OP_NAME);
    List<String> value = (List<String>) record.get(VALUE);
    if (opName.equals(TRANSFER)) {
      checkFailTransfer(value, state);
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
   * @param state a map of current state given all previous records
   */
  private void checkFailTransfer(List<String> value, Map<String, Long> state)
          throws VerifierException {
    String[] transferParams = value.get(0).split(" ");
    String fromAcct = transferParams[0];
    long amount = Long.parseLong(transferParams[2]);
    if (state.get(fromAcct) >= amount) {
      // amount should be transferable
      throw new VerifierException(TRANSFER.getName(), value);
    }
  }
}
