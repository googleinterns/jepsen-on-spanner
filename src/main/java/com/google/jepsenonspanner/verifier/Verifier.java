package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.client.Executor;
import us.bpsm.edn.Keyword;

import java.util.Map;

import static com.google.jepsenonspanner.constants.BenchmarkTypes.BANK_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.LINEARIZABILITY_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.INVALID_TYPE_MSG;

/**
 * Each type of workload must implement this interface to check against a history file
 */
public interface Verifier {
  // Pre-defined field names for the history file
  static final Keyword PROCESS = Keyword.newKeyword("process");
  static final Keyword VALUE = Keyword.newKeyword("value");
  static final Keyword TYPE = Keyword.newKeyword("type");
  static final Keyword OP_NAME = Keyword.newKeyword("f");
  static final Keyword INVOKE = Keyword.newKeyword(Executor.INVOKE_STR);
  static final Keyword OK = Keyword.newKeyword(Executor.OK_STR);
  static final Keyword FAIL = Keyword.newKeyword(Executor.FAIL_STR);
  static final Keyword INFO = Keyword.newKeyword(Executor.INFO_STR);

  final String VALID_INFO = "Valid!";
  final String INVALID_INFO = "Invalid operation found at ";

  /**
   * Given the path to a history file and a map of initial state of the database, checks if the
   * history file reflects a consistency error the benchmark is looking for. Returns false if
   * there is an error, and prints the location of the error to System.out.
   */
  boolean verify(String filePath, Map<String, Long> initialState);

  static Verifier createVerifier(String benchmarkType) {
    switch (benchmarkType) {
      case BANK_TYPE:
        return new BankVerifier();
      case LINEARIZABILITY_TYPE:
        return new KnossosVerifier();
      default:
        throw new RuntimeException(INVALID_TYPE_MSG);
    }
  }
}