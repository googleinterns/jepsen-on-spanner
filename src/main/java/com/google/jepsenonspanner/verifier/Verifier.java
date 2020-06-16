package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.client.Executor;
import us.bpsm.edn.Keyword;

import java.util.Map;

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


  /**
   * Given the path to a history file and a map of initial state of the database, check if the
   * history file reflects a consistency error the benchmark is looking for. Returns false if
   * there is an error, and prints the location of the error to System.out.
   */
  public boolean verify(String path, Map<String, Long> initialState);
}
