package com.google.jepsenonspanner.verifier;

import java.util.List;

/**
 * A VerifierException should be thrown whenever an invalid record is detected. The top level
 * function will catch it and expose the invalid record to the user. Created to simplify error
 * parsing in multi-level function calls.
 */
public class VerifierException extends Exception {
  private String opName;
  private List<String> representation;

  public String getOpName() {
    return opName;
  }

  public List<String> getRepresentation() {
    return representation;
  }

  public VerifierException(String opName, List<String> representation) {
    super(opName + " " + String.valueOf(representation));
    this.opName = opName;
    this.representation = representation;
  }
}
