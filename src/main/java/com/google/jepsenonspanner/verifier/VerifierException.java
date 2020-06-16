package com.google.jepsenonspanner.verifier;

import java.util.List;

public class VerifierException extends Exception {
  private String opName;
  private List<String> representation;

  public String getOpName() {
    return opName;
  }

  public List<String> getRepresentation() {
    return representation;
  }

  VerifierException(String opName, List<String> representation) {
    super(opName + " " + String.valueOf(representation));
    this.opName = opName;
    this.representation = representation;
  }
}
