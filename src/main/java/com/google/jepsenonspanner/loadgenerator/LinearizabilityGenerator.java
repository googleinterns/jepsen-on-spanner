package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;

import java.util.List;

public class LinearizabilityGenerator extends LoadGenerator {
  private List<String> keys;

  public LinearizabilityGenerator(int opLimit, List<String> keys) {
    super(opLimit);
    this.keys = keys;
  }

  @Override
  public Operation nextOperation() {

    return null;
  }
}
