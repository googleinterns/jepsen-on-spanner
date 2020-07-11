package com.google.jepsenonspanner.verifier;

import java.util.Map;

public class LinearizabilityVerifier implements Verifier {
  @Override
  public boolean verify(Map<String, Long> initialState, String... filePath) {
    return new BankVerifier().verify(initialState, filePath[0]) && new ExternalVisibilityVerifier().verify(
            initialState, filePath[1]);
  }
}
