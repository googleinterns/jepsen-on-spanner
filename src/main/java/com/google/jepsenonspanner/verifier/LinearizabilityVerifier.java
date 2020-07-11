package com.google.jepsenonspanner.verifier;

import java.util.Map;

/**
 * A wrapper class to run both Knossos and External Visibility Verifier
 */
public class LinearizabilityVerifier implements Verifier {
  @Override
  public boolean verify(Map<String, Long> initialState, String... filePath) {
    return new KnossosVerifier().verify(initialState, filePath[0]) && new ExternalVisibilityVerifier().verify(
            initialState, filePath[1]);
  }
}
