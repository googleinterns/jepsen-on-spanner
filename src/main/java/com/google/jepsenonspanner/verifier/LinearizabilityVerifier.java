package com.google.jepsenonspanner.verifier;

import java.util.Map;

/**
 * A wrapper class to run both Knossos and External Visibility Verifier
 */
public class LinearizabilityVerifier implements Verifier {
  @Override
  public boolean verify(Map<String, Long> initialState, String... filePath) {
    if (filePath.length != 2) {
      throw new RuntimeException("Linearizability Verifier only accepts 2 files");
    }
    return new KnossosVerifier().verify(initialState, filePath[0]) && new ExternalConsistencyVerifier().verify(
            initialState, filePath[1]);
  }
}
