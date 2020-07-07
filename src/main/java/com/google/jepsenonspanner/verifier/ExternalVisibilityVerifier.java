package com.google.jepsenonspanner.verifier;

import java.util.Map;

public class ExternalVisibilityVerifier implements Verifier {
  @Override
  public boolean verify(String filePath, Map<String, Long> initialState) {
    return false;
  }
}
