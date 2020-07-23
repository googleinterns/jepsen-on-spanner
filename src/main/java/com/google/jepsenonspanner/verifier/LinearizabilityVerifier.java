package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.verifier.knossos.LinearVerifier;

import java.time.Duration;
import java.time.Instant;
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

    return timedVerify(new KnossosVerifier(), /*filePathIdx=*/0, initialState, filePath) &&
            timedVerify(new ExternalConsistencyVerifier(), /*filePathIdx=*/1, initialState,
                    filePath) &&
            timedVerify(new LinearVerifier(), /*filePathIdx=*/0, initialState, filePath);
  }

  private boolean timedVerify(Verifier verifier, int filePathIdx, Map<String, Long> initialState, String... filePath) {
    Instant start = Instant.now();
    boolean pass = verifier.verify(initialState, filePath[filePathIdx]);
    Instant end = Instant.now();
    Duration duration = Duration.between(start, end);
    System.out.printf("%s took %d milliseconds\n", verifier.getClass().getTypeName(),
            duration.toMillis());
    return pass;
  }
}
