package com.google.jepsenonspanner.loadgenerator;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Super class to adjust how ratio between different load type for each generator. User should
 * define their own class that extends this class, but also implements concrete load types as
 * enum classes. The enum class should take care of converting the categorize results to human
 * understandable load name.
 */
public abstract class LoadRatioConfig {
  private int[] ratios;
  private int distributionSum;

  public LoadRatioConfig(int... ratios) {
    this.ratios = ratios;

    // Sum up all distribution and check that it is greater than zero
    this.distributionSum = IntStream.of(ratios).sum();
    if (distributionSum <= 0) {
      throw new RuntimeException("Invalid config");
    }
  }

  public int categorize(int randNum) {
    int currDistributionSum = distributionSum;
    randNum %= distributionSum;
    for (int i = ratios.length - 1; i >= 0; i--) {
      currDistributionSum -= ratios[i];
      if (randNum >= currDistributionSum) {
        return i;
      }
    }
    // This should not happen
    throw new RuntimeException("Error in categorizing randNum " + randNum);
  }
}
