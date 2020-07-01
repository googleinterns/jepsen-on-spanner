package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;

import java.util.Random;

import static com.google.jepsenonspanner.constants.BenchmarkTypes.BANK_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.LINEARIZABILITY_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.INVALID_TYPE_MSG;

/**
 * A load generator generates testing load for the client to interact with the Spanner instance.
 * Each generator must implement this interface
 *
 */
public abstract class LoadGenerator {

  protected int opLimit = 0;
  protected Random rand;
  protected int seed;

  public LoadGenerator(int opLimit) {
    this(opLimit, new Random().nextInt());
  }

  public LoadGenerator(int opLimit, int seed) {
    this.opLimit = opLimit;
    this.seed = seed;
    this.rand = new Random(seed);
  }

  /**
   * Returns the next operations for the client wrapper to execute
   */
  public abstract Operation nextOperation();

  /**
   * Returns if the generator has more loads
   */
  public boolean hasLoad() {
    return opLimit > 0;
  }

  public static LoadGenerator createGenerator(String benchmarkType, String filePath) {
    switch (benchmarkType) {
      case BANK_TYPE:
        return BankLoadGenerator.createGeneratorFromConfig(filePath);
      case LINEARIZABILITY_TYPE:
        return LinearizabilityLoadGenerator.createGeneratorFromConfig(filePath);
      default:
        throw new RuntimeException(INVALID_TYPE_MSG);
    }
  }
}
