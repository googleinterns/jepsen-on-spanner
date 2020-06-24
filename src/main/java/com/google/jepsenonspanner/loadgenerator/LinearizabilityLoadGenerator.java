package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;

import java.util.List;

public class LinearizabilityLoadGenerator extends LoadGenerator {
  private List<String> keys;
  private boolean allowMultiKeys;
  private boolean allowMixedReadsWrites;
  private boolean allowCAS;

  public static class Config extends LoadRatioConfig {
    private int read;
    private int write;
    private int cas;

    public enum LoadType {
      READ,
      WRITE,
      CAS
    }

    public Config(int ... loadRatios) {
      super(loadRatios);
    }

    /**
     * Given a random number by the load generator, return which load to issue
     * @param randNum random number given by generator
     */
    public LoadType categorizeLinearizabilityLoad(int randNum) {
      switch (super.categorize(randNum)) {
        case 0:
          return LoadType.READ;
        case 1:
          return LoadType.WRITE;
        default:
          return LoadType.CAS;
      }
    }
  }

  public LinearizabilityLoadGenerator(int opLimit, List<String> keys, boolean allowMultiKeys,
                                      boolean allowMixedReadsWrites, boolean allowCAS) {
    super(opLimit);
    this.keys = keys;
    this.allowMultiKeys = allowMultiKeys;
    this.allowMixedReadsWrites = allowMixedReadsWrites;
    this.allowCAS = allowCAS;
  }

  @Override
  public Operation nextOperation() {
    return null;
  }
}
