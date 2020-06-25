package com.google.jepsenonspanner.loadgenerator;

import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import com.google.jepsenonspanner.operation.ReadWriteTransaction;

import java.util.List;
import java.util.Random;

public class LinearizabilityLoadGenerator extends LoadGenerator {
  private List<String> keys;
  private boolean allowMultiKeys;
  private boolean allowMixedReadsWrites;
  private boolean allowCAS;
  private Random rand;
  private Config config;

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
                                      boolean allowMixedReadsWrites, boolean allowCAS,
                                      int ... loadRatios) {
    super(opLimit);
    this.keys = keys;
    this.allowMultiKeys = allowMultiKeys;
    this.allowMixedReadsWrites = allowMixedReadsWrites;
    this.allowCAS = allowCAS;
    this.config = new Config(loadRatios);
  }

  @Override
  public Operation nextOperation() {

    // check if reached limit
    if (opLimit <= 0) {
      throw new RuntimeException("Bank generator has reached limit");
    }

    opLimit--;
    int nextOp = rand.nextInt();
    switch (config.categorizeLinearizabilityLoad(nextOp)) {
      case READ:
        return read();
      case WRITE:
        return write();
      default:
        return cas();
    }
  }

  public ReadTransaction read() {
    return null;
  }

  public ReadWriteTransaction write() {
    return null;
  }

  public ReadWriteTransaction cas() {
    return null;
  }
}
