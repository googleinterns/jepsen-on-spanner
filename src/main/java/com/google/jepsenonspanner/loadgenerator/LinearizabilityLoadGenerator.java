package com.google.jepsenonspanner.loadgenerator;

import com.google.gson.Gson;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import com.google.jepsenonspanner.operation.ReadWriteTransaction;
import com.google.jepsenonspanner.operation.TransactionalAction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implements the Linearizability Load Generator. Generates two kinds of loads: transaction and
 * compare-and-set (CAS). Transaction can be configured to generate reads and writes across
 * multiple keys.
 * TODO: Implement CAS, but Knossos does not support multi-CAS-register
 */
public class LinearizabilityLoadGenerator extends LoadGenerator {
  private String[] keys;
  private int valueLimit;
  private boolean allowMultiKeys;
  private Config config;

  private static final String OP_LIMIT = "opLimit";
  private static final String VALUE_LIMIT = "valueLimit";
  private static final String KEYS = "keys";
  private static final String ALLOW_MULTI_KEY = "multiKey";
  private static final String ALLOW_MIXED_READ_WRITE = "allowMixedReadsWrites";
  private static final String OP_RATIO = "opRatio";
  private static final String ERR_MESSAGE = "Error parsing config file ";
  private static final String TXN_LOAD_NAME = "txn";
  private static final String READ_OP_NAME = ":read";
  private static final String WRITE_OP_NAME = ":write";

  public static class Config extends LoadRatioConfig {
    public enum LoadType {
      READ_ONLY,
      WRITE_ONLY,
      TRANSACTION,
      CAS
    }

    Config(int... loadRatios) {
      super(loadRatios);
      // 4 types of operations
      if (loadRatios.length != 4) {
        throw new RuntimeException("Invalid ratio length");
      }
    }

    /**
     * Given a random number by the load generator, return which load to issue
     * @param randNum random number given by generator
     */
    LoadType categorizeLinearizabilityLoad(int randNum) {
      switch (super.categorize(randNum)) {
        case 0:
          return LoadType.READ_ONLY;
        case 1:
          return LoadType.WRITE_ONLY;
        case 2:
          return LoadType.TRANSACTION;
        default:
          return LoadType.CAS;
      }
    }
  }

  public LinearizabilityLoadGenerator(int opLimit, int valueLimit, String[] keys,
                                      boolean allowMultiKeys, int ... opRatios) {
    this(new Random().nextInt(), opLimit, valueLimit, keys, allowMultiKeys, opRatios);
  }

  /**
   * Default constructor.
   * @param seed random seed the underlying Random object takes
   * @param opLimit number of operations to issue on this worker
   * @param valueLimit max value that can be written into each key
   * @param keys an array of keys the database has
   * @param allowMultiKeys if each operation is on multiple keys
   * @param opRatios ratios between each operation, should have size of 4
   */
  public LinearizabilityLoadGenerator(int seed, int opLimit, int valueLimit, String[] keys,
                                      boolean allowMultiKeys, int ... opRatios) {
    super(opLimit, seed);
    this.valueLimit = valueLimit;
    this.keys = keys;
    this.allowMultiKeys = allowMultiKeys;
    this.config = new Config(opRatios);
  }

  public static LinearizabilityLoadGenerator createGeneratorFromConfig(String configPath) {
    Gson gson = new Gson();
    try {
      HashMap<String, String> config = gson.fromJson(new FileReader(new File(configPath)),
              HashMap.class);
      int opLimit = Integer.parseInt(config.get(OP_LIMIT));
      int valueLimit = Integer.parseInt(config.get(VALUE_LIMIT));
      boolean allowMultiKeys = Boolean.parseBoolean(config.get(ALLOW_MULTI_KEY));
      boolean allowMixedReadsWrites = Boolean.parseBoolean(config.get(ALLOW_MIXED_READ_WRITE));
      String[] keys = config.get(KEYS).split(" ");
      String[] opRatioString = config.get(OP_RATIO).split(" ");
      int[] opRatios = Arrays.stream(opRatioString).mapToInt(Integer::parseInt).toArray();
      return new LinearizabilityLoadGenerator(opLimit, valueLimit, keys, allowMultiKeys, opRatios);
    } catch (FileNotFoundException | ClassCastException e) {
      e.printStackTrace();
      throw new RuntimeException(ERR_MESSAGE + configPath);
    }
  }

  @Override
  public Operation nextOperation() {

    // check if reached limit
    if (opLimit <= 0) {
      throw new RuntimeException("Linearizability generator has reached limit");
    }

    opLimit--;
    int nextOp = rand.nextInt();
    switch (config.categorizeLinearizabilityLoad(nextOp)) {
      case READ_ONLY:
        return readOnly();
      case WRITE_ONLY:
        return writeOnly();
      case TRANSACTION:
        return transaction();
      default:
        return cas();
    }
  }

  /**
   * Returns a list of randomly selected keys to generate operation on
   */
  private List<String> selectKeys() {
    int numKeys = 1;
    if (allowMultiKeys) {
      numKeys = rand.nextInt(keys.length) + 1;
    }
    IntStream selectedKeyIdx = rand.ints(0, keys.length).distinct().limit(numKeys);
    return selectedKeyIdx.mapToObj(idx -> keys[idx]).collect(Collectors.toList());
  }

  private ReadTransaction readOnly() {
    List<String> selectedKeys = selectKeys();
    List<String> representation = selectedKeys.stream().map(key -> String.format("%s %s nil",
            READ_OP_NAME, key)).collect(Collectors.toList());
    return ReadTransaction.createStrongRead(TXN_LOAD_NAME, selectedKeys, representation);
  }

  private ReadWriteTransaction writeOnly() {
    List<String> selectedKeys = selectKeys();
    // Generate random values on writes
    List<TransactionalAction> writes =
            selectedKeys.stream().map(key -> TransactionalAction.createTransactionalWrite(key,
                    rand.nextInt(valueLimit) + 1)).collect(Collectors.toList());
    // Generate the string representations
    List<String> representation = writes.stream().map(action -> String.format("%s %s %d",
            WRITE_OP_NAME, action.getKey(), action.getValue())).collect(Collectors.toList());
    return new ReadWriteTransaction(TXN_LOAD_NAME, representation, writes);
  }

  private ReadWriteTransaction transaction() {
    List<String> selectedKeys = selectKeys();
    List<TransactionalAction> txns = new ArrayList<>();
    List<String> representation = new ArrayList<>();
    for (String key : selectedKeys) {
      // A random boolean value to select between reads or writes
      boolean readWriteSelect = rand.nextBoolean();
      if (readWriteSelect) {
        txns.add(TransactionalAction.createTransactionalRead(key));
        representation.add(String.format("%s %s nil", READ_OP_NAME, key));
      } else {
        int valueToWrite = rand.nextInt(valueLimit) + 1;
        txns.add(TransactionalAction.createTransactionalWrite(key, valueToWrite));
        representation.add(String.format("%s %s %d", WRITE_OP_NAME, key, valueToWrite));
      }
    }
    return new ReadWriteTransaction(TXN_LOAD_NAME, representation, txns);
  }

  // TODO: implement
  private ReadWriteTransaction cas() {
    return null;
  }
}
