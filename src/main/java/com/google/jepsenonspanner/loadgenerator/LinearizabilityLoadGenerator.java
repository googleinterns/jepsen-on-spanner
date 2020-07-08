package com.google.jepsenonspanner.loadgenerator;

import com.google.gson.Gson;
import com.google.jepsenonspanner.operation.OpRepresentation;
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

  // Added numbers in the front to break tie when transactions happen at the same time so that
  // verifier correctly identifies a valid history; this is for the type column of the history
  private static final String WRITE_ONLY_LOAD_NAME = "0txn";
  private static final String READ_WRITE_LOAD_NAME = "1txn";
  private static final String READ_ONLY_LOAD_NAME = "2txn";

  // These strings are for the string representation column i.e. a read will look like :read :x nil
  public static final String READ_OP_NAME = ":read";
  public static final String WRITE_OP_NAME = ":write";

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
                                      boolean allowMultiKeys, Config config) {
    this(new Random().nextInt(), opLimit, valueLimit, keys, allowMultiKeys, config);
  }

  /**
   * Default constructor.
   * @param seed random seed the underlying Random object takes
   * @param opLimit number of operations to issue on this worker
   * @param valueLimit max value that can be written into each key
   * @param keys an array of keys the database has
   * @param allowMultiKeys if each operation is on multiple keys
   * @param config ratios between each operation, should have size of 4
   */
  public LinearizabilityLoadGenerator(int seed, int opLimit, int valueLimit, String[] keys,
                                      boolean allowMultiKeys, Config config) {
    super(opLimit, seed);
    this.valueLimit = valueLimit;
    this.keys = keys;
    this.allowMultiKeys = allowMultiKeys;
    this.config = config;
  }

  public static LinearizabilityLoadGenerator createGeneratorFromConfig(String configPath) {
    Gson gson = new Gson();
    try {
      HashMap<String, String> config = gson.fromJson(new FileReader(new File(configPath)),
              HashMap.class);
      int opLimit = Integer.parseInt(config.get(OP_LIMIT));
      int valueLimit = Integer.parseInt(config.get(VALUE_LIMIT));
      boolean allowMultiKeys = Boolean.parseBoolean(config.get(ALLOW_MULTI_KEY));
      String[] keys = config.get(KEYS).split(" ");
      String[] opRatioString = config.get(OP_RATIO).split(" ");
      int[] opRatios = Arrays.stream(opRatioString).mapToInt(Integer::parseInt).toArray();
      return new LinearizabilityLoadGenerator(opLimit, valueLimit, keys, allowMultiKeys,
              new Config(opRatios));
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
    List<OpRepresentation> representation = new ArrayList<>();
    for (String key : selectedKeys) {
      OpRepresentation repr = OpRepresentation.createReadRepresentation(READ_OP_NAME,
              convertKeyToEdnKeyword(key));
      representation.add(repr);
    }
    return ReadTransaction.createStrongRead(READ_ONLY_LOAD_NAME, selectedKeys, representation);
  }

  private ReadWriteTransaction writeOnly() {
    List<String> selectedKeys = selectKeys();
    // Generate random values on writes
    List<TransactionalAction> writes =
            selectedKeys.stream().map(key -> TransactionalAction.createTransactionalWrite(key,
                    rand.nextInt(valueLimit) + 1)).collect(Collectors.toList());
    // Generate the string representations
    List<OpRepresentation> representation = new ArrayList<>();
    for (TransactionalAction write : writes) {
      OpRepresentation repr = OpRepresentation.createOtherRepresentation(WRITE_OP_NAME,
              convertKeyToEdnKeyword(write.getKey()), String.valueOf(write.getValue()));
      representation.add(repr);
    }
    return new ReadWriteTransaction(WRITE_ONLY_LOAD_NAME, representation, writes);
  }

  private ReadWriteTransaction transaction() {
    List<String> selectedKeys = selectKeys();
    List<TransactionalAction> txns = new ArrayList<>();
    List<OpRepresentation> representation = new ArrayList<>();
    for (String key : selectedKeys) {
      // A random boolean value to select between reads or writes
      boolean readWriteSelect = rand.nextBoolean();
      if (readWriteSelect) {
        txns.add(TransactionalAction.createTransactionalRead(key));
        representation.add(OpRepresentation.createReadRepresentation(READ_OP_NAME,
                convertKeyToEdnKeyword(key)));
      } else {
        int valueToWrite = rand.nextInt(valueLimit) + 1;
        txns.add(TransactionalAction.createTransactionalWrite(key, valueToWrite));
        representation.add(OpRepresentation.createOtherRepresentation(WRITE_OP_NAME,
                convertKeyToEdnKeyword(key), String.valueOf(valueToWrite)));
      }
    }
    return new ReadWriteTransaction(READ_WRITE_LOAD_NAME, representation, txns);
  }

  // TODO: implement
  private ReadWriteTransaction cas() {
    throw new UnsupportedOperationException();
  }

  /** Convert this key to a representation that can be stored in history table */
  private String convertKeyToEdnKeyword(String key) {
    return ":" + key;
  }
}
