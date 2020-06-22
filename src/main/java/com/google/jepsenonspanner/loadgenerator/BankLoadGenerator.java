package com.google.jepsenonspanner.loadgenerator;

import com.google.gson.Gson;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.operation.ReadTransaction;
import com.google.jepsenonspanner.operation.ReadWriteTransaction;
import com.google.jepsenonspanner.operation.TransactionalAction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Implements the bank benchmark load generator. Generates two kinds of load: a read across all
 * accounts and a transfer between two accounts.
 */
public class BankLoadGenerator extends LoadGenerator {

  /**
   * Configuration class to adjust distribution of randomly generated loads
   */
  public static class Config {
    private int strongRead;
    private int boundedStaleRead;
    private int exactStaleRead;
    private int transfer;

    public enum LoadType {
      STRONG_READ,
      BOUNDED_STALE_READ,
      EXACT_STALE_READ,
      TRANSFER
    }

    public Config(int strongRead, int boundedStaleRead, int exactStaleRead, int transfer) {
      if (strongRead + boundedStaleRead + exactStaleRead + transfer == 0)
        throw new RuntimeException("Invalid config");
      this.strongRead = strongRead;
      this.boundedStaleRead = boundedStaleRead;
      this.exactStaleRead = exactStaleRead;
      this.transfer = transfer;
    }

    /**
     * Given a random number by the load generator, return which load to issue
     * 0 - strong read, 1 - bounded stale read, 2 - exact stale read, 3 - transfer
     * @param randNum random number given by generator
     */
    public LoadType categorize(int randNum) {
      int distributionSum = strongRead + boundedStaleRead + exactStaleRead + transfer;
      randNum %= distributionSum;
      distributionSum -= transfer;
      if (randNum >= distributionSum) {
        return LoadType.TRANSFER;
      }
      distributionSum -= exactStaleRead;
      if (randNum >= distributionSum) {
        return LoadType.EXACT_STALE_READ;
      }
      distributionSum -= boundedStaleRead;
      if (randNum >= distributionSum) {
        return LoadType.BOUNDED_STALE_READ;
      }
      return LoadType.STRONG_READ;
    }
  }

  private int maxBalance;
  private int acctNumber;
  private Random rand;
  private int randSeed;
  private Config config;
  private long startTime;

  private static final long MAX_MILLISECOND_PAST = 5 * 60 * 1000; // 5 minutes
  public static final String READ_LOAD_NAME = "read";
  public static final String TRANSFER_LOAD_NAME = "transfer";
  private static final String OP_LIMIT = "opLimit";
  private static final String MAX_BALANCE = "maxBalance";
  private static final String ACCT_NUMBER = "acctNumber";
  private static final String RATIO_CONFIG = "opRatio";
  private static final String ERR_MSG = "Error parsing config file ";

  /**
   * Constructor for the bank load generator specifying seed
   *  @param opLimit number of operations to issue on this worker
   * @param maxBalance the maximum balance on each account
   * @param acctNumber number of accounts
   * @param config ratio of strong read : bounded stale read : exact stale read : write
   * @param seed random seed
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, Config config,
                           int seed) throws RuntimeException {
    super(opLimit);
    if (config == null) {
      throw new RuntimeException("Invalid configuration");
    }
    this.maxBalance = maxBalance;
    this.acctNumber = acctNumber;
    this.rand = new Random(seed);
    this.config = config;
    this.startTime = System.currentTimeMillis();
    System.out.printf("Created bank generator with seed %d\n", seed);
  }

  /**
   * Constructor with a random seed
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, Config, int)
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, Config config) {
    this(opLimit, maxBalance, acctNumber, config, /*seed=*/new Random().nextInt());
  }

  /**
   * Constructor with a default distribution of 2:1:1:2
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, Config)
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber) {
    this(opLimit, maxBalance, acctNumber, /*config=*/new Config(/*strongRead=*/2, /*boundedStaleRead
    =*/1, /*exactStaleRead=*/1, /*transfer=*/2));
  }

  /**
   * Constructor with a default distribution of 2:1:1:2 and a supplied seed
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, Config)
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, int randSeed) {
    this(opLimit, maxBalance, acctNumber, /*config=*/new Config(/*strongRead=*/2, /*boundedStaleRead
    =*/1, /*exactStaleRead=*/1, /*transfer=*/2), randSeed);
  }

  public static BankLoadGenerator createGeneratorFromConfig(String configPath) {
    Gson gson = new Gson();
    try {
      HashMap<String, String> config = gson.fromJson(new FileReader(new File(configPath)),
              HashMap.class);
      int opLimit = Integer.parseInt(config.get(OP_LIMIT));
      int maxBalance = Integer.parseInt(config.get(MAX_BALANCE));
      int acctNumber = Integer.parseInt(config.get(ACCT_NUMBER));
      String[] configRatios = config.get(RATIO_CONFIG).split(" ");
      if (configRatios.length != 4) {
        throw new RuntimeException(ERR_MSG + configPath);
      }
      return new BankLoadGenerator(opLimit, maxBalance, acctNumber,
              new Config(Integer.parseInt(configRatios[0]), Integer.parseInt(configRatios[1]),
                      Integer.parseInt(configRatios[2]), Integer.parseInt(configRatios[3])));
    } catch (FileNotFoundException | ClassCastException e) {
      e.printStackTrace();
      throw new RuntimeException(ERR_MSG + configPath);
    }
  }

  @Override
  public Operation nextOperation() {
    // check if reached limit
    if (opLimit <= 0) {
      throw new RuntimeException("Bank generator has reached limit");
    }

    opLimit--;
    int nextOp = rand.nextInt();
    switch (config.categorize(nextOp)) {
      case STRONG_READ:
        return strongRead();
      case BOUNDED_STALE_READ:
        return boundedStaleRead();
      case EXACT_STALE_READ:
        return exactStaleRead();
      default:
        return transfer();
    }
  }

  private List<String> getReadKeys() {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < acctNumber; i++) {
      keys.add(String.valueOf(i));
    }
    return keys;
  }

  private ReadTransaction strongRead() {
    return ReadTransaction.createStrongRead(READ_LOAD_NAME, getReadKeys());
  }

  private ReadTransaction boundedStaleRead() {
    return ReadTransaction.createBoundedStaleRead(READ_LOAD_NAME, getReadKeys(),
            rand.nextInt((int) Math.min(MAX_MILLISECOND_PAST,
                    Math.max(System.currentTimeMillis() - startTime, 1))) + 1);
  }

  private ReadTransaction exactStaleRead() {
    return ReadTransaction.createExactStaleRead(READ_LOAD_NAME, getReadKeys(),
            rand.nextInt((int) Math.min(MAX_MILLISECOND_PAST,
                    Math.max(System.currentTimeMillis() - startTime, 1))) + 1);
  }

  private ReadWriteTransaction transfer() {
    List<TransactionalAction> transaction = new ArrayList<>();

    // transfer from account 1 to account 2
    int[] accounts = rand.ints(0, acctNumber).distinct().limit(2).toArray();
    String acct1 = String.valueOf(accounts[0]);
    String acct2 = String.valueOf(accounts[1]);
    transaction.add(TransactionalAction.createTransactionalRead(acct1));
    transaction.add(TransactionalAction.createTransactionalRead(acct2));

    // add the dependent write operations
    int transferAmount = rand.nextInt(this.maxBalance / this.acctNumber) + 1;
    TransactionalAction acct1Write =
            TransactionalAction.createDependentTransactionalWrite(acct1, transferAmount,
                    balance -> balance - transferAmount,
                    balance -> balance >= transferAmount);
    transaction.get(0).setDependentAction(acct1Write);
    TransactionalAction acct2Write =
            TransactionalAction.createDependentTransactionalWrite(acct2, transferAmount,
                    balance -> balance + transferAmount,
                    balance -> true);
    transaction.get(1).setDependentAction(acct2Write);

    return new ReadWriteTransaction(TRANSFER_LOAD_NAME, Collections.singletonList(String.format(
            "%s %s %d", acct1, acct2, transferAmount)), transaction);
  }
}
