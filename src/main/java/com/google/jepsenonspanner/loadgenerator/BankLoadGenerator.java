package com.google.jepsenonspanner.loadgenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Implements the bank benchmark load generator. Generates two kinds of load: a read across all
 * accounts and a transfer between two accounts.
 */
public class BankLoadGenerator implements LoadGenerator {

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

  private int opLimit = 20;
  private int maxBalance;
  private int acctNumber;
  private Random rand;
  private int randSeed;
  private Config config;

  private static final int MAX_MILLISECOND_PAST = 5 * 60 * 1000; // 5 minutes

  /**
   * Constructor for the bank load generator specifying seed
   *  @param opLimit number of operations to issue on this worker
   * @param maxBalance the maximum balance on each account
   * @param acctNumber number of accounts
   * @param config ratio of strong read : bounded stale read : exact stale read : write
   * @param seed random seed
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, Config config,
                           int seed) {
    if (config == null) {
      throw new RuntimeException("Invalid configuration");
    }
    this.opLimit = opLimit;
    this.maxBalance = maxBalance;
    this.acctNumber = acctNumber;
    this.rand = new Random(seed);
    this.config = config;
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
   * Constructor with a default distribution of 1:1:1:1
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, Config)
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber) {
    this(opLimit, maxBalance, acctNumber, /*config=*/new Config(/*strongRead=*/2, /*boundedStaleRead
    =*/1, /*exactStaleRead=*/1, /*transfer=*/2));
  }

  @Override
  public boolean hasLoad() {
    return opLimit > 0;
  }

  @Override
  public List<Operation> nextOperation() {
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
        return staleRead(/*bounded=*/true);
      case EXACT_STALE_READ:
        return staleRead(/*bounded=*/false);
      default:
        return transfer();
    }
  }

  private List<Operation> strongRead() {
    List<Operation> transaction = new ArrayList<>();
    for (int i = 0; i < acctNumber; i++) {
      transaction.add(new Operation(Operation.OpType.READ, Integer.toString(i), 0));
    }
    return transaction;
  }

  private List<Operation> staleRead(boolean bounded) {
    int millisecondsPast = rand.nextInt(MAX_MILLISECOND_PAST) + 1; // prevent 0 ms in the past
    List<Operation> transaction = new ArrayList<>();
    for (int i = 0; i < acctNumber; i++) {
      transaction.add(new Operation(Operation.OpType.READ, Integer.toString(i), 0,
              millisecondsPast, bounded));
    }
    return transaction;
  }

  private List<Operation> transfer() {
    List<Operation> transaction = new ArrayList<>();

    // transfer from account 1 to account 2
    int[] accounts = rand.ints(0, acctNumber).distinct().limit(2).toArray();
    int acct1 = accounts[0];
    int acct2 = accounts[1];
    transaction.add(new Operation(Operation.OpType.READ, Integer.toString(acct1), 0));
    transaction.add(new Operation(Operation.OpType.READ, Integer.toString(acct2), 0));

    // add the dependent write operations
    int transferAmount = rand.nextInt(this.maxBalance) + 1;
    Operation acct1Write = new Operation(Operation.OpType.WRITE, Integer.toString(acct1),
            transferAmount,
            (balance, transfer) -> balance - transfer,
            (balance, transfer) -> balance >= transfer);
    transaction.get(0).setDependentOp(acct1Write);
    Operation acct2Write = new Operation(Operation.OpType.WRITE, Integer.toString(acct2),
            transferAmount,
            (balance, transfer) -> balance + transfer,
            (balance, transfer) -> true);
    transaction.get(1).setDependentOp(acct2Write);

    return transaction;
  }
}
