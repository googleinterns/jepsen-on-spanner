package com.google.jepsenonspanner.loadgenerator;

import javax.print.DocFlavor;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Implements the bank benchmark load generator. Generates two kinds of load: a read across all
 * accounts and a transfer between two accounts.
 */
public class BankLoadGenerator implements LoadGenerator {

  private int opLimit = 20;
  private int maxBalance;
  private int acctNumber;
  private Random rand;
  private int randSeed;
  private int[] distribution;

  private static final int MAX_MILLISECOND_PAST = 3000000; // 5 minutes

  /**
   * Constructor for the bank load generator specifying seed
   *
   * @param opLimit number of operations to issue on this worker
   * @param maxBalance the maximum balance on each account
   * @param acctNumber number of accounts
   * @param distribution ratio of strong read : bounded stale read : exact stale read : write
   * @param seed random seed
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, int[] distribution,
                           int seed) {
    if (distribution == null || distribution.length != 4) {
      throw new RuntimeException("Invalid distribution");
    }
    this.opLimit = opLimit;
    this.maxBalance = maxBalance;
    this.acctNumber = acctNumber;
    this.rand = new Random(seed);
    this.distribution = distribution;
  }

  /**
   * Constructor with a random seed
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, int[], int)
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber, int[] distribution) {
    this(opLimit, maxBalance, acctNumber, distribution, /*seed=*/new Random().nextInt());
  }

  /**
   * Constructor with a default distribution of 1:1:1:1
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, int[])
   */
  public BankLoadGenerator(int opLimit, int maxBalance, int acctNumber) {
    this(opLimit, maxBalance, acctNumber, /*distribution=*/new int[] {1, 1, 1, 1});
  }

  /**
   * Returns if the generator has more loads
   */
  @Override
  public boolean hasLoad() {
    return opLimit > 0;
  }

  /**
   * Returns the next operations for the client wrapper to execute
   */
  @Override
  public List<Operation> nextOperation() {
    // check if reached limit
    if (opLimit <= 0) {
      throw new RuntimeException("Bank generator has reached limit");
    }

    opLimit--;
    int distributionSum = distribution[0] + distribution[1] + distribution[2] + distribution[3];
    int nextOp = rand.nextInt() % distributionSum;
    distributionSum -= distribution[3];
    if (nextOp >= distributionSum) {
      return transfer();
    }
    distributionSum -= distribution[2];
    if (nextOp >= distributionSum) {
      return staleRead(/*bounded=*/false);
    }
    distributionSum -= distribution[1];
    if (nextOp >= distributionSum) {
      return staleRead(/*bounded=*/true);
    }
    return strongRead();
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
    int acct1 = rand.nextInt(acctNumber);
    int acct2 = -1;
    do acct2 = rand.nextInt(acctNumber); while (acct2 != acct1);
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
