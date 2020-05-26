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

  private static final int MAX_MILLISECOND_PAST = 3000000; // 5 minutes

  /**
   * Constructor for the bank load generator specifying seed
   *
   * @param opLimit_ number of operations to issue on this worker
   * @param maxBalance_ the maximum balance on each account
   * @param acctNumber_ number of accounts
   * @param seed_ random seed
   */
  public BankLoadGenerator(int opLimit_, int maxBalance_, int acctNumber_, int seed_) {
    opLimit = opLimit_;
    maxBalance = maxBalance_;
    acctNumber = acctNumber_;
    rand = new Random(seed_);
  }

  /**
   * Constructor with a random seed
   *
   * @see BankLoadGenerator#BankLoadGenerator(int, int, int, int)
   */
  public BankLoadGenerator(int opLimit_, int maxBalance_, int acctNumber_) {
    this(opLimit_, maxBalance_, acctNumber_, new Random().nextInt());
  }

  /**
   * Returns if the generator has more loads
   */
  @Override
  public boolean hasLoad() {
    return opLimit != 0;
  }

  /**
   * Returns the next operations for the client wrapper to execute
   */
  @Override
  public List<Operation> nextOperation() {
    opLimit--;
    switch(rand.nextInt() % 3) {
      case 0:
        return strongRead();
      case 1:
        return staleRead();
      default:
        return transfer();
    }
  }

  private List<Operation> strongRead() {
    List<Operation> transaction = new ArrayList<>();
    for (int i = 0; i < acctNumber; i++) {
      transaction.add(new Operation(Operation.OpType.READ, Integer.toString(i), null));
    }
    return transaction;
  }

  private List<Operation> staleRead() {
    int millisecondsPast = rand.nextInt(MAX_MILLISECOND_PAST) + 1; // prevent 0 ms in the past
    List<Operation> transaction = new ArrayList<>();
    for (int i = 0; i < acctNumber; i++) {
      transaction.add(new Operation(Operation.OpType.READ, Integer.toString(i), null,
              millisecondsPast));
    }
    return transaction;
  }

  private List<Operation> transfer() {
    List<Operation> transaction = new ArrayList<>();

    // transfer from account 1 to account 2
    int acct1 = rand.nextInt(acctNumber);
    int acct2 = -1;
    do acct2 = rand.nextInt(acctNumber); while (acct2 != acct1);
    transaction.add(new Operation(Operation.OpType.READ, Integer.toString(acct1), null));
    transaction.add(new Operation(Operation.OpType.READ, Integer.toString(acct2), null));

    // add the dependent write operations
    String transferAmount = Integer.toString(rand.nextInt(this.maxBalance) + 1);
    Operation acct1Write = new Operation(Operation.OpType.WRITE, Integer.toString(acct1),
            transferAmount,
            (balance, transfer) -> Integer.toString(Integer.parseInt(balance) - Integer.parseInt(transfer)),
            (balance, transfer) -> Integer.parseInt(balance) >= Integer.parseInt(transfer));
    transaction.get(0).setDependentOp(acct1Write);
    Operation acct2Write = new Operation(Operation.OpType.WRITE, Integer.toString(acct2),
            transferAmount,
            (balance, transfer) -> Integer.toString(Integer.parseInt(balance) + Integer.parseInt(transfer)),
            (balance, transfer) -> true);
    transaction.get(1).setDependentOp(acct2Write);

    return transaction;
  }
}
