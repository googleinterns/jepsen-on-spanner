package com.google.jepsenonspanner.operation;

import com.google.cloud.spanner.TransactionContext;
import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.Executor;

import java.util.List;
import java.util.function.Consumer;

public class Transaction implements OperationList {

  private List<TransactionalOperation> ops;
  private boolean readOnly;

  public Transaction(List<TransactionalOperation> ops, boolean readOnly) {
    this.ops = ops;
    this.readOnly = readOnly;
  }

  @Override
  public void executeOps(Executor client) {
    Consumer<TransactionContext> transactionToRun = transaction /* type: TransactionContext*/ -> {
      for (TransactionalOperation op : ops) {
        long dependentValue = -1;
        for (; op != null; op = op.getDependentOp()) {
          if (!op.decideProceed(dependentValue)) {
            return;
            // abort the whole transaction if anything is determined as unable to proceed
          }
          op.findDependentValue(dependentValue);
          if (op.isRead()) {
            dependentValue = client.executeTransactionalRead(op, transaction);
          } else {
            client.executeTransactionalWrite(op, transaction);
          }
        }
      }
    };
    client.runTxn(transactionToRun, readOnly);
  }

  public void record(Recorder recorder) {

  }
}
