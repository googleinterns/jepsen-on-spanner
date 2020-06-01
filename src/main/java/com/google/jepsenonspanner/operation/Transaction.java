package com.google.jepsenonspanner.operation;

import com.google.cloud.spanner.TransactionContext;
import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.SpannerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

public class Transaction implements OperationList {

  private List<TransactionalOperation> ops;

  public Transaction(List<TransactionalOperation> ops) {
    this.ops = ops;
  }

  @Override
  public void executeOps(SpannerClient client) {
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
    client.runTxn(transactionToRun);
  }

  @Override
  public void record(Recorder recorder) {

  }
}
