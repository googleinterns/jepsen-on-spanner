package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionContext;
import com.google.jepsenonspanner.client.Executor;

import java.util.List;
import java.util.function.Consumer;

public class ReadWriteTransaction extends OperationList {

  private List<TransactionalOperation> ops;

  public ReadWriteTransaction(String loadName, List<String> recordRepresentation,
                              List<TransactionalOperation> ops) {
    super(loadName, recordRepresentation);
    this.ops = ops;
  }

  @Override
  public void executeOps(Executor client) {
    Consumer<TransactionContext> transactionToRun = transaction /* type: TransactionContext*/ -> {
      for (TransactionalOperation op : ops) {
        long dependentValue = -1;

        for (; op != null; op = op.getDependentOp()) {
          if (!op.decideProceed(dependentValue)) {
            throw new RuntimeException("Unable to proceed");
            // TODO: roll back all the values
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

    try {
      client.recordInvoke(getLoadName(), getRecordRepresentation());
      Timestamp commitTimestamp = client.runTxn(transactionToRun);
      client.recordComplete(getLoadName(), getRecordRepresentation(), commitTimestamp);
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.UNKNOWN) {
        client.recordFail(getLoadName(), getRecordRepresentation());
      } else {
        client.recordInfo(getLoadName(), getRecordRepresentation());
      }
    }

  }
}
