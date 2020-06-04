package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.jepsenonspanner.client.Executor;

import java.util.List;

public class ReadWriteTransaction extends OperationList {

  private List<TransactionalOperation> ops;

  public ReadWriteTransaction(String loadName, List<String> recordRepresentation,
                              List<TransactionalOperation> ops) {
    super(loadName, recordRepresentation);
    this.ops = ops;
  }

  @Override
  public void executeOps(Executor client) {
    try {
      Timestamp recordTimestamp = client.recordInvoke(getLoadName(), getRecordRepresentation());
      Timestamp commitTimestamp = client.runTxn(new Executor.TransactionFunction() {
        @Override
        public void run() {
          for (TransactionalOperation op : ops) {
            long dependentValue = -1;

            for (; op != null; op = op.getDependentOp()) {
              if (!op.decideProceed(dependentValue)) {
                throw new RuntimeException("Unable to proceed");
                // TODO: confirm that exception rolls back all the values
                // abort the whole transaction if anything is determined as unable to proceed
              }
              op.findDependentValue(dependentValue);
              if (op.isRead()) {
                dependentValue = client.executeTransactionalRead(op.getKey());
              } else {
                client.executeTransactionalWrite(op.getKey(), op.getValue());
              }
            }
          }
        }
      });
      client.recordComplete(getLoadName(), getRecordRepresentation(), commitTimestamp, recordTimestamp);
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.UNKNOWN) {
        client.recordFail(getLoadName(), getRecordRepresentation());
      } else {
        client.recordInfo(getLoadName(), getRecordRepresentation());
      }
    }

  }
}
