package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Executor;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * ReadWriteTransaction class encapsulates a series of read and write transactions that may or
 * may not be dependent. The intention is that this class will have as little dependency with the
 * Spanner instance as possible, since that part should be encapsulated in the Executor class.
 */
public class ReadWriteTransaction extends OperationList {

  private List<TransactionalOperation> ops;

  public ReadWriteTransaction(String loadName, List<String> recordRepresentation,
                              List<TransactionalOperation> ops) {
    super(loadName, recordRepresentation);
    this.ops = ops;
  }

  @Override
  public Consumer<Executor> getExecutionPlan() {
    return executor -> {
      try {
        Timestamp recordTimestamp = executor.recordInvoke(getLoadName(), getRecordRepresentation());
        Timestamp commitTimestamp = executor.runTxn(new Executor.TransactionFunction() {
          @Override
          public void run() {
            for (TransactionalOperation op : ops) {
              long dependentValue = -1;

              // Iterate through all dependent operations and execute them first
              for (; op != null; op = op.getDependentOp()) {
                if (!op.decideProceed(dependentValue)) {
                  throw new RuntimeException("Unable to proceed");
                  // abort the whole transaction if anything is determined as unable to proceed
                  // This will force Spanner to throw a ErrorCode.UNKNOWN exception
                }
                op.findDependentValue(dependentValue);
                if (op.isRead()) {
                  dependentValue = executor.executeTransactionalRead(op.getKey());
                } else {
                  executor.executeTransactionalWrite(op.getKey(), op.getValue());
                }
              }
            }
          }
        });
        executor.recordComplete(getLoadName(), getRecordRepresentation(), commitTimestamp,
                recordTimestamp);
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.UNKNOWN) {
          // The transaction function has thrown an error, meaning that the transaction fails
          executor.recordFail(getLoadName(), getRecordRepresentation());
        } else {
          executor.recordInfo(getLoadName(), getRecordRepresentation());
        }
      }
    };
  }

  @VisibleForTesting
  /** ALL TESTING FUNCTIONS BELOW */
  public List<TransactionalOperation> getOps() {
    return Collections.unmodifiableList(ops);
  }
}
