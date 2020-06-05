package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionContext;
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
public class ReadWriteTransaction extends Operation {

  private List<TransactionalAction> spannerActions;

  public ReadWriteTransaction(String loadName, List<String> recordRepresentation,
                              List<TransactionalAction> spannerActions) {
    super(loadName, recordRepresentation);
    this.spannerActions = spannerActions;
  }

  /**
   * The execution function of a ReadWriteTransaction will:
   * - Write an "invoke" entry into the history table
   * - Traverse through each TransactionalAction, and traverse through any dependent action in a
   * DFS style if there is any; abort any time there is a failed condition by throwing a
   * RuntimeException
   * - Write an "ok" entry into the history table and update the timestamp of the "invoke" entry
   * - If there is a SpannerException caused by a RuntimeError thrown from the transaction
   * function, write a "fail" entry
   * - Otherwise, write an "info" entry
   */
  @Override
  public Consumer<Executor> getExecutionPlan() {
    return executor -> {
      try {
        Timestamp recordTimestamp = executor.recordInvoke(getLoadName(), getRecordRepresentation());
        Timestamp commitTimestamp = executor.runTxn(new Executor.TransactionFunction() {
          @Override
          public void run(TransactionContext transaction) {
            for (TransactionalAction action : spannerActions) {
              long dependentValue = -1;

              // Iterate through all dependent operations and execute them first
              for (; action != null; action = action.getDependentAction()) {
                if (!action.decideProceed(dependentValue)) {
                  throw new RuntimeException(String.format("Unable to proceed to dependent " +
                          "action %s", String.valueOf(action)));
                  // abort the whole transaction if anything is determined as unable to proceed
                  // This will force Spanner to throw a ErrorCode.UNKNOWN exception
                }
                action.findDependentValue(dependentValue);
                if (action.isRead()) {
                  dependentValue = executor.executeTransactionalRead(action.getKey(), transaction);
                } else {
                  executor.executeTransactionalWrite(action.getKey(), action.getValue(), transaction);
                }
              }
            }
          }
        });
        executor.recordComplete(getLoadName(), getRecordRepresentation(), commitTimestamp,
                recordTimestamp);
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.UNKNOWN && e.getCause() instanceof RuntimeException) {
          // The transaction function has thrown a RuntimeException, meaning that the transaction
          // fails; note that RuntimeException can also be thrown from executeTransactionalRead /
          // Write
          executor.recordFail(getLoadName(), getRecordRepresentation());
        } else {
          executor.recordInfo(getLoadName(), getRecordRepresentation());
        }
      }
    };
  }

  @VisibleForTesting
  /** ALL TESTING FUNCTIONS BELOW */
  public List<TransactionalAction> getSpannerActions() {
    return Collections.unmodifiableList(spannerActions);
  }
}
