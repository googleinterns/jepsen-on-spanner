package com.google.jepsenonspanner.operation;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Executor;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * ReadWriteTransaction class encapsulates a series of read and write transactions that may or
 * may not be dependent. The intention is that this class will have as little dependency with the
 * Spanner instance as possible, since that part should be encapsulated in the Executor class.
 */
public class ReadWriteTransaction extends Operation {

  private List<TransactionalAction> spannerActions;
  private boolean failed;

  public ReadWriteTransaction(String loadName, List<String> recordRepresentation,
                              List<TransactionalAction> spannerActions) {
    super(loadName, recordRepresentation);
    this.spannerActions = spannerActions;
    this.failed = false;
  }

  /**
   * The execution function of a ReadWriteTransaction will:
   * - Write an "invoke" entry into the history table
   * - Traverse through each TransactionalAction, and traverse through any dependent action in a
   * BFS style if there is any; abort any time there is a failed condition by throwing a
   * RuntimeException
   * - Write an "ok" entry into the history table and update the timestamp of the "invoke" entry
   * - If there is a SpannerException caused by a RuntimeError thrown from the transaction
   * function, write a "fail" entry
   * - Otherwise, write an "info" entry
   */
  @Override
  public Consumer<Executor> getExecutionPlan() {
    String currentOp = toString();
    return executor -> {
      try {
        Timestamp recordTimestamp = executor.recordInvoke(getLoadName(), getRecordRepresentation());
        Timestamp commitTimestamp = executor.runTxn(new Executor.TransactionFunction() {
          @Override
          public void run(TransactionContext transaction) {
            Queue<TransactionalAction> bfs = new LinkedList<>(spannerActions);
            while (!bfs.isEmpty()) {
              TransactionalAction action = bfs.poll();
              long dependentValue = -1;
              if (action.isRead()) {
                dependentValue = executor.executeTransactionalRead(action.getKey(), transaction);
                System.out.printf("Read key = %s, value = %s in %s\n", action.getKey(),
                        dependentValue, currentOp);
              } else {
                System.out.printf("Writing key = %s, value = %s in %s\n", action.getKey(),
                        action.getValue(), currentOp);
                executor.executeTransactionalWrite(action.getKey(), action.getValue(), transaction);
              }
              TransactionalAction dependent = action.getDependentAction();
              if (dependent == null) {
                continue;
              }
              if (!dependent.decideProceed(dependentValue)) {
                failed = true;
                return;
              }
              failed = false;
              dependent.findDependentValue(dependentValue);
              bfs.offer(dependent);
            }
          }
        });
        if (failed) {
          executor.recordFail(getLoadName(), getRecordRepresentation());
        } else {
          executor.recordComplete(getLoadName(), getRecordRepresentation(), commitTimestamp,
                  recordTimestamp);
        }
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.UNKNOWN && e.getCause() instanceof OperationException) {
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
