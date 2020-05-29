package com.google.jepsenonspanner.operation;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TimestampBound;
import com.google.jepsenonspanner.client.Recorder;
import com.google.jepsenonspanner.client.SpannerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Transaction implements OperationList {

  private List<TransactionalOperation> ops;

  public Transaction(List<TransactionalOperation> ops) {
    this.ops = ops;
  }

  @Override
  public void executeOps(SpannerClient client) {
//    StaleOperation firstStaleOp = (StaleOperation) firstOp;
//    List<StaleOperation> result = new ArrayList<>();
//    try (ResultSet resultSet = client.singleUse(firstStaleOp.isBounded() ?
//            TimestampBound.ofMaxStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS) :
//            TimestampBound.ofExactStaleness(firstStaleOp.getStaleness(), TimeUnit.MILLISECONDS))
//            .read(TESTING_TABLE_NAME, getReadKeySet(ops), Arrays.asList(KEY_COLUMN_NAME,
//                    VALUE_COLUMN_NAME))) {
//      while (resultSet.next()) {
//        result.add(new StaleOperation(resultSet.getString(0), (int) resultSet.getLong(1),
//                firstStaleOp.isBounded(), firstStaleOp.getStaleness()));
//      }
//    }
  }

  @Override
  public void record(Recorder recorder) {

  }
}
