package com.google.jepsenonspanner.client;

import com.google.cloud.spanner.DatabaseClient;
import com.google.jepsenonspanner.operation.StaleOperation;
import com.google.jepsenonspanner.operation.TransactionalOperation;

import java.util.List;

public class Recorder {

  public void recordStaleOps(List<StaleOperation> ops, DatabaseClient client) {

  }

  public void recordTransactionalOps(List<TransactionalOperation> ops, DatabaseClient client) {

  }
}
