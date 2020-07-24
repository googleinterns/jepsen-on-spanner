package com.google.jepsenonspanner.verifier.knossos;

import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Record;
import com.google.jepsenonspanner.operation.OpRepresentation;
import us.bpsm.edn.Keyword;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;

/**
 * This class encapsulates a node in the Linearizability DFS graph. Contains the state of the
 * database, the records that are in progress and the ones that have been linearized. It
 * is essentially the state of the current search. Corresponds to a config in the paper.
 */
public class Node {
  private Map<String, Long> databaseState;
  // Since each thread can have at most one record in both call and ret set, index them using pID
  private HashMap<Long, Record> calls;
  private HashMap<Long, Record> rets;
  // Keeps track of the next position in the history to transition to
  private int recordIdx;

  // Memorize nodes visited so far so that we do not go through an invalid path twice
  private static HashSet<Node> nodesVisited = new HashSet<>();
  // Records the maximum record index we have seen; if we reached the end of the history, this
  // should be equal to length of the history
  private static int maxRecordIdxSeen = -1;

  private Node(Map<String, Long> databaseState,
               HashMap<Long, Record> calls, HashMap<Long, Record> rets, int recordIdx) {
    this.databaseState = new HashMap<>(databaseState);
    this.calls = new HashMap<>(calls);
    this.rets = new HashMap<>(rets);
    this.recordIdx = recordIdx;
    if (recordIdx > maxRecordIdxSeen) {
      maxRecordIdxSeen = recordIdx;
    }
  }

  /**
   * Constructor for the initial node.
   */
  public Node(Map<String, Long> initialState) {
    this(initialState, new HashMap<>(), new HashMap<>(), /*recordIdx=*/0);
  }

  /**
   * Copy constructor that advances the record index to the next position in history.
   */
  private Node(Node other) {
    this(other.databaseState, other.calls, other.rets, other.recordIdx + 1);
  }

  /**
   * Transitions this Node into the next possible Node(s). Reads the next position of record
   * and call, return or linearize that record based on if it exists in the call or ret sets.
   */
  public List<Node> transition(List<Record> history) {
    if (recordIdx >= history.size()) {
      return Collections.emptyList();
    }
    Record record = history.get(recordIdx);
    if (record.getType().equals(INVOKE_STR)) {
      return call(record);
    } else if (rets.containsKey(record.getpID())) {
      return ret(record);
    } else {
      return linearize(record);
    }
  }

  /**
   * Add the record to the call set. Do this when the record is an invoke record.
   */
  public List<Node> call(Record record) {
    Node node = new Node(this);
    node.calls.put(record.getpID(), record);
    nodesVisited.add(node);
    return Collections.singletonList(node);
  }

  /**
   * This record must be a return record that corresponds to an invoke record in the calls set.
   * Attempts to linearize this return record, together with all hanging records in the calls set.
   * If successful, all these records will be moved to the rets set.
   * Return all valid linearization orders, or return an empty list if no valid linearization can
   * be achieved.
   */
  public List<Node> linearize(Record record) {
    Set<Long> pIDSet = new HashSet<>(calls.keySet());
    pIDSet.add(record.getpID());
    long[] recordPIDs = pIDSet.stream().mapToLong(Number::longValue).toArray();

    List<Node> toSearch = new ArrayList<>();
    backtrackHelper(recordPIDs, 0, new HashMap<>(), record, toSearch);
    return toSearch;
  }

  /**
   * Permutates the processIDs, applying them in different orders on the database state while
   * checking if any invalid state is reached. Only adds the new node when all changes are
   * applied and the state is still valid.
   * @param changeHistory stores all the keys changed and apply them in the end, so no overhead
   *                      for keys that are not changed
   * @param record the return record that we try to linearize
   */
  private void backtrackHelper(long[] pIDs, int idx, Map<String, Long> changeHistory,
                               Record record, List<Node> toSearch) {
    if (idx >= pIDs.length) {
      Node newNode = new Node(this);
      newNode.calls.remove(record.getpID());
      newNode.rets.putAll(newNode.calls);
      newNode.calls.clear();
      newNode.databaseState.putAll(changeHistory);
      if (!nodesVisited.contains(newNode)) {
        nodesVisited.add(newNode);
        toSearch.add(newNode);
      }
    }
    for (int i = idx; i < pIDs.length; i++) {
      swapPIDs(pIDs, i, idx);

      Record recordToLinearize = calls.get(pIDs[idx]);
      if (record.getpID() == pIDs[idx]) {
        recordToLinearize = record;
      }
      if (recordToLinearize.getType().equals(OK_STR)) {
        // This is a read result, so we need to check if it is consistent with the current
        // database state
        Map<String, Long> readsToLinearize = getReadResults(recordToLinearize);
        Map<String, Long> currentState = new HashMap<>(databaseState);
        currentState.putAll(changeHistory);
        if (!isConsistent(readsToLinearize, currentState)) {
          // Not consistent, so we backtrack
          swapPIDs(pIDs, i, idx);
          continue;
        }
      }
      Map<String, Long> resultToLinearize = getWriteResults(recordToLinearize);
      Map<String, Long> changeHistoryCopy = new HashMap<>(changeHistory);
      changeHistoryCopy.putAll(resultToLinearize);

      backtrackHelper(pIDs, idx + 1, changeHistoryCopy, record, toSearch);

      swapPIDs(pIDs, i, idx);
    }
  }

  private boolean isConsistent(Map<String, Long> readResult, Map<String, Long> currentState) {
    return readResult.keySet()
            .stream()
            .allMatch(key -> readResult.get(key).equals(currentState.get(key)));
  }

  private void swapPIDs(long[] pIDs, int idx1, int idx2) {
    long temp = pIDs[idx1];
    pIDs[idx1] = pIDs[idx2];
    pIDs[idx2] = temp;
  }

  private Map<String, Long> getWriteResults(Record record) {
    Map<String, Long> res = new HashMap<>();
    List<OpRepresentation> representations = record.getOpRepresentation();
    for (OpRepresentation repr : representations) {
      if (isWrite(repr)) {
        res.put(getKeyFromOpRepresentation(repr), getValueFromOpRepresentation(repr));
      }
    }
    return res;
  }

  private Map<String, Long> getReadResults(Record record) {
    Map<String, Long> res = new HashMap<>();
    List<OpRepresentation> representations = record.getOpRepresentation();
    for (OpRepresentation repr : representations) {
      if (isRead(repr)) {
        res.put(getKeyFromOpRepresentation(repr), getValueFromOpRepresentation(repr));
      }
    }
    return res;
  }

  private boolean isRead(OpRepresentation repr) {
    return ((Keyword) repr.getEdnPrintableObjects().get(0)).getName().equals("read");
  }

  private boolean isWrite(OpRepresentation repr) {
    return ((Keyword) repr.getEdnPrintableObjects().get(0)).getName().equals("write");
  }

  private String getKeyFromOpRepresentation(OpRepresentation repr) {
    List<Object> rawObjects = repr.getEdnPrintableObjects();
    return ((Keyword) rawObjects.get(rawObjects.size() - 2)).getName();
  }

  private long getValueFromOpRepresentation(OpRepresentation repr) {
    List<Object> rawObjects = repr.getEdnPrintableObjects();
    return (long) rawObjects.get(rawObjects.size() - 1);
  }

  /**
   * Remove the return record from the rets set. Do this when this record is already linearized.
   */
  public List<Node> ret(Record record) {
    Node node = new Node(this);
    node.rets.remove(record.getpID());
    nodesVisited.add(node);
    return Collections.singletonList(node);
  }

  public static int getMaxRecordIdxSeen() {
    return maxRecordIdxSeen;
  }

  /**
   * Resets the static members, in case we want to run a new verifier.
   */
  public static void reset() {
    maxRecordIdxSeen = -1;
    nodesVisited.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Node node = (Node) o;

    return databaseState.equals(node.databaseState) &&
            calls.equals(node.calls) &&
            rets.equals(node.rets) &&
            recordIdx == node.recordIdx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseState, calls, rets, recordIdx);
  }

  @Override
  public String toString() {
    return "Node{" +
            "databaseState=" + databaseState +
            ",\n\t calls=" + calls.values().stream().map(record -> record.getpID() + " " + record.getRawRepresentation()).collect(Collectors.toList()) +
            ",\n\t rets=" + rets.values().stream().map(record -> record.getpID() + " " + record.getRawRepresentation()).collect(Collectors.toList()) +
            ",\n recordIdx=" + recordIdx +
            '}';
  }

  @VisibleForTesting
  public static HashSet<Node> getNodesVisited() {
    return nodesVisited;
  }

  @VisibleForTesting
  public int getRecordIdx() {
    return recordIdx;
  }
}
