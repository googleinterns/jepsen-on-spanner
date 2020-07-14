package com.google.jepsenonspanner.verifier;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Record;
import com.google.jepsenonspanner.operation.OpRepresentation;
import us.bpsm.edn.Keyword;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Record.FAIL_STR;
import static com.google.jepsenonspanner.client.Record.INFO_STR;
import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;

/**
 * This verifier specifically test for the following case
 *
 * Consider a scenario, where start state is [[:x 0] [:y 0]], and a * signifies the commit time:
 * :write :x 1  |---*------------------------------------|
 *              2   5                                    20
 * :write :y 2       |--*-|
 *                   6  8 10
 * :read :y 2               |----*---|
 *                          11   13  15
 * :read :x 0  *                        |----|
 *             1                        16   19
 * A concurrent write to x and y seems to tell us that the write to x commits before y, but the
 * actual effect did not happen until time = 20. After the write to y returns, a read to y does
 * reflect this write. However, when the read to x is issued, since the write is still happening,
 * the commit timestamp of this read is set to a time earlier than the commit timestamp of the
 * write, which led the user to not observe the effect of the write. This read sequence will allow
 * the user to deduce that the write to x happens after the write to y, but a stale read at 6
 * will produce [[:x 1] [:y 0]], which contradicts the deduction.
 *
 * In the following comments, we are going to refer to a read whose commit timestamp is less than
 * its real start time as an "abnormal read".
 */
public class ExternalConsistencyVerifier implements Verifier {
  private static final Keyword READ_KEYWORD = Keyword.newKeyword("read");
  private static final Keyword WRITE_KEYWORD = Keyword.newKeyword("write");

  // Keeps track of all non-abnormal reads, ranked by their commit timestamps
  private TreeMap<Timestamp, Record> finishedReads = new TreeMap<>();

  // Keeps track of changes to the database state, ranked by their commit timestamps. Only stores
  // delta instead of the whole state map
  private TreeMap<Timestamp, Map<String, Long>> changeHistory = new TreeMap<>();

  @Override
  public boolean verify(Map<String, Long> initialState, String... filePath) {
    try {
      FileReader fs = new FileReader(new File(filePath[0]));
      return verify(fs, initialState);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(INVALID_FILE);
    }
  }

  @VisibleForTesting
  boolean verify(Readable input, Map<String, Long> initialState) {
    // Keeps track of all abnormal reads whose invoke records have been seen yet whose ok record
    // has not. The key is a hash of the record to identify two records that belong to the same
    // operation. Maintain this data structure because we cannot tell if a read is abnormal from
    // its ok record, and we also do not know about the read result from its invoke record. Thus we
    // need a way to connect the two records.
    Map<String, Record> hangingReadsToIgnore = new HashMap<>();

    List<Record> records = Verifier.parseRecords(input);
    records.sort(Comparator.comparing(this::getTimestampAccordingToLoad));

    for (Record record : records) {
      if (record.getType().equals(INFO_STR) || record.getType().equals(FAIL_STR)) {
        // The linearizability benchmark should not generate any error; if there is any error,
        // stop the load and inspect.
        System.out.println(INVALID_INFO + "\n\t" + record);
        return false;
      }
      if (recordIsReadOnly(record)) {
        String currentRecordId = readOperationId(record);
        if (record.getType().equals(OK_STR)) {
          if (!hangingReadsToIgnore.containsKey(currentRecordId)) {
            // A finished non-abnormal read, so add it to finishedReads
            finishedReads.put(record.getCommitTimestamp(), record);
          } else {
            // A finished abnormal read, we need its real start time
            Timestamp startTimestamp = hangingReadsToIgnore.get(currentRecordId).getRealTimestamp();
            if (!checkAbnormalRead(record, startTimestamp, initialState)) {
              return false;
            }

            // Remember to garbage collect
            hangingReadsToIgnore.remove(currentRecordId);
          }
        } else if (record.getType().equals(INVOKE_STR) &&
                record.getCommitTimestamp().compareTo(record.getRealTimestamp()) < 0) {
          // An abnormal read, but we cannot verify it yet, since we do not know its read result
          hangingReadsToIgnore.put(currentRecordId, record);
        }
      } else {
        if (record.getType().equals(OK_STR)) {
          updateStateChanges(record);
        }
      }
    }
    System.out.println(VALID_INFO);
    return true;
  }

  /**
   * Goes through all previously finished reads, then checks for each read R, if the state
   * deduced by R and current abnormal read can be observed at any point between the commit
   * timestamp of two reads i.e. if this state can be observed by any changes in between. If not,
   * return false i.e. invalid.
   * @param record the ok record that corresponds to the abnormal read
   * @param currentStartTimestamp the start timestamp of the abnoraml read
   * @param initialState initial state of the database
   */
  private boolean checkAbnormalRead(Record record, Timestamp currentStartTimestamp, Map<String,
          Long> initialState) {
    Timestamp currentCommitTimestamp = record.getCommitTimestamp();

    Set<String> keysInThisRead = getRecordKeys(record);
    // Find all the reads that happened between the current commit timestamp and the current real
    // start time; these are the reads we want to check against the current abnoraml read
    SortedMap<Timestamp, Record> subMapToInspect = finishedReads.subMap(currentCommitTimestamp,
            currentStartTimestamp);
    Timestamp maxCommitTimestamp = subMapToInspect.lastKey();
    Collection<Record> readsToInspect = subMapToInspect.values();
    // Find the set of keys that are involved in all the reads that we need to inspect, and
    // extract a latest state that contains all these keys. This aims to save space so that we do
    // not need to extract those keys that are not involved in any reads.
    Set<String> allKeys =
            readsToInspect.stream().collect(HashSet::new,
                    (set, singleRecord) -> set.addAll(getRecordKeys(singleRecord)),
                    AbstractCollection::addAll);
    allKeys.addAll(keysInThisRead);
    Map<String, Long> latestState = getLastChangedValues(allKeys, currentCommitTimestamp,
            initialState);

    // Track the read results of all normal reads
    List<Map<String, Long>> statesObserved = new ArrayList<>();
    for (Record read : readsToInspect) {
      if (read.getRealTimestamp().compareTo(currentStartTimestamp) > 0) {
        // read is not strictly before the current abnormal read, so skip
        continue;
      }
      statesObserved.add(getReadResults(read));
    }
    Map<String, Long> abnormalReadStateObserved = getReadResults(record);

    // Find all state changes between the commit timestamp of the abnormal read and the max
    // commit timestamp of normal reads
    Map<Timestamp, Map<String, Long>> statesToInspect =
            changeHistory.subMap(currentCommitTimestamp, /*inclusive=*/false,
                    maxCommitTimestamp, /*inclusive=*/true);
    if (!findState(statesObserved, abnormalReadStateObserved, latestState)) {
      for (Map<String, Long> singleChange : statesToInspect.values()) {
        for (String key : singleChange.keySet()) {
          if (latestState.containsKey(key)) {
            latestState.put(key, singleChange.get(key));
          }
        }
        if (findState(statesObserved, abnormalReadStateObserved, latestState)) {
          break;
        }
      }
    }
    if (!statesObserved.isEmpty()) {
      System.out.println(INVALID_INFO + "\n\t" + record);
      System.out.println("State observed: " + statesObserved + abnormalReadStateObserved);
      System.out.println("State changes: " + statesToInspect.values());
      return false;
    }

    return true;
  }

  /**
   * If any of the state comprised of one normal read and one abnormal read is found in the
   * latest state, remove the state of the normal read from the list. If the list is empty,
   * return true, which means that all states have been verified, and this abnormal read is valid.
   */
  private boolean findState(List<Map<String, Long>> normalReadStates,
                         Map<String, Long> abnormalReadState, Map<String, Long> latestState) {
    normalReadStates.removeIf(
            normalReadState -> foundObservedState(normalReadState, abnormalReadState, latestState));
    return normalReadStates.isEmpty();
  }

  /**
   * Given the observed state and the latest database state, returns if the observed state is
   * found in the latest state.
   */
  private boolean foundObservedState(Map<String, Long> abnormalReadObservedState,
                                     Map<String, Long> normalReadObservedState,
                                     Map<String, Long> latestState) {
    Map<String, Long> stateObserved = new HashMap<>(normalReadObservedState);
    // Do this so we overwrite the contents of the normal read
    // e.g. normal read = :x 0, :y 1, abnormal read = :x 2, :z 2
    // stateObserved = :x 2, :y 1, :z 2
    stateObserved.putAll(abnormalReadObservedState);
    return stateObserved.keySet().stream()
            .allMatch(key -> latestState.get(key).equals(stateObserved.get(key)));
  }

  /**
   * Keep track of the changes that happened in this record
   */
  private void updateStateChanges(Record record) {
    List<List<Object>> representations = record.getRawRepresentation();
    Map<String, Long> stateChange = new HashMap<>();
    for (List<Object> repr : representations) {
      if (repr.get(0).equals(WRITE_KEYWORD)) {
        String key = ((Keyword) repr.get(1)).getName();
        long value = (long) repr.get(2);
        stateChange.put(key, value);
      }
    }
    changeHistory.put(record.getCommitTimestamp(), stateChange);
  }

  /**
   * For the given keys, go backwards in history and find the latest values (up to upToTimestamp)
   * corresponding to these keys. Used to construct a partial state at upToTimestamp.
   */
  private Map<String, Long> getLastChangedValues(Collection<String> keys, Timestamp upToTimestamp,
                                                 Map<String, Long> initialState) {
    NavigableMap<Timestamp, Map<String, Long>> stateChangesToInspect =
            changeHistory.headMap(upToTimestamp, /*inclusive=*/true);
    HashMap<String, Long> latestState = new HashMap<>();
    for (Map<String, Long> stateChange : stateChangesToInspect.descendingMap().values()) {
      for (Iterator<String> it = keys.iterator(); it.hasNext();) {
        String key = it.next();
        if (stateChange.containsKey(key)) {
          // The key is found, so remove it
          latestState.put(key, stateChange.get(key));
          keys.remove(key);
        }
      }
    }
    for (String key : keys) {
      // No changes have happened to these keys, so use the initial state values
      latestState.put(key, initialState.get(key));
    }
    return latestState;
  }

  /**
   * Given a record, record all keys involved in this record
   */
  private Set<String> getRecordKeys(Record record) {
    return record.getOpRepresentation().stream()
                .map(this::getKeyFromOpRepresentation)
                .collect(Collectors.toSet());
  }

  /**
   * Given a readOnly ok record, returns the read result as a map
   */
  private Map<String, Long> getReadResults(Record record) {
    Map<String, Long> result = new HashMap<>();
    for (OpRepresentation rawRepr : record.getOpRepresentation()) {
      String key = getKeyFromOpRepresentation(rawRepr);
      long value = getValueFromOpRepresentation(rawRepr);
      result.put(key, value);
    }
    return result;
  }

  private String getKeyFromOpRepresentation(OpRepresentation repr) {
    List<Object> rawObjects = repr.getEdnPrintableObjects();
    return ((Keyword) rawObjects.get(rawObjects.size() - 2)).getName();
  }

  private long getValueFromOpRepresentation(OpRepresentation repr) {
    List<Object> rawObjects = repr.getEdnPrintableObjects();
    return (long) rawObjects.get(rawObjects.size() - 1);
  }

  private boolean recordIsReadOnly(Record record) {
    return record.getRawRepresentation().stream().allMatch(reprs -> reprs.get(0).equals(READ_KEYWORD));
  }

  /**
   * Returns the real timestamp if this record is readOnly, otherwise returns the commit timestamp.
   * Used in sorting records.
   */
  private Timestamp getTimestampAccordingToLoad(Record record) {
    if (recordIsReadOnly(record)) {
      return record.getRealTimestamp();
    } else {
      return record.getCommitTimestamp();
    }
  }

  /**
   * Hash the read only record.
   */
  private String readOperationId(Record record) {
    return String.format("%d %s", record.getpID(), getRecordKeys(record));
  }
}
