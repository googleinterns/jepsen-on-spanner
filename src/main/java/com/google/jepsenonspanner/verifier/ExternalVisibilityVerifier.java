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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Record.FAIL_STR;
import static com.google.jepsenonspanner.client.Record.INFO_STR;
import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;

public class ExternalVisibilityVerifier implements Verifier {
  private static final Keyword READ_KEYWORD = Keyword.newKeyword("read");
  private static final Keyword WRITE_KEYWORD = Keyword.newKeyword("write");

  private TreeMap<Timestamp, Record> finishedReads = new TreeMap<>();
  private TreeMap<Timestamp, Map<String, Long>> stateChanges = new TreeMap<>();
  private Map<String, Record> hangingReadsToIgnore = new HashMap<>();

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
    List<Record> records = Verifier.parseRecords(input);
    records.sort(Comparator.comparing(this::getTimestampAccordingToLoad));

    for (Record record : records) {
      if (record.getType().equals(INFO_STR) || record.getType().equals(FAIL_STR)) {
        System.out.println(INVALID_INFO + "\n\t" + record);
        return false;
      }
      if (recordIsReadOnly(record)) {
        String currentRecordId = readOperationId(record);
        if (record.getType().equals(OK_STR)) {
          if (!hangingReadsToIgnore.containsKey(currentRecordId)) {
            finishedReads.put(record.getCommitTimestamp(), record);
          } else {
            Timestamp startTimestamp = hangingReadsToIgnore.get(currentRecordId).getRealTimestamp();
            if (!checkAbnormalRead(record, startTimestamp, initialState)) {
              return false;
            }
            hangingReadsToIgnore.remove(currentRecordId);
          }
        } else if (record.getType().equals(INVOKE_STR) &&
                record.getCommitTimestamp().compareTo(record.getRealTimestamp()) < 0) {
          hangingReadsToIgnore.put(currentRecordId, record);
        }
      } else {
        List<List<Object>> representations = record.getRawRepresentation();
        Map<String, Long> stateChange = new HashMap<>();
        for (List<Object> repr : representations) {
          if (repr.get(0).equals(WRITE_KEYWORD)) {
            String key = ((Keyword) repr.get(1)).getName();
            long value = (long) repr.get(2);
            stateChange.put(key, value);
          }
        }
        stateChanges.put(record.getCommitTimestamp(), stateChange);
      }
    }
    System.out.println(VALID_INFO);
    return true;
  }

  private boolean checkAbnormalRead(Record record, Timestamp currentStartTimestamp, Map<String,
          Long> initialState) {
    Timestamp currentCommitTimestamp = record.getCommitTimestamp();

    // Then filter out all the reads that happened between the current commit timestamp
    // and the current real start time
    Set<String> keysInThisRead = getRecordKeys(record);
    System.out.println(currentCommitTimestamp);
    System.out.println(currentStartTimestamp);
    Map<Timestamp, Record> subMapToInspect = finishedReads.subMap(currentCommitTimestamp,
            currentStartTimestamp);
    Collection<Record> readsToInspect = subMapToInspect.values();
    Set<String> allKeys =
            readsToInspect.stream().collect(HashSet::new,
                    (set, singleRecord) -> set.addAll(getRecordKeys(singleRecord)),
                    AbstractCollection::addAll);
    allKeys.addAll(keysInThisRead);
    Map<String, Long> latestState = getLastChangedValues(allKeys, currentCommitTimestamp,
            initialState);
    Timestamp startInspectTimestamp = currentCommitTimestamp;
    for (Record read : readsToInspect) {
      if (read.getRealTimestamp().compareTo(currentStartTimestamp) > 0) {
        // read is not strictly before the current abnormal read, so skip
        continue;
      }
      boolean valid = false;
      Map<Timestamp, Map<String, Long>> statesToInspect =
              stateChanges.subMap(startInspectTimestamp, /*inclusive=*/false,
                      read.getCommitTimestamp(), /*inclusive=*/true);
      Map<String, Long> stateObserved = new HashMap<>(getReadResults(read));
      // We want the current abnormal record's read result to overwrite the earlier read
      stateObserved.putAll(getReadResults(record));
      for (Map<String, Long> singleChange : statesToInspect.values()) {
        for (String key : singleChange.keySet()) {
          if (latestState.containsKey(key)) {
            latestState.put(key, singleChange.get(key));
          }
        }
        if (stateObserved.keySet().stream()
                .allMatch(key -> latestState.get(key).equals(stateObserved.get(key)))) {
          // The stateObserved, according to the two reads, have existed at a point
          valid = true;
        }
      }
      if (!valid) {
        System.out.println(INVALID_INFO + "\n\t" + record + "\n\t" + read);
        System.out.println("State observed: " + stateObserved);
        System.out.println("State changes: " + statesToInspect.values());
        return false;
      }
      startInspectTimestamp = read.getCommitTimestamp();
    }
    return true;
  }

  private Map<String, Long> getLastChangedValues(Collection<String> keys, Timestamp upToTimestamp,
                                                 Map<String, Long> initialState) {
    NavigableMap<Timestamp, Map<String, Long>> stateChangesToInspect =
            stateChanges.headMap(upToTimestamp, /*inclusive=*/true);
    HashMap<String, Long> latestState = new HashMap<>();
    for (Map<String, Long> stateChange : stateChangesToInspect.descendingMap().values()) {
      for (Iterator<String> it = keys.iterator(); it.hasNext();) {
        String key = it.next();
        if (stateChange.containsKey(key)) {
          latestState.put(key, stateChange.get(key));
          keys.remove(key);
        }
      }
    }
    for (String key : keys) {
      latestState.put(key, initialState.get(key));
    }
    return latestState;
  }

  private Set<String> getRecordKeys(Record record) {
    return record.getOpRepresentation().stream()
                .map(this::getKeyFromOpRepresentation)
                .collect(Collectors.toSet());
  }

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
    System.out.println(rawObjects);
    return (long) rawObjects.get(rawObjects.size() - 1);
  }

  private boolean recordHasRead(Record record) {
    return record.getRawRepresentation().stream().anyMatch(reprs -> reprs.get(0).equals(READ_KEYWORD));
  }

  private boolean recordIsReadOnly(Record record) {
    return record.getRawRepresentation().stream().allMatch(reprs -> reprs.get(0).equals(READ_KEYWORD));
  }

  private Timestamp getTimestampAccordingToLoad(Record record) {
    if (recordIsReadOnly(record)) {
      return record.getRealTimestamp();
    } else {
      return record.getCommitTimestamp();
    }
  }

  private String readOperationId(Record record) {
    return String.format("%d %s", record.getpID(), getRecordKeys(record));
  }
}
