package com.google.jepsenonspanner.verifier;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Record;
import us.bpsm.edn.Keyword;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.jepsenonspanner.client.Record.INVOKE_STR;
import static com.google.jepsenonspanner.client.Record.OK_STR;
import static com.google.jepsenonspanner.loadgenerator.LinearizabilityLoadGenerator.READ_OP_NAME;

public class ExternalVisibilityVerifier implements Verifier {
  private static final Keyword READ_KEYWORD = Keyword.newKeyword("read");

  @Override
  public boolean verify(String filePath, Map<String, Long> initialState) {
    try {
      FileReader fs = new FileReader(new File(filePath));
      return verify(fs, initialState);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(INVALID_FILE);
    }
  }

  @VisibleForTesting
  boolean verify(Readable input, Map<String, Long> initialState) {
    List<Record> records = Verifier.parseRecords(input);
    Collections.sort(records, new Comparator<Record>() {
      @Override
      public int compare(Record r1, Record r2) {
        return getTimestampAccordingToLoad(r1).compareTo(getTimestampAccordingToLoad(r2));
      }
    });
    Set<String> finishedRecords = new HashSet<>();
    for (int i = 0; i < records.size(); i++) {
      Record record = records.get(i);
      String currentId = uniqueRecordId(record);
      if (record.getType().equals(OK_STR)) {
        finishedRecords.add(currentId);
      }
      
    }
  }

  boolean recordHasRead(Record record) {
    return record.getRepresentation().stream().anyMatch(reprs -> reprs.get(0).equals(READ_KEYWORD));
  }

  boolean recordIsReadOnly(Record record) {
    return record.getRepresentation().stream().allMatch(reprs -> reprs.get(0).equals(READ_KEYWORD));
  }

  Timestamp getTimestampAccordingToLoad(Record record) {
    if (recordIsReadOnly(record)) {
      return record.getRealTimestamp();
    } else {
      return record.getCommitTimestamp();
    }
  }

  String uniqueRecordId(Record record) {
    List<List<Object>> repr = record.getRepresentation();
    List<List<Object>> reprRemovingNil = new ArrayList<>();
    for (List<Object> singleRepr : repr) {
      if (singleRepr.get(0).equals(READ_OP_NAME)) {
        // Remove the nil field in reads
        reprRemovingNil.add(singleRepr.subList(0, repr.size() - 1));
      } else {
        reprRemovingNil.add(singleRepr);
      }
    }
    return String.format("%d %s", record.getpID(), String.valueOf(reprRemovingNil));
  }
}
