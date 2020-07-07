package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.jepsenonspanner.client.Executor.OP_NAME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.RECORD_TYPE_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.VALUE_COLUMN_NAME;

public class Record {
  private String type;
  private String load;
  private List<List<Object>> representation;
  private long pID;
  private Timestamp commitTimestamp;
  private Timestamp realTimestamp;

  public Record(Struct row) {
    Map<Keyword, Object> record = new HashMap<>();
    record.put(Keyword.newKeyword("type"),
            Keyword.newKeyword(recordCodeToString((int) row.getLong(RECORD_TYPE_COLUMN_NAME))));
    record.put(Keyword.newKeyword("f"),
            Keyword.newKeyword(row.getString(OP_NAME_COLUMN_NAME).substring(1)));
    List<String> representation = row.getStringList(VALUE_COLUMN_NAME);
    List<List<Object>> value = new ArrayList<>();
    Parser p = Parsers.newParser(Parsers.defaultConfiguration());
    for (String repr : representation) {
      String[] reprSplit = repr.split(" ");
      List<Object> currentRepr = new ArrayList<>();
      for (String split : reprSplit) {
        // Parse user defined representations as EDN compatible data structures
        Parseable pbr = Parsers.newParseable(split);
        currentRepr.add(p.nextValue(pbr));
      }
      value.add(currentRepr);
    }
    record.put(Keyword.newKeyword("value"), value);
    record.put(Keyword.newKeyword("process"), row.getLong(PID_COLUMN_NAME));
    record.put(Keyword.newKeyword("timestamp"), row.getTimestamp(TIME_COLUMN_NAME));
    record.put(Keyword.newKeyword("realTime"), row.getTimestamp(REAL_TIME_COLUMN_NAME));
    return record;
  }
}
