package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;
import us.bpsm.edn.printer.Printer;
import us.bpsm.edn.printer.Printers;
import us.bpsm.edn.protocols.Protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.jepsenonspanner.client.Executor.FAIL_STR;
import static com.google.jepsenonspanner.client.Executor.INFO_STR;
import static com.google.jepsenonspanner.client.Executor.INVOKE_STR;
import static com.google.jepsenonspanner.client.Executor.OK_STR;
import static com.google.jepsenonspanner.client.Executor.OP_NAME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.PID_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.REAL_TIME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.RECORDER_ERROR;
import static com.google.jepsenonspanner.client.Executor.RECORD_TYPE_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.TIME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.VALUE_COLUMN_NAME;

/**
 * This class encapsulates a record that will be written to the EDN file. Other than recording
 * all the fields in a record, it also provides support for sorting in timestamp order and a
 * printer protocol for the Java-EDN api.
 */
public class Record implements Comparable<Record> {
  private String type;
  private String load;
  private List<List<Object>> representation;
  private long pID;
  private Timestamp commitTimestamp;
  private Timestamp realTimestamp;

  private static final Keyword TYPE_KEYWORD = Keyword.newKeyword("type");
  private static final Keyword LOAD_KEYWORD = Keyword.newKeyword("f");
  private static final Keyword REPR_KEYWORD = Keyword.newKeyword("value");
  private static final Keyword PID_KEYWORD = Keyword.newKeyword("process");

  private Record(String type, String load,
                 List<List<Object>> representation, long pID,
                 Timestamp commitTimestamp, Timestamp realTimestamp) {
    this.type = type;
    this.load = load;
    this.representation = representation;
    this.pID = pID;
    this.commitTimestamp = commitTimestamp;
    this.realTimestamp = realTimestamp;
  }

  public static Record createRecordWithoutTimestamp(Struct row) {
    String type = recordCodeToString((int) row.getLong(RECORD_TYPE_COLUMN_NAME));
    String load = row.getString(OP_NAME_COLUMN_NAME).substring(1);
    long pID = row.getLong(PID_COLUMN_NAME);
    List<List<Object>> representation = new ArrayList<>();
    List<String> value = row.getStringList(VALUE_COLUMN_NAME);
    Parser p = Parsers.newParser(Parsers.defaultConfiguration());
    for (String repr : value) {
      String[] reprSplit = repr.split(" ");
      List<Object> currentRepr = new ArrayList<>();
      for (String split : reprSplit) {
        // Parse user defined representations as EDN compatible data structures
        Parseable pbr = Parsers.newParseable(split);
        currentRepr.add(p.nextValue(pbr));
      }
      representation.add(currentRepr);
    }
    return new Record(type, load, representation, pID, /*commitTimestamp=*/null, /*realTimestamp
    =*/null);
  }

  public static Record createRecordWithTimestamp(Struct row) {
    Record record = createRecordWithoutTimestamp(row);
    record.commitTimestamp = row.getTimestamp(TIME_COLUMN_NAME);
    if (!row.isNull(REAL_TIME_COLUMN_NAME)) {
      record.realTimestamp = row.getTimestamp(REAL_TIME_COLUMN_NAME);
    }
    return record;
  }

  private static String recordCodeToString(int code) throws RuntimeException {
    switch (code) {
      case 0:
        return INVOKE_STR;
      case 1:
        return FAIL_STR;
      case 2:
        return INFO_STR;
      case 3:
        return OK_STR;
      default:
        throw new RuntimeException(RECORDER_ERROR);
    }
  }

  @Override
  public int compareTo(Record record) {
    Timestamp myTimestamp = getTimestamp();
    Timestamp otherTimestamp = record.getTimestamp();
    return myTimestamp.compareTo(otherTimestamp);
  }

  private Timestamp getTimestamp() {
    if (commitTimestamp == null) {
      throw new RuntimeException("Record cannot be compared because it does not have timestamp");
    }
    return realTimestamp == null ? commitTimestamp : realTimestamp;
  }

  private Map<Keyword, Object> getEdnRecord() {
    return Map.of(
      TYPE_KEYWORD, Keyword.newKeyword(type),
      LOAD_KEYWORD, Keyword.newKeyword(load),
      REPR_KEYWORD, representation,
      PID_KEYWORD, pID
    );
  }

  static Printer.Fn<Record> getPrintFunction() {
    return (self, printer) -> printer.printValue(self.getEdnRecord());
  }

  static Protocol<Printer.Fn<?>> getPrettyPrintProtocol() {
    return Printers.prettyProtocolBuilder().put(Record.class, getPrintFunction()).build();
  }
}