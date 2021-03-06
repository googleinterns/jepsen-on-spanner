package com.google.jepsenonspanner.client;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.jepsenonspanner.operation.OpRepresentation;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.printer.Printer;
import us.bpsm.edn.printer.Printers;
import us.bpsm.edn.protocols.Protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.client.Executor.OP_NAME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.PID_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.REAL_TIME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.RECORD_TYPE_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.TIME_COLUMN_NAME;
import static com.google.jepsenonspanner.client.Executor.VALUE_COLUMN_NAME;

/**
 * This class encapsulates a record that will be written to the EDN file. Other than recording
 * all the fields in a record, it also provides support for sorting in timestamp order and a
 * printer protocol for the Java-EDN api.
 */
public class Record {
  private Keyword type;
  private Keyword load;
  private List<OpRepresentation> representation;
  private long pID;
  private Timestamp commitTimestamp;
  private Timestamp realTimestamp;

  public static final Keyword TYPE_KEYWORD = Keyword.newKeyword("type");
  public static final Keyword LOAD_KEYWORD = Keyword.newKeyword("f");
  public static final Keyword REPR_KEYWORD = Keyword.newKeyword("value");
  public static final Keyword PID_KEYWORD = Keyword.newKeyword("process");
  public static final Keyword COMMIT_TIMESTAMP_KEYWORD = Keyword.newKeyword("commitTimestamp");
  public static final Keyword REAL_TIMESTAMP_KEYWORD = Keyword.newKeyword("realTimestamp");
  public static final Keyword INVOKE_STR = Keyword.newKeyword("invoke");
  public static final Keyword OK_STR = Keyword.newKeyword("ok");
  public static final Keyword FAIL_STR = Keyword.newKeyword("fail");
  public static final Keyword INFO_STR = Keyword.newKeyword("info");

  private Record(Keyword type, Keyword load,
                 List<OpRepresentation> representation, long pID,
                 Timestamp commitTimestamp, Timestamp realTimestamp) {
    this.type = type;
    this.load = load;
    this.representation = representation;
    this.pID = pID;
    this.commitTimestamp = commitTimestamp;
    this.realTimestamp = realTimestamp;
  }

  /**
   * Creates a record that leaves the two timestamp fields null.
   */
  static Record createRecordWithoutTimestamp(Struct row) {
    Keyword type = recordCodeToString((int) row.getLong(RECORD_TYPE_COLUMN_NAME));
    String load = row.getString(OP_NAME_COLUMN_NAME).substring(1);
    long pID = row.getLong(PID_COLUMN_NAME);
    List<String> value = row.getStringList(VALUE_COLUMN_NAME);
    List<OpRepresentation> representation =
            value.stream().map(OpRepresentation::createOtherRepresentation).collect(Collectors.toList());
    return new Record(type, Keyword.newKeyword(load), representation, pID, /*commitTimestamp
    =*/null, /*realTimestamp=*/null);
  }

  /**
   * Creates a record that attempts to fill the timestamp fields. Assuming that the row struct
   * has these fields.
   */
  static Record createRecordWithTimestamp(Struct row) {
    Record record = createRecordWithoutTimestamp(row);
    if (row.isNull(TIME_COLUMN_NAME)) {
      throw new RuntimeException("Record cannot be created with a timestamp");
    }
    record.commitTimestamp = row.getTimestamp(TIME_COLUMN_NAME);
    if (!row.isNull(REAL_TIME_COLUMN_NAME)) {
      record.realTimestamp = row.getTimestamp(REAL_TIME_COLUMN_NAME);
    }
    return record;
  }

  /**
   * Creates a record from a map read from a history file. The representations here will not
   * recognize fields in the records that need update like nil; they are handled by the
   * individual verifier.
   */
  public static Record createRecordFromMap(Map<Keyword, Object> map) {
    List<List<Object>> representationObjs = (List<List<Object>>) map.get(REPR_KEYWORD);
    List<OpRepresentation> representations =
            representationObjs.stream().map(OpRepresentation::createOtherFromObjs).collect(
                    Collectors.toList());
    Long commitTimestampMilli = (Long) map.get(COMMIT_TIMESTAMP_KEYWORD);
    Long realTimestampMilli = (Long) map.get(REAL_TIMESTAMP_KEYWORD);
    return new Record((Keyword) map.get(TYPE_KEYWORD), (Keyword) map.get(LOAD_KEYWORD),
            representations, (long) map.get(PID_KEYWORD),
            parseTimestamp(commitTimestampMilli), parseTimestamp(realTimestampMilli));
  }

  static Timestamp parseTimestamp(Long milliseconds) {
    return milliseconds == null ? null : Timestamp.of(new java.sql.Timestamp(milliseconds));
  }

  /**
   * Converts the code stored in spanner to meaning operation type. See Executor.
   */
  private static Keyword recordCodeToString(int code) throws RuntimeException {
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
        throw new RuntimeException("Error parsing type code");
    }
  }

  /**
   * Converts this instance to a map to be printed to an EDN file.
   */
  private Map<Keyword, Object> getEdnRecord() {
    return Map.of(
      TYPE_KEYWORD, type,
      LOAD_KEYWORD, load,
      REPR_KEYWORD, getRawRepresentation(),
      PID_KEYWORD, pID
    );
  }

  /**
   * Returns a function that tells EDN printer to customly print this class.
   */
  static Printer.Fn<Record> getPrintFunction() {
    return (self, printer) -> printer.printValue(self.getEdnRecord());
  }

  /**
   * Returns a EDN printing protocol that includes the function to print this class.
   */
  static Protocol<Printer.Fn<?>> getPrettyPrintProtocol() {
    return Printers.prettyProtocolBuilder().put(Record.class, getPrintFunction()).build();
  }

  /**
   * Converts this instance to a map to be printed to an EDN file, including timestamp.
   */
  private Map<Keyword, Object> getEdnRecordWithTimestamp() {
    Map<Keyword, Object> record = new HashMap<>();
    record.put(TYPE_KEYWORD, type);
    record.put(LOAD_KEYWORD, load);
    record.put(REPR_KEYWORD, getRawRepresentation());
    record.put(PID_KEYWORD, pID);
    record.put(COMMIT_TIMESTAMP_KEYWORD, commitTimestamp == null ? null :
            commitTimestamp.toSqlTimestamp().getTime());
    record.put(REAL_TIMESTAMP_KEYWORD, realTimestamp == null ? null :
            realTimestamp.toSqlTimestamp().getTime());
    return record;
  }

  /**
   * Returns a function that tells EDN printer to customly print this class, including timestamp.
   */
  static Printer.Fn<Record> getPrintFunctionWithTimestamp() {
    return (self, printer) -> printer.printValue(self.getEdnRecordWithTimestamp());
  }

  /**
   * Returns a EDN printing protocol that includes the function to print this class, including
   * timestamp.
   */
  static Protocol<Printer.Fn<?>> getPrettyPrintProtocolWithTimestamp() {
    return Printers.prettyProtocolBuilder().put(Record.class, getPrintFunctionWithTimestamp()).build();
  }

  public Keyword getType() {
    return type;
  }

  public Keyword getLoad() {
    return load;
  }

  /**
   * Returns the actual list of objects under the hood. Since this class is used only in
   * Verifiers, we do not return the OpRepresentation instances, but instead directly return
   * data structures verifiers can deal with.
   */
  public List<List<Object>> getRawRepresentation() {
    return representation.stream().map(OpRepresentation::getEdnPrintableObjects).collect(Collectors.toList());
  }

  public List<OpRepresentation> getOpRepresentation() {
    return representation;
  }

  public long getpID() {
    return pID;
  }

  public Timestamp getCommitTimestamp() { return commitTimestamp; }

  public Timestamp getRealTimestamp() { return realTimestamp; }

  @Override
  public String toString() {
    return "Record{" +
            "type=" + type +
            ", load=" + load +
            ", representation=" + representation +
            ", pID=" + pID +
            ", commitTimestamp=" + commitTimestamp +
            ", realTimestamp=" + realTimestamp +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record record = (Record) o;
    return getpID() == record.getpID() &&
            getType().equals(record.getType()) &&
            getLoad().equals(record.getLoad()) &&
            representation.equals(record.representation) &&
            getCommitTimestamp().equals(record.getCommitTimestamp()) &&
            getRealTimestamp().equals(record.getRealTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getLoad(), representation, getpID(), getCommitTimestamp(),
            getRealTimestamp());
  }
}
