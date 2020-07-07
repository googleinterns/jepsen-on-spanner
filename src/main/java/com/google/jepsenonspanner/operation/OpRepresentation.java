package com.google.jepsenonspanner.operation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class encapsulates the representation each operation would record in the history table.
 * It is also used to ensure that a nil value is properly updated in read results. For example,
 * if a representation looks like [:read :x nil], the representation part will be a list of
 * single string ":read", and the readKey will be ":x"; if a representation looks like [:write :x
 * 5] however, the representation list will be [":write", ":x", "5"], while the other values will
 * be null.
 */
public class OpRepresentation {
  private List<String> representation;
  private boolean isRead;
  private String readKey;
  private Long readValue;
  private static final String NIL_VALUE = "nil";
  private static final String DELIMITER = " ";

  private OpRepresentation(List<String> representation, boolean isRead, String readKey,
                           Long readValue) {
    this.representation = representation;
    this.isRead = isRead;
    this.readKey = readKey;
    this.readValue = readValue;
  }

  public static OpRepresentation createReadRepresentation(List<String> representation, String key) {
    return new OpRepresentation(representation, /*isRead=*/true, key, /*readValue=*/null);
  }

  public static OpRepresentation createReadRepresentation(String representation, String key) {
    return new OpRepresentation(Collections.singletonList(representation), /*isRead=*/true, key, /*readValue
    =*/null);
  }

  public static OpRepresentation createReadRepresentation(String key) {
    return new OpRepresentation(Collections.emptyList(), /*isRead=*/true, key, /*readValue=*/null);
  }

  public static OpRepresentation createOtherRepresentation(List<String> representation) {
    return new OpRepresentation(representation, /*isRead=*/false, /*readKey=*/null, /*readValue
    =*/null);
  }

  public static OpRepresentation createOtherRepresentation(String... representation) {
    return new OpRepresentation(Arrays.asList(representation), /*isRead=*/false, /*readKey=*/null,
            /*readValue=*/null);
  }

  public boolean isRead() {
    return isRead;
  }

  public String getReadKey() {
    return readKey;
  }

  /**
   * Strips the EDN representation of keys, so that they can be properly recognized in the read
   * results. e.g. A key :x represents a EDN Keyword, but in the testing table it will simply be
   * stored as x. Supports stripping of EDN Keyword or Strings.
   */
  public String getPureKey() {
    return readKey.replaceAll("[\":]", "");
  }

  public void setReadValue(Long readValue) {
    this.readValue = readValue;
  }

  /**
   * Returns a space concatenated string that represents parts of this operation. For example, a
   * bank read will have reads across several keys, so the representation string of a single key
   * would be "0" nil. This function will reflect a successful read by replacing the nil with a
   * value read.
   */
  @Override
  public String toString() {
    // A non-read representation, so we do not need extra info
    if (!isRead) {
      return String.join(DELIMITER, representation);
    }

    // Need to add the read representation
    List<String> readRepresentation = Arrays.asList(readKey, readValue == null ? NIL_VALUE :
            String.valueOf(readValue));
    // Concatenate two lists
    List<String> wholeRepresentation = Stream.concat(representation.stream(),
            readRepresentation.stream()).collect(Collectors.toList());
    return String.join(DELIMITER, wholeRepresentation);
  }
}
