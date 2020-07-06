package com.google.jepsenonspanner.operation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpRepresentation {
  private List<String> representation;
  private boolean isRead;
  private String readKey;
  private Long readValue;
  private static final String NIL_VALUE = "nil";

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

  public String getPureKey() {
    return readKey.replaceAll("[\":]", "");
  }

  public void setReadValue(Long readValue) {
    this.readValue = readValue;
  }

  public List<String> getRepresentation() {
    // A non-read representation, so we do not need extra info
    if (!isRead) {
      return representation;
    }

    // Need to add the read representation
    List<String> readRepresentation = Arrays.asList(readKey, readValue == null ? NIL_VALUE :
            String.valueOf(readValue));
    // Concatenate two lists
    return Stream.concat(representation.stream(), readRepresentation.stream()).collect(Collectors.toList());
  }
}
