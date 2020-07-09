package com.google.jepsenonspanner.operation;

import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;
import us.bpsm.edn.printer.Printer;
import us.bpsm.edn.printer.Printers;
import us.bpsm.edn.protocols.Protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class encapsulates the representation each operation would record in the history table.
 * It is also used to ensure that a nil value is properly updated in read results. For example,
 * if a representation looks like [:read :x nil], the representation part will be a list of
 * single string ":read", and the readKey will be ":x"; if a representation looks like [:write :x
 * 5] however, the representation list will be [":write", ":x", "5"], while the other values will
 * be null.
 */
public class OpRepresentation {
  private List<Object> representation;
  private boolean needsUpdate;
  private Object keyToUpdate;
  private Long valueToUpdate;
  private static final String NIL_VALUE = "nil";
  private static final String DELIMITER = " ";

  private OpRepresentation(List<Object> representation, boolean needsUpdate, Object keyToUpdate,
                           Long valueToUpdate) {
    if ((keyToUpdate != null) && !(keyToUpdate instanceof String) && !(keyToUpdate instanceof Keyword)) {
      // Only support string or keyword as key for now
      throw new UnsupportedOperationException();
    }
    this.representation = representation;
    this.needsUpdate = needsUpdate;
    this.keyToUpdate = keyToUpdate;
    this.valueToUpdate = valueToUpdate;
  }

  private static OpRepresentation createRepresentationFromStrings(List<String> representationStrings,
                                                                  boolean isRead, String readKey,
                                                                  Long readValue) {
    Parser parser = Parsers.newParser(Parsers.defaultConfiguration());
    List<Object> representation =
            representationStrings.stream().map(repr -> parser.nextValue(Parsers.newParseable(repr))).collect(
                    Collectors.toList());
    Object key = parser.nextValue(Parsers.newParseable(readKey));
    return new OpRepresentation(representation, isRead, key, readValue);
  }

  public static OpRepresentation createReadRepresentation(List<String> representation, String key) {
    return createRepresentationFromStrings(representation, /*isRead=*/true, key, /*readValue=*/null);
  }

  public static OpRepresentation createReadRepresentation(String representation, String key) {
    return createRepresentationFromStrings(Collections.singletonList(representation), /*isRead=*/true, key, /*readValue
    =*/null);
  }

  public static OpRepresentation createReadFromObjs(List<Object> representation, Object key,
                                                    Long value) {
    return new OpRepresentation(representation, /*isRead=*/true, key, /*readValue=*/null);
  }

  public static OpRepresentation createReadRepresentation(String key) {
    return createRepresentationFromStrings(Collections.emptyList(), /*isRead=*/true, key, /*readValue=*/null);
  }

  public static OpRepresentation createOtherRepresentation(List<String> representation) {
    return createRepresentationFromStrings(representation, /*isRead=*/false, /*readKey=*/null, /*readValue
    =*/null);
  }

  public static OpRepresentation createOtherRepresentation(String... representation) {
    return createRepresentationFromStrings(Arrays.asList(representation), /*isRead=*/false, /*readKey=*/null,
            /*readValue=*/null);
  }

  public static OpRepresentation createOtherFromObjs(List<Object> representation) {
    return new OpRepresentation(representation, /*isRead=*/false, /*key=*/null, /*readValue=*/null);
  }

  /**
   * Create a representation from a String that is concatenated using DELIMITER. Note that this
   * constructor does not recognize nil fields or reads, because it assumes that these reads no
   * longer need to be updated.
   */
  public static OpRepresentation createOtherRepresentation(String concatenatedString) {
    return createOtherRepresentation(concatenatedString.split(DELIMITER));
  }

  public boolean needsUpdate() {
    return needsUpdate;
  }

  /**
   * Strips the EDN representation of keys, so that they can be properly recognized in the read
   * results. e.g. A key :x represents a EDN Keyword, but in the testing table it will simply be
   * stored as x. Supports stripping of EDN Keyword or Strings.
   */
  public String getPureKey() {
    if (!needsUpdate()) {
      throw new RuntimeException("Cannot get key for non-read representations");
    }
    if (keyToUpdate instanceof String) {
      return (String) keyToUpdate;
    }
    if (keyToUpdate instanceof Keyword) {
      return ((Keyword) keyToUpdate).getName();
    }
    throw new UnsupportedOperationException();
  }

  public void setValueToUpdate(Long valueToUpdate) {
    this.valueToUpdate = valueToUpdate;
  }

  /**
   * Returns a space concatenated string that represents parts of this operation. For example, a
   * bank read will have reads across several keys, so the representation string of a single key
   * would be "0" nil. This function will reflect a successful read by replacing the nil with a
   * value read.
   */
  @Override
  public String toString() {
    List<String> representationStrings = representation.stream().map(Printers::printString).collect(
            Collectors.toList());
    if (needsUpdate()) {
      // A read, so we need to add the read values at the end
      representationStrings.add(Printers.printString(keyToUpdate));
      representationStrings.add(Printers.printString(valueToUpdate));
    }

    return String.join(DELIMITER, representationStrings);
  }

  /**
   * Returns the parsed representations as objects that can be printed to an EDN file
   */
  public List<Object> getEdnPrintableObjects() {
    return representation;
  }

  /**
   * Returns the function that instructs the EDN printer to print this class
   */
  public static Printer.Fn<OpRepresentation> getPrintFunction() {
    return (self, printer) -> printer.printValue(self.getEdnPrintableObjects());
  }

  /**
   * Returns the printing protocol that includes the print function
   */
  public static Protocol<Printer.Fn<?>> getPrettyPrintProtocol() {
    return Printers.prettyProtocolBuilder().put(OpRepresentation.class, getPrintFunction()).build();
  }
}
