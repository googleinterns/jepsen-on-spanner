package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExternalVisibilityVerifierTest {
  private static ExternalVisibilityVerifier verifier;
  private static Map<String, Long> initialState = Map.of("x", 0L, "y", 0L);

  @BeforeEach
  void setUp() {
    verifier = new ExternalVisibilityVerifier();
  }

  boolean stringAsReadableHelper(String history) {
    return verifier.verify(new StringReader(history), initialState);
  }

  @Test
  void testBasicValidHistory() {
    String input =
            "[{:type :invoke, :f :txn, :value [[:read :x nil] [:read :y nil]], :process 0, " +
                    ":commitTimestamp 2, :realTimestamp 0}" +
            "{:type :ok, :f :txn, :value [[:read :x 0] [:read :y 0]], :process 0," +
                    ":commitTimestamp 2, :realTimestamp 3}]";
    assertTrue(stringAsReadableHelper(input));
  }

  @Test
  void testBasicInvalidHistory() {
    String input =
            "[{:type :invoke, :f :txn, :value [[:read :x nil] [:read :y nil]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :fail, :f :txn, :value [[:read :x nil] [:read :y nil]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}]";
    assertFalse(stringAsReadableHelper(input));
  }

  @Test
  void testInvalidAbnormalReadHistory() {
    String input =
            "[{:type :invoke, :f :txn, :value [[:read :x nil]], :process 0, " +
                    ":commitTimestamp 8, :realTimestamp 4}" +
                    "{:type :invoke, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 2}" +
                    "{:type :ok, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 20}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 8}" +
                    "{:type :ok, :f :txn, :value [[:read :x 1]], :process 0," +
                    ":commitTimestamp 8, :realTimestamp 10}" +
                    "{:type :invoke, :f :txn, :value [[:read :y nil]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 11}" +
                    "{:type :ok, :f :txn, :value [[:read :y 0]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 15}" +
                    "]";
    assertFalse(stringAsReadableHelper(input));
  }

  @Test
  void testInvalidAbnormalReadHistoryMoreChanges() {
    String input =
            "[{:type :invoke, :f :txn, :value [[:read :x nil]], :process 0, " +
                    ":commitTimestamp 8, :realTimestamp 4}" +
                    "{:type :invoke, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 2}" +
                    "{:type :ok, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 20}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 8}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 1]], :process 2," +
                    ":commitTimestamp 7, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3]], :process 2," +
                    ":commitTimestamp 7, :realTimestamp 8}" +
                    "{:type :ok, :f :txn, :value [[:read :x 3]], :process 0," +
                    ":commitTimestamp 8, :realTimestamp 10}" +
                    "{:type :invoke, :f :txn, :value [[:read :y nil]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 11}" +
                    "{:type :ok, :f :txn, :value [[:read :y 0]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 15}" +
                    "]";
    assertFalse(stringAsReadableHelper(input));
  }

  @Test
  void testInvalidAbnormalReadHistoryMultipleReads() {
    String input =
            "[{:type :invoke, :f :txn, :value [[:read :x nil]], :process 0, " +
                    ":commitTimestamp 8, :realTimestamp 4}" +
                    "{:type :invoke, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 2}" +
                    "{:type :ok, :f :txn, :value [[:write :y 2]], :process 2," +
                    ":commitTimestamp 5, :realTimestamp 20}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 1]], :process 1," +
                    ":commitTimestamp 6, :realTimestamp 8}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 1]], :process 2," +
                    ":commitTimestamp 7, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3]], :process 2," +
                    ":commitTimestamp 7, :realTimestamp 8}" +
                    "{:type :ok, :f :txn, :value [[:read :x 3]], :process 0," +
                    ":commitTimestamp 8, :realTimestamp 10}" +
                    "{:type :invoke, :f :txn, :value [[:read :y nil]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 11}" +
                    "{:type :ok, :f :txn, :value [[:read :y 0]], :process 0," +
                    ":commitTimestamp 1, :realTimestamp 15}" +
                    "]";
    assertFalse(stringAsReadableHelper(input));
  }
}
