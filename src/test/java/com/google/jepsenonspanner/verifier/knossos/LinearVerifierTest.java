package com.google.jepsenonspanner.verifier.knossos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LinearVerifierTest {
  private static LinearVerifier verifier;
  private final static Map<String, Long> initialState = Map.of("x", 0L, "y", 0L, "z", 0L);

  @BeforeEach
  void setUp() {
    verifier = new LinearVerifier();
  }

  @Test
  void testBasic() {
    assertTrue(verifier.verify(new StringReader(
            "[{:type :invoke, :f :txn, :value [[:read :x nil]" +
            " [:read :y nil]], :process 0, " +
            ":commitTimestamp 2, :realTimestamp 0}" +
            "{:type :ok, :f :txn, :value [[:read :x 0] [:read :y 0]], :process 0," +
            ":commitTimestamp 2, :realTimestamp 3}]"), initialState));
  }

  @Test
  void testBasicInvalidHistory() {
    assertFalse(verifier.verify(new StringReader(
            "[{:type :invoke, :f :txn, :value [[:read :x nil] [:read :y nil]], :process 0, " +
            ":commitTimestamp 0, :realTimestamp 0}" +
            "{:type :ok, :f :txn, :value [[:read :x 2] [:read :y 0]], :process 0," +
            ":commitTimestamp 3, :realTimestamp 3}]"), initialState));
  }

  @Test
  void testValidInterleave() {
    assertTrue(verifier.verify(new StringReader(
            "[" +
                    "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:read :x 0] [:write :y 2]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 2]], :process 2," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
            "]"), initialState));
  }

  @Test
  void testValidInterleave2() {
    assertTrue(verifier.verify(new StringReader(
            "[" +
                    "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:read :x 3] [:write :y 2]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 2]], :process 2," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "]"), initialState));
  }

  @Test
  void testInvalidInterleave() {
    assertFalse(verifier.verify(new StringReader(
            "[" +
                    "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:read :x 2] [:write :y 2]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 2]], :process 2," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "]"), initialState));
  }

  @Test
  void testValidInterleave3() {
    assertTrue(verifier.verify(new StringReader(
            "[" +
                    "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:read :x 0] [:write :y 2]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 0]], :process 2," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "]"), initialState));
  }

  @Test
  void testInvalidInterleave2() {
    assertFalse(verifier.verify(new StringReader(
            "[" +
                    "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:read :x 0] [:write :y 2]], :process 0," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
                    ":commitTimestamp 0, :realTimestamp 0}" +
                    "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 0]], :process 2," +
                    ":commitTimestamp 3, :realTimestamp 3}" +
                    "]"), initialState));
  }

  @Test
  void testKnossosExample() {
    assertTrue(verifier.verify(initialState, "/usr/local/google/home/hanchiz/knossos/data/multi" +
            "-register" +
            "/good/multi-register.edn"));
  }
}
