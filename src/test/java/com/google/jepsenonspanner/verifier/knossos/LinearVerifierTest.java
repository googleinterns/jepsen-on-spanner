package com.google.jepsenonspanner.verifier.knossos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

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
    assertFalse(verifier.verify(new StringReader(
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
  void testValidUnrelatedKeys() {
    assertTrue(verifier.verify(new StringReader(
            "[\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :y nil]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :y 0]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x nil]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x 0]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "]"), initialState));
  }

  @Test
  void testInvalidUnrelatedKeys() {
    assertFalse(verifier.verify(new StringReader(
            "[\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x nil]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x 1]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x nil]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x 0]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "]"), initialState));
  }

  @Test
  void testValidSameKey() {
    assertTrue(verifier.verify(new StringReader(
            "[\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x nil]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x 0]],\n" +
                    "        :process 2\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :invoke,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x nil]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:read :x 1]],\n" +
                    "        :process 1\n" +
                    "    }\n" +
                    "    {\n" +
                    "        :type :ok,\n" +
                    "        :f :txn,\n" +
                    "        :value [[:write :x 1]],\n" +
                    "        :process 3\n" +
                    "    }\n" +
                    "]"), initialState));
  }

  @Test
  void testSimpleGenerated() {
    HashMap<String, Long> initKVs = new HashMap<>();
    try (Stream<String> stream = Files.lines(Paths.get("init.csv"))) {
      stream.forEach(line -> {
        String[] splitLine = line.split(",");
        initKVs.put(splitLine[0], Long.parseLong(splitLine[1]));
      });
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("PARSING_ERROR");
    }
    assertTrue(verifier.verify(initKVs, "history.edn"));
  }
}
