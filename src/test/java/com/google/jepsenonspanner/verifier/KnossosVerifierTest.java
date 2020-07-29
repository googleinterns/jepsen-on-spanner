package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class KnossosVerifierTest {
  static HashMap<String, Long> test = new HashMap(Map.of("x", 0, "y", 0));

  @Test
  void verifyNegative() {
    KnossosVerifier v = new KnossosVerifier();
    String input = "[\n" +
            "    {\n" +
            "        :type :invoke,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x nil]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :ok,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x 1]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "]";
    assertFalse(v.verifyByString(input, test));
  }

  @Test
  void verifyPositive() {
    KnossosVerifier v = new KnossosVerifier();
    String input = "[\n" +
            "    {\n" +
            "        :type :invoke,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x nil]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :ok,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x 0]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :invoke,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :y nil] [:write :x 1]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :ok,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :y 0] [:write :x 1]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :invoke,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x nil]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "    {\n" +
            "        :type :ok,\n" +
            "        :f :txn,\n" +
            "        :value [[:read :x 1]],\n" +
            "        :process 3\n" +
            "    }\n" +
            "]";
    assertTrue(v.verifyByString(input, test));
  }

  @Test
  void verifyPositive2() {
    KnossosVerifier v = new KnossosVerifier();
    String input = "[\n" +
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
            "]";
    assertTrue(v.verifyByString(input, test));
  }

  @Test
  void verifyNegative2() {
    KnossosVerifier v = new KnossosVerifier();
    String input = "[" +
            "{:type :invoke, :f :txn, :value [[:read :x nil] [:write :y 2]], :process 0, " +
            ":commitTimestamp 0, :realTimestamp 0}" +
            "{:type :invoke, :f :txn, :value [[:write :x 3] [:read :y nil]], :process 2, " +
            ":commitTimestamp 0, :realTimestamp 0}" +
            "{:type :ok, :f :txn, :value [[:read :x 0] [:write :y 2]], :process 0," +
            ":commitTimestamp 3, :realTimestamp 3}" +
            "{:type :ok, :f :txn, :value [[:write :x 3] [:read :y 0]], :process 2," +
            ":commitTimestamp 3, :realTimestamp 3}" +
            "]";
    assertFalse(v.verifyByString(input, test));
  }

  @Test
  void testSimpleGenerated() {
    Verifier v = new KnossosVerifier();
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
    assertTrue(v.verify(initKVs, "history.edn"));
  }

  @Test
  void testOriginal() {
    Verifier v = new KnossosVerifier();
    assertTrue(v.verify(test, "/usr/local/google/home/hanchiz/knossos/data/multi" +
            "-register/good/multi-register.edn"));
  }
}
