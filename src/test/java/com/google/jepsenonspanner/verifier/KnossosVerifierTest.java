package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

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
}
