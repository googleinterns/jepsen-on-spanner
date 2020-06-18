package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.Test;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parsers;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BankVerifierTest {
  private HashMap<String, Long> state;
  private BankVerifier verifier;

  @Test
  void setUp() {
    state = new HashMap(Map.of(
            "0", 20L,
            "1", 20L));
    verifier = new BankVerifier();
  }

  @Test
  void testVerifyRead() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testVerifyInvalidRead() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 15\" \"1 25\"], :process 0}]";
    assertFalse(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testVerifyFailRead() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :fail, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}]";
    assertFalse(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testVerifyValidTransfer() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 20\"], :process 0}" +
            "{:type :ok, :f :transfer, :value [\"0 1 20\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 0\" \"1 40\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testVerifyValidFailedTransfer() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 21\"], :process 0}" +
            "{:type :fail, :f :transfer, :value [\"0 1 21\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testVerifyInvalidFailedTransfer() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 21\"], :process 0}" +
            "{:type :fail, :f :transfer, :value [\"0 1 21\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 0\" \"1 41\"], :process 0}]";
    assertFalse(verifier.verify(new StringReader(input), state));
  }
}