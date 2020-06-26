package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.BeforeEach;
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

  @BeforeEach
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

  @Test
  void testValidConcurrentFailTransfer() {
    // A valid history where T2 finished first, and T1 did not observe its effect
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            "{:type :ok, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            // state observed could be either 0:35, 1:5 or 0:20, 1:20
            "{:type :fail, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 35\" \"1 5\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testValidConcurrentOkTransfer() {
    // A valid history where T2 finished first, and T1 observed its effect
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            "{:type :ok, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            // state observed could be either 0:35, 1:5 or 0:20, 1:20
            "{:type :invoke, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :ok, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 10\" \"1 30\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testInvalidConcurrentOkTransfer() {
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 5\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            "{:type :ok, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            // state observed could be either 0:35, 1:5 or 0:20, 1:20
            "{:type :fail, :f :transfer, :value [\"0 1 5\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 35\" \"1 5\"], :process 0}]";
    assertFalse(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testValidMultipleConcurrentFailTransfer() {
    // A valid history where T2 finished first, and T1 did not observe its effect
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            "{:type :ok, :f :transfer, :value [\"1 0 15\"], :process 1}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 15\"], :process 2}" +
            "{:type :ok, :f :transfer, :value [\"0 1 15\"], :process 2}" +
            // state observed could be either 0:35, 1:5 or 0:20, 1:20
            "{:type :fail, :f :transfer, :value [\"0 1 25\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }

  @Test
  void testValidMultipleConcurrentFailTransferMultiFail() {
    // A valid history where T2 finished first, and T1 did not observe its effect
    String input = "[{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 20\" \"1 20\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 15\"], :process 0}" +
            "{:type :invoke, :f :transfer, :value [\"1 0 30\"], :process 1}" +
            "{:type :invoke, :f :transfer, :value [\"0 1 15\"], :process 2}" +
            "{:type :ok, :f :transfer, :value [\"0 1 15\"], :process 2}" +
            // state observed could be either 0:5, 1:35 or 0:20, 1:20
            "{:type :fail, :f :transfer, :value [\"1 0 30\"], :process 1}" +
            // state observed could be either 0:5, 1:35 or 0:20, 1:20
            "{:type :fail, :f :transfer, :value [\"0 1 15\"], :process 0}" +
            "{:type :invoke, :f :read, :value [\"0 nil\" \"1 nil\"], :process 0}" +
            "{:type :ok, :f :read, :value [\"0 5\" \"1 35\"], :process 0}]";
    assertTrue(verifier.verify(new StringReader(input), state));
  }
}
