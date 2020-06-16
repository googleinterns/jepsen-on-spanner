package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.Test;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parsers;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BankVerifierTest {
  Map<String, Long> state;

  @Test
  void setUp() {
    BankVerifier verifier = new BankVerifier();
    state = Map.of(
            "0", 20L,
            "1", 20L,
            "2", 20L,
            "3", 20L,
            "4", 20L);
  }

  @Test
  void verify() {
    BankVerifier verifier = new BankVerifier();
    String input = "[{:type :invoke, :f :read, :value [\"0\" \"1\"]}]";
    verifier.verify(new StringReader(input), state);
  }
}