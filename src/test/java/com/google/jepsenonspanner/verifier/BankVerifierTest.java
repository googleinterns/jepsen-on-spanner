package com.google.jepsenonspanner.verifier;

import org.junit.jupiter.api.Test;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parsers;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BankVerifierTest {

  @Test
  void verify() {
    BankVerifier verifier = new BankVerifier();
    Map<String, Long> state = Map.of(
            "0", 20L,
            "1", 20L,
            "2", 20L,
            "3", 20L,
            "4", 20L);
    verifier.verify("history.edn", new HashMap<>(state));
  }
}