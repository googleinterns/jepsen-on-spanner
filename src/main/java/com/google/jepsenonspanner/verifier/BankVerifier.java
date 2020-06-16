package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BankVerifier implements Verifier {
  private static Keyword READ = Keyword.newKeyword(BankLoadGenerator.READ_LOAD_NAME);
  private static Keyword TRANSFER = Keyword.newKeyword(BankLoadGenerator.TRANSFER_LOAD_NAME);

  @Override
  public void verify(String path, Map<String, Long> state) {
    try {
      FileReader fs = new FileReader(new File(path));
      verify(fs, state);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  void verify(Readable input, Map<String, Long> state) {
    try {
      Parseable pbr = Parsers.newParseable(input);
      Parser parser = Parsers.newParser(Parsers.defaultConfiguration());
      List<Map<Keyword, Object>> records = (List<Map<Keyword, Object>>) parser.nextValue(pbr);

      for (Map<Keyword, Object> record : records) {
        if (record.get(TYPE) == OK) {
          Keyword opName = (Keyword) record.get(OP_NAME);
          List<String> value = (List<String>) record.get(VALUE);
          if (opName.equals(READ)) {
            Map<String, Long> currentState = new HashMap<>();
            for (String representation : value) {
              String[] keyValues = representation.split(" ");
              currentState.put(keyValues[0], Long.parseLong(keyValues[1]));
            }
            if (!currentState.equals(state)) {
              System.out.printf("Invalid history found at operation: %s %s", opName.getName(),
                      value);
            }
          } else if (opName.equals(TRANSFER)) {
            String[] transferParams = value.get(0).split(" ");
            String fromAcct = transferParams[0];
            String toAcct = transferParams[1];
            long amount = Long.parseLong(transferParams[2]);
            state.put(fromAcct, state.get(fromAcct) - amount);
            state.put(toAcct, state.get(toAcct) + amount);
          }
        }
      }

      System.out.println("Valid!");
    } catch (ClassCastException e) {
      e.printStackTrace();
    }
  }
}
