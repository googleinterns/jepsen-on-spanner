package com.google.jepsenonspanner.verifier;

import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class BankVerifier implements Verifier {
  @Override
  public void verify(String path) {
    try {
      FileReader fs = new FileReader(new File(path));
      Parseable pbr = Parsers.newParseable(fs);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
