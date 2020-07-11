package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.client.Record;
import us.bpsm.edn.Keyword;
import us.bpsm.edn.parser.Parseable;
import us.bpsm.edn.parser.Parser;
import us.bpsm.edn.parser.Parsers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.jepsenonspanner.constants.BenchmarkTypes.BANK_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.LINEARIZABILITY_TYPE;
import static com.google.jepsenonspanner.constants.BenchmarkTypes.INVALID_TYPE_MSG;

/**
 * Each type of workload must implement this interface to check against a history file
 */
public interface Verifier {
  // Pre-defined result strings for the history file
  String VALID_INFO = "Valid!";
  String INVALID_INFO = "Invalid operation found at ";
  String INVALID_FILE = "Invalid file";

  /**
   * Given the path to a history file and a map of initial state of the database, checks if the
   * history file reflects a consistency error the benchmark is looking for. Returns false if
   * there is an error, and prints the location of the error to System.out.
   */
  boolean verify(Map<String, Long> initialState, String... filePath);

  static List<Record> parseRecords(Readable input) {
    Parseable pbr = Parsers.newParseable(input);
    Parser parser = Parsers.newParser(Parsers.defaultConfiguration());

    // parses the edn file to record data structure
    List<Map<Keyword, Object>> recordMaps = (List<Map<Keyword, Object>>) parser.nextValue(pbr);
    return recordMaps.stream().map(Record::createRecordFromMap).collect(Collectors.toList());
  }

  static Verifier createVerifier(String benchmarkType) {
    switch (benchmarkType) {
      case BANK_TYPE:
        return new BankVerifier();
      case LINEARIZABILITY_TYPE:
        return new LinearizabilityVerifier();
      default:
        throw new RuntimeException(INVALID_TYPE_MSG);
    }
  }
}
