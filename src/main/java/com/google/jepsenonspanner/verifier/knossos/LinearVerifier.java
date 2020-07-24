package com.google.jepsenonspanner.verifier.knossos;

import com.google.common.annotations.VisibleForTesting;
import com.google.jepsenonspanner.client.Record;
import com.google.jepsenonspanner.verifier.Verifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * A verifier that implements the optimized version of the Wing-Gong Linearizability algorithm.
 * Knossos uses the same optimization.
 */
public class LinearVerifier implements Verifier {
  @Override
  public boolean verify(Map<String, Long> initialState, String... filePath) {
    try {
      FileReader fs = new FileReader(new File(filePath[0]));
      return verify(fs, initialState);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(INVALID_FILE);
    }
  }

  /**
   * Performs an improved version of the Wing-Gong Linearization algorithm, described in this paper:
   * http://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf.
   * A dfs search across different states of the database.
   */
  @VisibleForTesting
  boolean verify(Readable input, Map<String, Long> initialState) {
    Node.reset();
    List<Record> records = Verifier.parseRecords(input);
    Stack<Node> dfs = new Stack<>();
    Node initialNode = new Node(initialState);
    dfs.push(initialNode);

    while (!dfs.empty()) {
      Node top = dfs.pop();
      List<Node> nextNodes = top.transition(records);
      dfs.addAll(nextNodes);
    }
    int maxRecordIdxSeen = Node.getMaxRecordIdxSeen();
    if (maxRecordIdxSeen < records.size()) {
      // Did not reach the end of the history, so it is invalid
      System.out.println(INVALID_INFO + "index = " + maxRecordIdxSeen + " " + records.get(maxRecordIdxSeen));
      Node.reset();
      return false;
    }
    System.out.println(VALID_INFO);
    Node.reset();
    return true;
  }
}
