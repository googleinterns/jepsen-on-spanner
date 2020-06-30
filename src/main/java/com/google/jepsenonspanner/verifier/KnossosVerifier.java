package com.google.jepsenonspanner.verifier;

import java.util.Map;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import com.google.common.annotations.VisibleForTesting;

public class KnossosVerifier implements Verifier {
  private IFn require = Clojure.var("clojure.core", "require");
  private static final String KNOSSOS_COMPETITION = "knossos.competition";
  private static final String KNOSSOS_MODEL = "knossos.model";
  private static final String KNOSSOS_CLI = "knossos.cli";

  @Override
  public boolean verify(String filePath, Map<String, Long> initialState) {
    require.invoke(Clojure.read(KNOSSOS_CLI));
    IFn readHistory = Clojure.var(KNOSSOS_CLI, "read-history");
    PersistentVector history = (PersistentVector) readHistory.invoke(filePath);
    return verify(history, initialState);
  }

  @VisibleForTesting
  boolean verifyByString(String input, Map<String, Long> initialState) {
    PersistentVector history = (PersistentVector) Clojure.read(input);
    return verify(history, initialState);
  }

  private boolean verify(PersistentVector history, Map<String, Long> initialState) {
    StringBuilder sb = new StringBuilder("{");
    for (Map.Entry<String, Long> initialKVs : initialState.entrySet()) {
      sb.append(String.format(":%s %d, ", initialKVs.getKey(), initialKVs.getValue()));
    }
    sb.append("}");
    String initialStateInClojure = sb.toString();
    require.invoke(Clojure.read(KNOSSOS_COMPETITION));
    require.invoke(Clojure.read(KNOSSOS_MODEL));
    IFn analysis = Clojure.var(KNOSSOS_COMPETITION, "analysis");
    IFn multiRegisterModel = Clojure.var(KNOSSOS_MODEL, "multi-register");
    Map<Object, Object> verifyStats =
            (Map<Object, Object>) analysis.invoke(multiRegisterModel.invoke(Clojure.read(initialStateInClojure)),
                    history);
    boolean result = (boolean) verifyStats.get(Clojure.read(":valid?"));
    if (result) {
      System.out.println(VALID_INFO);
    } else {
      Map<Object, Object> op = (Map<Object, Object>) verifyStats.get(Clojure.read(":op"));
      System.out.println(INVALID_INFO + op.get(Clojure.read(":f")) + " " + op.get(Clojure.read(
              ":value")));
    }
    return result;
  }
}
