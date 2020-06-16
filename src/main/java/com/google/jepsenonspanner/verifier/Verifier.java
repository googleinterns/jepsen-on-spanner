package com.google.jepsenonspanner.verifier;

import com.google.jepsenonspanner.client.Executor;
import us.bpsm.edn.Keyword;

import java.util.Map;

public interface Verifier {
  static final Keyword PROCESS = Keyword.newKeyword("process");
  static final Keyword VALUE = Keyword.newKeyword("value");
  static final Keyword TYPE = Keyword.newKeyword("type");
  static final Keyword OP_NAME = Keyword.newKeyword("f");
  static final Keyword INVOKE = Keyword.newKeyword(Executor.INVOKE_STR);
  static final Keyword OK = Keyword.newKeyword(Executor.OK_STR);
  static final Keyword FAIL = Keyword.newKeyword(Executor.FAIL_STR);
  static final Keyword INFO = Keyword.newKeyword(Executor.INFO_STR);

  public void verify(String path, Map<String, Long> initialState);
}
