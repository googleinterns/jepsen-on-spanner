import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LoadGenerator;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.verifier.BankVerifier;
import com.google.jepsenonspanner.verifier.Verifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class JepsenOnSpanner {
  private static final String PARSING_ERROR = "Error parsing history file";
  private static final String HISTORY_PATH = "history.edn";
  private static final String INIT = "INIT";
  private static final String WORKER = "WORKER";
  private static final String VERIFIER = "VERIFIER";

  @Parameter(names = {"--project", "-p"}, description = "Project ID", required = true)
  private String projectId;

  @Parameter(names = {"--instance", "-i"}, description = "Instance ID", required = true)
  private String instanceId;

  @Parameter(names = {"--database", "-d"}, description = "Database ID", required = true)
  private String databaseId;

  @Parameter(names = {"--component", "-c"}, description = "Component for the binary to run",
          required = true, validateWith = ValidateComponent.class)
  private String component;

  @Parameter(names = {"--pID"}, description = "Process ID", required = true)
  private int processId;

  @Parameter(names = {"--initial-values", "-iv"}, description = "Path to csv file containing " +
          "initial state of the database; if not supplied, default to empty", validateWith =
          IsCsv.class)
  private String initValuePath;

  @Parameter(names = {"--config-file", "-cf"}, description = "Path to json file containing config" +
          " for load generator", validateWith = IsJson.class)
  private String configPath;

  private static void validatePathEndsWith(String suffix, String name, String value) throws ParameterException {
    if (!value.endsWith(suffix)) {
      throw new ParameterException("Parameter " + name + " should be a " + suffix +
              " path (found " + value +")");
    }
  }

  public static class IsCsv implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      validatePathEndsWith(/*suffix=*/".csv", name, value);
    }
  }

  public static class IsJson implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      validatePathEndsWith(/*suffix=*/".json", name, value);
    }
  }

  /**
   * Verifies if the argument supplied for component is a valid string.
   */
  public static class ValidateComponent implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (!value.equals(INIT) && !value.equals(WORKER) && !value.equals(VERIFIER)) {
        throw new ParameterException("Invalid argument " + value);
      }
    }
  }

  /**
   * Checks if a config path is provided if this is a worker
   */
  private boolean invalidArgs() {
    return component.equals(WORKER) && configPath == null;
  }

  public static void main(String[] args) {
    JepsenOnSpanner entry = new JepsenOnSpanner();
    JCommander parser = JCommander.newBuilder().addObject(entry).build();
    parser.parse(args);
    if (entry.invalidArgs()) {
      System.err.println("Unspecified config path");
      return;
    }

    entry.run();
  }

  private void run() {
    Executor executor = new Executor(projectId, instanceId, databaseId, processId,
            component.equals(INIT));
    try {
      if (component.equals(INIT)) {
        initDatabase(executor);
      } else if (component.equals(WORKER)) {
        runWorkload(executor);
      } else if (component.equals(VERIFIER)) {
        verifyHistory(executor);
      }
      System.out.printf("Component %s done\n", component);
    } finally {
      executor.close();
    }
  }

  /**
   * Executes the init component i.e. create testing and history tables, and initialize key value
   * pairs in the database. If the csv path is not supplied, no value will be inserted.
   */
  private void initDatabase(Executor executor) {
    executor.createTables();
    if (initValuePath != null) {
      executor.initKeyValues(retrieveInitialState(initValuePath));
    }
  }

  /**
   * Creates a generator and execute its loads.
   */
  private void runWorkload(Executor executor) {
    LoadGenerator gen = BankLoadGenerator.createGeneratorFromConfig(configPath);
    while (gen.hasLoad()) {
      Operation op = gen.nextOperation();
      System.out.println("Generated op " + op.toString());
      op.getExecutionPlan().accept(executor);
      System.out.println("Op " + op.toString() + " done");
    }
  }

  /**
   * Extracts history from the Spanner instance and verifies it.
   */
  private void verifyHistory(Executor executor) {
    executor.extractHistory();
    Verifier v = new BankVerifier();
    if (initValuePath != null) {
      v.verify(HISTORY_PATH, retrieveInitialState(initValuePath));
    } else {
      v.verify(HISTORY_PATH, new HashMap<>());
    }
  }

  /**
   * Given a path to the file storing initial state, retrieve the state as a Key Value map
   */
  private static HashMap<String, Long> retrieveInitialState(String path) {
    HashMap<String, Long> initKVs = new HashMap<>();
    try (Stream<String> stream = Files.lines(Paths.get(path))) {
      stream.forEach(line -> {
        String[] splitLine = line.split(",");
        initKVs.put(splitLine[0], Long.parseLong(splitLine[1]));
      });
      return initKVs;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(PARSING_ERROR);
    }
  }
}
