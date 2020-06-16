import com.google.jepsenonspanner.client.Executor;
import com.google.jepsenonspanner.loadgenerator.BankLoadGenerator;
import com.google.jepsenonspanner.loadgenerator.LoadGenerator;
import com.google.jepsenonspanner.operation.Operation;
import com.google.jepsenonspanner.verifier.BankVerifier;
import com.google.jepsenonspanner.verifier.Verifier;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

public class JepsenOnSpanner {
  private static final String ERROR_MSG = "Usage: test --instance [instanceId] --database " +
          "[databaseId] --component [INIT / WORKER / VERIFIER] --pID [process ID] " +
          "--initial-values [path to initial values as csv]";

  public static void main(String[] args) {
//    Executor executor = new Executor(INSTANCE_ID)
    boolean init = false;
    boolean worker = false;
    boolean verifier = false;
    String instanceId = null;
    String databaseId = null;
    int processId = -1;
    String path = null;

    if (args.length != 8 && args.length != 10) {
      System.out.println(ERROR_MSG);
      return;
    }

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--instance":
        case "-i":
          instanceId = args[++i];
          break;
        case "--database":
        case "-d":
          databaseId = args[++i];
          break;
        case "--component":
        case "-c":
          String component = args[++i];
          switch (component) {
            case "INIT":
              init = true;
              break;
            case "WORKER":
              worker = true;
              break;
            case "VERIFIER":
              verifier = true;
              break;
            default:
              System.out.println(ERROR_MSG);
              return;
          }
          break;
        case "--pID":
          processId = Integer.parseInt(args[++i]);
          break;
        case "--initial-values":
          path = args[++i];
          if (!path.endsWith(".csv")) {
            System.out.println(ERROR_MSG);
            return;
          }
          break;
        default:
          System.out.println(ERROR_MSG);
          return;
      }
    }

    if (instanceId == null || databaseId == null || (init && path == null)) {
      System.out.println(ERROR_MSG);
      return;
    }

    System.out.println("Setting up connection with Spanenr...");
    Executor executor = new Executor(instanceId, databaseId, processId);
    System.out.println("Done!");
    try {
      if (init) {
        executor.createTables();
        executor.initKeyValues(retrieveInitialState(path));
      } else if (worker) {
        LoadGenerator gen = new BankLoadGenerator(5, 100, 3, new BankLoadGenerator.Config(1, 0, 0
                , 1));
        while (gen.hasLoad()) {
          Operation op = gen.nextOperation();
          System.out.println("Generated op " + op.toString());
          op.getExecutionPlan().accept(executor);
        }
      } else if (verifier) {
        executor.extractHistory();
        Verifier v = new BankVerifier();
        v.verify("history.edn", retrieveInitialState(path));
      }
    } finally {
      executor.close();
    }

    System.out.println("DONE");
  }

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
      throw new RuntimeException("ERROR");
    }
  }
}
