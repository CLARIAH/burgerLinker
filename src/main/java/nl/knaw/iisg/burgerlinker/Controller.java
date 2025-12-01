package nl.knaw.iisg.burgerlinker;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.TupleQueryResult;

import org.jeasy.rules.api.Rule;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.mvel.MVELRuleFactory;

import org.yaml.snakeyaml.Yaml;

import nl.knaw.iisg.burgerlinker.data.MyRDF;
import nl.knaw.iisg.burgerlinker.data.YamlRuleDefinitionReader;
import nl.knaw.iisg.burgerlinker.processes.Between;
import nl.knaw.iisg.burgerlinker.processes.Closure;
import nl.knaw.iisg.burgerlinker.processes.Process;
import nl.knaw.iisg.burgerlinker.processes.Within;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.*;


public class Controller {
	@SuppressWarnings("unused")
	final private List<String> FUNCTIONS = Arrays.asList("within_b_m", "within_b_d", "between_b_d",
                                                         "between_m_m", "between_d_m", "between_b_m",
                                                         "closure");
    final private Set<String> DATA_MODEL_KEYS = Set.of("BIRTHS", "DEATHS", "MARRIAGES");

    private String dataModelDir = "./res/data_models/";
    private String dataModelExt = "yaml";
    private String rulesetDir = "./res/rule_sets/";
    private String rulesetExt = "yaml";

	private String dataModelPath, rulesetPath, function, input, namespace, query;
	private int maxLev;
	private boolean fixedLev = false, ignoreDate = false, ignoreBlock = false,
                    singleInd = false, outputFormatCSV = true, doubleInputs = false,
                    reload = false, debug = false;
    private Map<String, String> dataModel;
    private File workdir;
    private MyRDF myRDF;

	public static final Logger lg = LogManager.getLogger(Controller.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	public Controller(String function, int maxlev, boolean fixedLev,
                      boolean ignoreDate, boolean ignoreBlock, boolean singleInd,
                      String input, String workdir, String outputFormat,
                      String dataModelPath, String ruleset, String namespace,
                      String query, boolean reload, boolean debug) {
		this.function = function;
		this.maxLev = maxlev;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.input = input;
        this.reload = reload;
        this.query = query;
        this.debug = debug;

        if (workdir == null) {
            System.out.println("An work directory must be provided using '--workdir <workdir>'");

            return;
        }

        if (!workdir.endsWith("/")){
            workdir += "/";
        }
		this.workdir = new File(workdir);
        if (!this.workdir.isDirectory()) {
            System.out.println(".: Creating working directory '" + this.workdir.getName() + "/'");
            this.workdir.mkdirs();
        } else {
            System.out.println(".: Found working directory '" + this.workdir.getName() + "/'");
        }

        this.namespace = namespace;
        if (!(this.namespace.endsWith("#")
              || this.namespace.endsWith("/")
              || this.namespace.equals("_:"))) {
            LOG.logWarn("Controller", "Provided base namespace possibly malformed: '"
                        + this.namespace + "'");
        }

        this.dataModelPath = dataModelPath;
        if (!this.dataModelPath.endsWith('.' + dataModelExt)) {
            // assume shorthand format
            this.dataModelPath = dataModelDir + this.dataModelPath + '.' + dataModelExt;
        }

        this.rulesetPath = ruleset;
        if (!this.rulesetPath.endsWith('.' + rulesetExt)) {
            // assume shorthand format
            this.rulesetPath = rulesetDir + this.rulesetPath + '.' + rulesetExt;
        }

		if(!outputFormat.equals("CSV")) {
			outputFormatCSV = false;
		}
	}

	public void runProgram() {
		try {
            if (checkInputFunction() && checkAllUserInputs()) {
                // read data model specification from file
                Map<String, String> dataModel = loadYamlFromFile(this.dataModelPath);
                if (dataModel == null) {
                    LOG.logError("runProgram", "Error reading data model");
                    return;
                }

                // build and validate data model
                if (!validateDataModel(dataModel)) {
                    return;
                }
                this.dataModel = dataModel;
                LOG.outputConsole(".: Reading Data Model from '" + new File(this.dataModelPath).getName() + "'");

                // read rule definitions and remap
                Rules rulesRaw = loadRulesFromFile(this.rulesetPath);
                Map<String, Map<String, Rule>> ruleMap = buildRuleMap(rulesRaw);
                LOG.outputConsole(".: Reading Rule Definitions from '" + new File(this.rulesetPath).getName() + "'");

                // read or load data
                try {
                    if (!initGraphStore()) {
                        return;
                    }
                } catch (Exception e) {
                    LOG.logError("runProgram", "Error initiating graph store: " + e);
                }
                if (myRDF.size() <= 0) {
                    LOG.outputConsole("Error accessing graph store. Use the '--reload' flag to reload the data.");
                    return;
                }

                outputDatasetStatistics(this.dataModel);
                if (this.query != null) {
                    execQuery(query);
                } else if (this.function == null) {
                    LOG.outputConsole(".: No function specified: looping over all functions.");
                    for (String func: FUNCTIONS) {
                        if (func.equals("closure")) {
                            continue;
                        }

                        execProcess(func, ruleMap);
                    }

                    execProcess("closure", ruleMap);  // do this last
                } else if (FUNCTIONS.contains(this.function)) {
                    try {
                        execProcess(this.function, ruleMap);
                    } catch (Exception e) {
                        LOG.logError("runProgram", "Error running process: " + e);
                    }
                }
            }
		} catch (Exception e) {
            LOG.logError("runProgram", "Error: " + e);
        }

        if (myRDF != null) {
    		myRDF.shutdown();
        }
	}

    public void execQuery(String query) throws InterruptedException {
        ActivityIndicator spinner = new ActivityIndicator(".: Executing custom query on RDF store");
        spinner.start();

        TupleQueryResult qResult = myRDF.getQueryResults(query);

        spinner.terminate();
        spinner.join();
        for (BindingSet bindingSet: qResult) {
            for (Binding binding: bindingSet) {
                LOG.outputConsole(binding.getName() + ": " + binding.getValue().stringValue());
            }
            LOG.outputConsole("-");
        }

        qResult.close();
    }

    public void execProcess(String function, Map<String, Map<String, Rule>> ruleMap) throws java.io.IOException {
        Map<String, Rule> rules;
        Process process;
        switch (function) {
			case "within_b_m":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Within Births-Marriages (newborn -> bride/groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARRIAGE,
                                          Process.RelationType.WITHIN,
                                          this.dataModel);
					within(process, rules);
				}

				break;
			case "within_b_d":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Within Births-Deaths (newborn -> deceased)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.WITHIN,
                                          this.dataModel);
					within(process, rules);
				}

				break;
			case "between_b_m":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Births-Marriages (newborn parents -> bride + groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARRIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
				}

				break;
			case "between_b_d":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Births-Deaths (parents of newborn -> deceased + partner)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
				}

				break;
			case "between_d_m":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Deaths-Marriages (parents of deceased -> bride + groom)");

                    process = new Process(Process.ProcessType.DECEASED_MARRIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
				}

				break;
			case "between_m_m":
                rules = ruleMap.get(function);
				if (checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Marriages-Marriages (parents of bride/groom -> bride + groom)");

                    process = new Process(Process.ProcessType.MARRIAGE_MARRIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
                    between(process, rules);
				}

				break;
			case "closure":
				if (checkInputDirectoryContents()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Computing Transitive Closure");

                    process = new Process(this.dataModel);
					closure(process, this.namespace);
				}

				break;
        }
    }

    // ============= Data Model functions ===========

    public Map<String, String> loadYamlFromFile(String path) {
        /**
         * Read YAML-encoded data from the provided file. Expects all  (nested)
         * keys and values to be of type String.
         */
        Map<String, String> data = null;

        if (!FILE_UTILS.checkIfFileExists(path)) {
            return data;
        }

        Yaml yaml = new Yaml();
        try {
            InputStream is = new FileInputStream(new File(path));
            data = yaml.load(is);
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return data;
    }

    public boolean validateDataModel(Map<String, String> dataModel) {
        /**
         * Validate the data model by checking whether all required keys have
         * been defined.
         */
        boolean valid = true;
        for (String k: DATA_MODEL_KEYS) {
            if (!dataModel.containsKey(k)) {
                LOG.logError("validateDataModel", "Missing required data model entry: " + k);
                valid = false;

                break;
            }
        }

        return valid;
    }

    // ========= Validation Rule Functions =========

    public Rules loadRulesFromFile(String path) {
        /**
         * Read MVEL rules from specified YAML rule set
         */
        Rules rules = new Rules();
        if (!FILE_UTILS.checkIfFileExists(path)) {
            LOG.logError("loadRulesFromFile", "Error reading rule definitions");
            return rules;
        }

        try {
            MVELRuleFactory ruleFactory = new MVELRuleFactory(new YamlRuleDefinitionReader());
            rules = ruleFactory.createRules(new FileReader(path));
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return rules;
    }

    public Map<String, Map<String, Rule>> buildRuleMap(Rules rules) {
        /**
         * Create map from process name to rule.
         */
        Map<String, Map<String, Rule>> out = new HashMap<>();
        for (Rule r: rules) {
            String[] name = r.getName().split("[\\.]");  // processName.checkName
            if (!FUNCTIONS.contains(name[0])) {
                LOG.logWarn("buildRuleMap", "Found rule for unknown target: " + name[0] + " . Skipping.");
                continue;
            }

            if (!out.containsKey(name[0])) {
                out.put(name[0], new HashMap<>());
            }

            Map<String, Rule> map = out.get(name[0]);
            map.put(name[1], r);
        }

        return out;
    }

	// ========= input checks =========

	public boolean checkAllUserInputs() throws java.io.IOException {
		boolean validInputs = true;

		validInputs = validInputs & checkOutputDir();
		validInputs = validInputs & checkInputMaxLevenshtein();

		checkOutputFormatRDF();

		return validInputs;
	}

	public boolean checkInputFunction() {
		if (function != null) {
			function = function.toLowerCase();
			for (String f: FUNCTIONS) {
				if(function.equalsIgnoreCase(f)) {
					LOG.logDebug("checkInputFunction",
							"User have chosen function: " + function);
					return true;
				}
			}

			LOG.logError("checkInputFunction",
					"Incorrect user input for parameter: --function",
					"Choose one of the following options: " + FUNCTIONS.toString());

            return false;
		}

		return true;
	}

	public boolean checkInputMaxLevenshtein() {
		if(maxLev >= 0 && maxLev <= 4) {
			LOG.logDebug("checkInputMaxLevenshtein",
					"User have chosen max Levenshtein distance: " + maxLev);

			return true;
		} else {
			LOG.logError("checkInputMaxLevenshtein",
					"Invalid user input for parameter: --maxlev",
					"Specify a 'maximum Levenshtein distance' between 0 and 4");

			return false;
		}
	}

	public boolean checkInputDataset() {
		if (input.contains(",")) {
			String[] inputs = input.split(",");
			boolean check = true;
            for (String path: inputs) {
                if (!checkInputDataset(path)) {
                    check = false;
                }
            }
			doubleInputs = true;

			return check;
		} else {
			return checkInputDataset(input);
		}
	}

	public boolean checkInputDataset(String path) {
        if (path.startsWith("http")) {
            // remote endpoint
            return true;
        }
		if (FILE_UTILS.checkIfFileExists(path)) {
			LOG.logDebug("checkInputFileInput", "The following dataset is set as input dataset: "
                                                 + path);

			return true;
		} else {
			String suggestedFix = "A valid RDF file, or multiple valid RDF files separated only by a comma "
                                  + "(without a space) are required as input after parameter: --input ";
			LOG.logError("checkInputFileInput", "Invalid or Missing user input for parameter: --input",
                         suggestedFix);

			return false;
		}
	}

	public boolean checkInputDirectoryContents() {
        if(FILE_UTILS.getAllValidLinksFile(this.workdir, false) != null) {
            return true;
        }

		return false;
	}

    public boolean checkOutputDir() throws java.io.IOException {
        return FILE_UTILS.checkIfDirectoryExists(this.workdir.getCanonicalPath());
    }

	public boolean checkOutputFormatRDF() {
		if(outputFormatCSV == true) {
			LOG.logDebug("checkOutputFormatCSV", "Output format is set as CSV");

			return true;
		} else {
			LOG.logDebug("checkOutputFormatCSV", "Output format is set as RDF");

			return false;
		}
	}

    public boolean askConfirmation(String msg) {
        Scanner sc = new Scanner(System.in);
        System.out.print(".: " + msg + " ([y]/n): ");

        String answer = sc.nextLine().trim().toLowerCase();
        return (answer.equals("y") || answer.equals("yes"));
    }

	// ========= functions =========

    public boolean initGraphStore() throws java.io.IOException {
        List<String> paths = new ArrayList<>();
        if (input != null) {
            if (input.startsWith("http")) {
                LOG.outputConsole(".: SPARQL Endpoint Provided. Preparing for remote query execution.");
                myRDF = new MyRDF(input);
                myRDF.setDebug(debug);

                return true;
            } else {
                checkInputDataset();
                for (String path: input.split(",")) {
                    paths.add(path);
                }
                Collections.sort(paths);
            }
        }

        File dir = new File(this.workdir.getCanonicalPath() + "/store");
        Path infoFile = Paths.get(dir.getCanonicalPath() + "/metadata.info");
        if (!this.reload && dir.isDirectory() && dir.listFiles().length > 0) {
            if (paths.size() > 0) {
                List<String> pathsRegistered = Files.readAllLines(infoFile);
                Collections.sort(pathsRegistered);

                if (!paths.equals(pathsRegistered) &&
                    !askConfirmation("Provided input files are not associated with this work directory. Continue anyway?")) {
                    return false;
                }
            }

            LOG.outputConsole(".: Found existing RDF store. Trying to establish connection.");

            ActivityIndicator spinner = new ActivityIndicator(".: Loading RDF Store");
            spinner.start();

            // load data from existing store
            myRDF = new MyRDF(dir);
            myRDF.setDebug(debug);

            spinner.terminate();
            try {
                spinner.join();
            } catch (Exception e) {
                LOG.logError("initGraphStore", "Error waiting for ActivityIndicator to stop: " + e);

                return false;
            }

            return true;
        }

        if (this.reload && dir.isDirectory() && dir.listFiles().length > 0) {
            if (!askConfirmation("Found existing RDF store. Overwrite?")) {
                return false;
            }

            dir.delete();
        }
        Files.createDirectory(dir.toPath());

        LOG.outputConsole(".: Creating new RDF store: " + "'" + dir.getCanonicalPath() + "'");
        LOG.outputConsole(".: NOTE: Parsing a new dataset for the first time might take a while.");

        // store file names
        Files.write(infoFile, paths, StandardCharsets.UTF_8);

        myRDF = new MyRDF(dir);
        myRDF.setDebug(debug);
        return myRDF.parse(paths);
    }

	public void outputDatasetStatistics(Map<String, String> dataModel) {
		DecimalFormat formatter = new DecimalFormat("#,###");

        ActivityIndicator spinner = new ActivityIndicator(".: Validating RDF Store");
        spinner.start();

        // run query
        List<BindingSet> qResults = myRDF.getQueryResultsAsList(MyRDF.QUERY_SUMMARY);
        spinner.terminate();
            try {
                spinner.join();
            } catch (Exception e) {
                LOG.logError("initGraphStore", "Error waiting for ActivityIndicator to stop: " + e);
            }

        int uriLenMax = 0;
        int countLenMax = 0;
        for (BindingSet bindingSet: qResults) {
            String uri = bindingSet.getValue("type").stringValue();
            if (uri.length() > uriLenMax) {
                uriLenMax = uri.length();
            }

            int amount = myRDF.valueToInt(bindingSet.getValue("instanceCount"));
            String amountStr = formatter.format(amount);
            if (amountStr.length() > countLenMax) {
                countLenMax = amountStr.length();
            }
        }

        LOG.outputConsole(".: Dataset Summary");
        LOG.outputConsole("     class" + " ".repeat(uriLenMax + countLenMax - 7) + "count");
        for (BindingSet bindingSet: qResults) {
            String uri = bindingSet.getValue("type").stringValue();
            int amount = myRDF.valueToInt(bindingSet.getValue("instanceCount"));

            LOG.outputConsole("   - " + String.format("%-" + uriLenMax + "s", uri) + "   "
                                      + String.format("%" + countLenMax + "s", formatter.format(amount)));
        }
	}

	public void within(Process process, Map<String, Rule> rules) throws java.io.IOException {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = process.toString().toLowerCase() + options;

        File dir = new File(this.workdir.getCanonicalPath() + "/" + dirName);
		if (dir.exists() || dir.mkdir()) {
            File dirDict = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DICTIONARY);
            File dirDB = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DATABASE);
            File dirRes = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_RESULTS);
            if ((dirDict.exists() || dirDict.mkdir())
                && (dirDB.exists() || dirDB.mkdir())
                && (dirRes.exists() || dirRes.mkdir())) {
                Within within = new Within(myRDF, process, rules, dir, maxLev, fixedLev,
                                           ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

                if (singleInd) {
                    within.link_within_single(Person.Gender.FEMALE, false);
                    within.link_within_single(Person.Gender.MALE, true);
                } else {
                    within.link_within(Person.Gender.FEMALE, false); // false = do not close stream
                    within.link_within(Person.Gender.MALE, true); // true = close stream
                }
            } else {
				LOG.logError(process.toString(), "Error in creating the three sub workdir directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main work directory");
		}
	}

    public void between(Process process, Map<String, Rule> rules) throws java.io.IOException {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = process.toString().toLowerCase() + options;

        File dir = new File(this.workdir.getCanonicalPath() + "/" + dirName);
		if (dir.exists() || dir.mkdir()) {
            File dirDict = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DICTIONARY);
            File dirDB = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DATABASE);
            File dirRes = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_RESULTS);
            if ((dirDict.exists() || dirDict.mkdir())
                && (dirDB.exists() || dirDB.mkdir())
                && (dirRes.exists() || dirRes.mkdir())) {
                Between between = new Between(myRDF, process, rules, dir, maxLev, fixedLev,
                                              ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

                between.link_between();
            } else {
				LOG.logError(process.toString(), "Error in creating the three sub work directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main work directory");
		}
	}

	public void closure(Process process, String namespace) throws java.io.IOException {
		String dirName = "closure";

        File dir = new File(this.workdir.getCanonicalPath() + "/" + dirName);
		if (dir.exists() || dir.mkdir()) {
            File dirDB = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DATABASE);
            File dirRes = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_RESULTS);
            if ((dirDB.exists() || dirDB.mkdir()) && (dirRes.exists() || dirRes.mkdir())) {
			 	Closure closure = new Closure(myRDF, process, namespace, dir, outputFormatCSV);

                closure.computeClosure();
			} else {
				LOG.logError("Closure", "Error in creating the main work directory");
			}
		}
	}
}
