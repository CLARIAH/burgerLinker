package nl.knaw.iisg.burgerlinker;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.query.BindingSet;

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
import nl.knaw.iisg.burgerlinker.utilities.*;


public class Controller {
	@SuppressWarnings("unused")
    final private Set<String> PROCESSES = Set.of("closure", "within_b_m", "within_b_d",
			                                     "between_b_m", "between_m_m", "between_d_m",
                                                 "between_b_d");
	final private Set<String> FUNCTIONS = Set.of("closure", "within_b_m", "within_b_d",
			                                     "between_b_m", "between_m_m", "between_d_m",
                                                 "between_b_d", "showDatasetStats", "convertToHDT");
    final private Set<String> DATA_MODEL_KEYS = Set.of("birth_event", "role_groom_mother", "person_gender",
                                                       "role_mother", "role_groom", "role_bride_mother",
                                                       "person_identifier", "marriage_event", "role_bride_father",
                                                       "person_family_name", "role_deceased", "person_given_name",
                                                       "role_newborn", "role_partner", "role_father",
                                                       "person", "role_groom_father","role_bride",
                                                       "death_event", "event_registration_identifier");

    private String dataModelDir = "./res/data_models/";
    private String dataModelExt = "yaml";
    private String rulesetDir = "./res/rule_sets/";
    private String rulesetExt = "yaml";

	private String dataModelPath, rulesetPath, function, input, namespace;
	private int maxLev;
	private boolean fixedLev = false, ignoreDate = false, ignoreBlock = false,
                    singleInd = false, outputFormatCSV = true, doubleInputs = false,
                    reload;
    private Map<String, String> dataModel;
    private File workdir;
    private MyRDF myRDF;

	public static final Logger lg = LogManager.getLogger(Controller.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	public Controller(String function, int maxlev, boolean fixedLev,
                      boolean ignoreDate, boolean ignoreBlock, boolean singleInd,
                      String input, String output, String outputFormat,
                      String dataModel, String ruleset, String namespace,
                      boolean reload) {
		this.function = function;
		this.maxLev = maxlev;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.input = input;
        this.reload = reload;

        if (output == null) {
            System.out.println("An output directory must be provided");

            return;
        }

        if (!output.endsWith("/")){
            output += "/";
        }
		this.workdir = new File(output);
        if (!this.workdir.isDirectory()) {
            System.out.println("Creating working directory '" + this.workdir.getName() + "/'");
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

        this.dataModelPath = dataModel;
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
                Map<String, Map<String, String>> dataModelRaw = loadYamlFromFile(this.dataModelPath);
                if (dataModelRaw == null) {
                    LOG.logError("runProgram", "Error reading data model");
                    return;
                }

                // build and validate data model
                this.dataModel = buildDataModel(dataModelRaw);
                if (!validateDataModel(this.dataModel)) {
                    return;
                }
                LOG.outputConsole(".: Reading Data Model from '" + new File(this.dataModelPath).getName() + "'");

                // read rule definitions and remap
                Rules rulesRaw = loadRulesFromFile(this.rulesetPath);
                Map<String, Map<String, Rule>> ruleMap = buildRuleMap(rulesRaw);
                LOG.outputConsole(".: Reading Rule Definitions from '" + new File(this.rulesetPath).getName() + "'");

                // read or load data
                try {
                    initGraphStore();
                } catch (Exception e) {
                    LOG.logError("runProgram", "Error initiating graph store: " + e);
                }

                outputDatasetStatistics(this.dataModel);
                if (PROCESSES.contains(this.function)) {
                    try {
                        execProcess(ruleMap);
                    } catch (Exception e) {
                        LOG.logError("runProgram", "Error running process: " + e);
                    }
                }
            }
		} catch (Exception e) {
            LOG.logError("runProgram", "Error: " + e);
        }

		myRDF.shutdown();
	}

    public void execProcess(Map<String, Map<String, Rule>> ruleMap) throws java.io.IOException {
        Map<String, Rule> rules;
        Process process;
        switch (this.function) {
			case "within_b_m":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Within Births-Marriages (newborn -> bride/groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARIAGE,
                                          Process.RelationType.WITHIN,
                                          this.dataModel);
					within(process, rules);
					LOG.outputTotalRuntime("Within Births-Marriages (newborn -> bride/groom)", startTime, true);
				}

				break;
			case "within_b_d":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Within Births-Deaths (newborn -> deceased)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.WITHIN,
                                          this.dataModel);
					within(process, rules);
					LOG.outputTotalRuntime("Within Births-Deaths (newborn -> deceased)", startTime, true);
				}

				break;
			case "between_b_m":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Births-Marriages (newborn parents -> bride + groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
					LOG.outputTotalRuntime("Between Births-Marriages (newborn parents -> bride + groom)", startTime, true);
				}

				break;
			case "between_b_d":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Births-Deaths (parents of newborn -> deceased + partner)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
					LOG.outputTotalRuntime("Between Births-Deaths (parents of newborn -> deceased + partner)", startTime, true);
				}

				break;
			case "between_d_m":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Deaths-Marriages (parents of deceased -> bride + groom)");

                    process = new Process(Process.ProcessType.DECEASED_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
					between(process, rules);
					LOG.outputTotalRuntime("Between Deaths-Marriages (parents of deceased -> bride + groom)", startTime, true);
				}

				break;
			case "between_m_m":
                rules = ruleMap.get(this.function);
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Between Marriages-Marriages (parents of bride/groom -> bride + groom)");

                    process = new Process(Process.ProcessType.MARIAGE_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          this.dataModel);
                    between(process, rules);
					LOG.outputTotalRuntime("Between Marriages-Marriages (parents of bride/groom -> bride + groom)", startTime, true);
				}

				break;
			case "closure":
				if(checkInputDataset() && checkInputDirectoryContents()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole(".: Starting Process - Computing Transitive Closure");

                    process = new Process(this.dataModel);
					closure(process, this.namespace);
					LOG.outputTotalRuntime("Computing the transitive closure", startTime, true);
				}

				break;
        }
    }

    // ============= Data Model functions ===========

    public Map<String, Map<String, String>> loadYamlFromFile(String path) {
        /**
         * Read YAML-encoded data from the provided file. Expects all  (nested)
         * keys and values to be of type String.
         */
        Map<String, Map<String, String>> data = null;

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

    public Map<String, String> buildDataModel(Map<String, Map<String, String>> dataModelRaw) {
        /**
         * Build a data model from the given input, by combining namespaces and
         * node names (fragments or path components) to form valid URIs.
         * Returns a map between internal entity names and their URIs.
         */
        Map<String, String> dataModel = new HashMap<>();
        for (String k: dataModelRaw.keySet()) {
            if (k.equals("namespace")) {
                // omit YAML anchors
                continue;
            }

            Map<String, String> valueMap = dataModelRaw.get(k);
            if (!(valueMap.containsKey("uri") && valueMap.containsKey("name"))) {
                LOG.logError("buildDataModel", "Data model misses required keys for entry" + k);
            }
            if (!(valueMap.get("uri").endsWith("/") || valueMap.get("uri").endsWith("#"))) {
                LOG.logError("buildDataModel", "Data model namespace URIs not well formed");
            }
            // add complete URI
            dataModel.put(k, valueMap.get("uri") + valueMap.get("name"));
        }

        return dataModel;
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
            if (!PROCESSES.contains(name[0])) {
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
		if(function == null) {
			LOG.logError("checkInputFunction",
					"Missing user input for parameter: --function",
					"Choose one of the following options: " + FUNCTIONS.toString());
		} else {
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
		}

		return false;
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
		if(input.contains(",")){
			String[] inputs = input.split(",");
			boolean check = checkInputDataset(inputs[0]);
			check = check & checkInputDataset(inputs[1]);
			doubleInputs = true;

			return check;
		} else {
			return checkInputDataset(input);
		}
	}

	public boolean checkInputDataset(String path) {
		if(FILE_UTILS.checkIfFileExists(path) == true) {
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

	// ========= functions =========

    public void initGraphStore() throws java.io.IOException {
        File dir = new File(this.workdir.getCanonicalPath() + "/store");
        if (!this.reload && dir.isDirectory() && dir.listFiles().length > 0) {
            LOG.outputConsole(".: Found existing RDF store. Reusing data.");

            ActivityIndicator spinner = new ActivityIndicator(".: Loading RDF Store");
            spinner.start();

            // load data from existing store
            myRDF = new MyRDF(dir);

            spinner.terminate();
            try {
                spinner.join();
            } catch (Exception e) {
                LOG.logError("initGraphStore", "Error waiting for ActivityIndicator to stop: " + e);
            }

            return;
        }

        checkInputDataset();
        String[] paths = {input};
        if(doubleInputs == true) {
            paths = input.split(",");
        }

        LOG.outputConsole(".: Creating new RDF store: " + "'" + dir.getCanonicalPath() + "'");
        myRDF = new MyRDF(dir);
        myRDF.parse(paths);
    }

	public void outputDatasetStatistics(Map<String, String> dataModel) {
		DecimalFormat formatter = new DecimalFormat("#,###");

        ActivityIndicator spinner = new ActivityIndicator(".: Validating RDF Store");
        spinner.start();

        // run query
        List<BindingSet> qResults = myRDF.getQueryResultsAsList(MyRDF.qInstanceCount);
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

        LOG.outputConsole(".: Dataset Overview");
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
		String dirName = function + options;

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

                if(singleInd == false) {
                    within.link_within("f", false); // false = do not close stream
                    within.link_within("m", true); // true = close stream
                } else {
                    within.link_within_single("f", false);
                    within.link_within_single("m", true);
                }
            } else {
				LOG.logError(process.toString(), "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main output directory");
		}
	}

    public void between(Process process, Map<String, Rule> rules) throws java.io.IOException {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

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
				LOG.logError(process.toString(), "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main output directory");
		}
	}

	public void closure(Process process, String namespace) throws java.io.IOException {
		String dirName = function;

        File dir = new File(this.workdir.getCanonicalPath() + "/" + dirName);
		if (dir.exists() || dir.mkdir()) {
            File dirDB = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_DATABASE);
            File dirRes = new File(dir.getCanonicalPath() + "/" + DIRECTORY_NAME_RESULTS);
            if ((dirDB.exists() || dirDB.mkdir()) && (dirRes.exists() || dirRes.mkdir())) {
			 	Closure closure = new Closure(myRDF, process, namespace, dir, outputFormatCSV);

                closure.computeClosure();
			} else {
				LOG.logError("Closure", "Error in creating the main output directory");
			}
		}
	}
}
