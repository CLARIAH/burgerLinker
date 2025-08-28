package nl.knaw.iisg.burgerlinker;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.yaml.snakeyaml.Yaml;

import nl.knaw.iisg.burgerlinker.processes.*;
import nl.knaw.iisg.burgerlinker.processes.Process;
import nl.knaw.iisg.burgerlinker.utilities.*;


public class Controller {
	@SuppressWarnings("unused")
    final private Set<String> PROCESSES = Set.of("closure", "within_b_m", "within_b_d",
			                                     "between_b_m", "between_m_m", "between_d_m",
                                                 "between_b_d");
	final private Set<String> FUNCTIONS = Set.of("closure", "within_b_m", "within_b_d",
			                                     "between_b_m", "between_m_m", "between_d_m",
                                                 "between_b_d", "showDatasetStats", "convertToHDT");
    final private Set<String> DATA_MODEL_KEYS = Set.of("birth_event", "role_groom_mother", "location_country",
                                                       "event_location", "person_gender", "role_mother",
                                                       "role_groom", "role_bride_mother", "person_identifier",
                                                       "mariage_event", "role_bride_father", "divorce_event",
                                                       "location_region", "person_family_name", "location_municipality",
                                                       "role_deceased", "person_given_name", "event_registration_location",
                                                       "role_newborn", "role_partner", "location_province",
                                                       "role_father", "person", "role_groom_father",
                                                       "location", "role_bride", "event_registration_data",
                                                       "death_event", "event_registration_identifier");

    private String dataModelDir = "./res/data_models/";
    private String dataModelExt = "yaml";

	private String dataModel, function, inputDataset, outputDirectory;
	private int maxLev;
	private boolean fixedLev = false, ignoreDate = false, ignoreBlock = false, singleInd = false, outputFormatCSV = true, doubleInputs = false;

	public static final Logger lg = LogManager.getLogger(Controller.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	public Controller(String function, int maxlev, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd,
                      String inputDataset, String outputDirectory, String outputFormat, String dataModel) {
		this.function = function;
		this.maxLev = maxlev;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.inputDataset = inputDataset;
		this.outputDirectory = outputDirectory;

        this.dataModel = dataModel;
        if (!this.dataModel.endsWith('.' + dataModelExt)) {
            // assume shorthand format
            this.dataModel = dataModelDir + this.dataModel + '.' + dataModelExt;
        }

		if(!outputFormat.equals("CSV")) {
			outputFormatCSV = false;
		}
	}

	public void runProgram() {
		if(checkInputFunction() == true) {
            if (PROCESSES.contains(this.function)) {
                execProcess();
            } else {
                switch (this.function) {
                case "showdatasetstats":
                    if(checkInputDataset()) {
                        outputDatasetStatistics();
                    }

                    break;
                case "converttohdt":
                    if(checkInputDirectory()) {
                        convertToHDT(inputDataset);
                    }

                    break;
                }
            }
		}
	}

    public void execProcess() {
        // read data model specification from file
        Map<String, Map<String, String>> dataModelRaw = loadYamlFromFile(this.dataModel);
        if (dataModelRaw == null) {
            LOG.logError("execProcess", "Error reading data model");
            return;
        }

        // build and validate data model
        Map<String, String> dataModel = buildDataModel(dataModelRaw);
        if (!validateDataModel(dataModel)) {
            return;
        }

        Process process;
        switch (this.function) {
			case "within_b_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Within Births-Marriages (newborn -> bride/groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARIAGE,
                                          Process.RelationType.WITHIN,
                                          dataModel);
					Within(process);
					LOG.outputTotalRuntime("Within Births-Marriages (newborn -> bride/groom)", startTime, true);
				}

				break;
			case "within_b_d":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Within Births-Deaths (newborn -> deceased)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.WITHIN,
                                          dataModel);
					Within(process);
					LOG.outputTotalRuntime("Within Births-Deaths (newborn -> deceased)", startTime, true);
				}

				break;
			case "between_b_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Births-Marriages (newborn parents -> bride + groom)");

                    process = new Process(Process.ProcessType.BIRTH_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          dataModel);
					Between(process);
					LOG.outputTotalRuntime("Between Births-Marriages (newborn parents -> bride + groom)", startTime, true);
				}

				break;
			case "between_b_d":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Births-Deaths (parents of newborn -> deceased + partner)");

                    process = new Process(Process.ProcessType.BIRTH_DECEASED,
                                          Process.RelationType.BETWEEN,
                                          dataModel);
					Between(process);
					LOG.outputTotalRuntime("Between Births-Deaths (parents of newborn -> deceased + partner)", startTime, true);
				}

				break;
			case "between_d_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Deaths-Marriages (parents of deceased -> bride + groom)");

                    process = new Process(Process.ProcessType.DECEASED_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          dataModel);
					Between(process);
					LOG.outputTotalRuntime("Between Deaths-Marriages (parents of deceased -> bride + groom)", startTime, true);
				}

				break;
			case "between_m_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Marriages-Marriages (parents of bride/groom -> bride + groom)");

                    process = new Process(Process.ProcessType.MARIAGE_MARIAGE,
                                          Process.RelationType.BETWEEN,
                                          dataModel);
                    Between(process);
					LOG.outputTotalRuntime("Between Marriages-Marriages (parents of bride/groom -> bride + groom)", startTime, true);
				}

				break;
			case "closure":
				if(checkInputDataset() && checkInputDirectoryContents()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Computing the transitive closure");

                    process = new Process(dataModel);
					computeClosure(process);
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
        } catch (FileNotFoundException e) {
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

	// ========= input checks =========

	public Boolean checkAllUserInputs() {
		Boolean validInputs = true;

		validInputs = validInputs & checkInputDataset();
		validInputs = validInputs & checkInputDirectory();
		validInputs = validInputs & checkInputMaxLevenshtein();

		checkOutputFormatRDF();

		return validInputs;
	}

	public Boolean checkInputFunction() {
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

	public Boolean checkInputMaxLevenshtein() {
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

	public Boolean checkInputDataset() {
		if(inputDataset.contains(",")){
			String[] inputs = inputDataset.split(",");
			Boolean check = checkInputDataset(inputs[0]);
			check = check & checkInputDataset(inputs[1]);
			doubleInputs = true;

			return check;
		} else {
			return checkInputDataset(inputDataset);
		}
	}

	public Boolean checkInputDataset(String fileURL) {
		if(FILE_UTILS.checkIfFileExists(fileURL) == true) {
			LOG.logDebug("checkInputFileInput", "The following dataset is set as input dataset: " + inputDataset);

			return true;
		} else {
			String suggestedFix = "A valid HDT file, or two valid HDT files separated only by a comma (without a space) are required as input after parameter: --inputData " ;
			LOG.logError("checkInputFileInput", "Invalid or Missing user input for parameter: --inputData", suggestedFix);

			return false;
		}
	}

	public Boolean checkInputDirectory() {
		if(FILE_UTILS.checkIfDirectoryExists(outputDirectory)) {
			LOG.logDebug("checkInputDirectoryOutput", "The following directory is set to store results: " + outputDirectory);

			return true;
		} else {
			LOG.logError("checkInputDirectoryOutput", "Invalid or Missing user input for parameter: --outputDir", "A valid directory for storing links is required as input after parameter: --outputDir");

			return false;
		}
	}

	public Boolean checkInputDirectoryContents() {
		if(checkInputDirectory()) {
			if(FILE_UTILS.getAllValidLinksFile(outputDirectory, false) != null) {
				return true;
			}
		}

		return false;
	}


	public Boolean checkOutputFormatRDF() {
		if(outputFormatCSV == true) {
			LOG.logDebug("checkOutputFormatCSV", "Output format is set as CSV");

			return true;
		} else {
			LOG.logDebug("checkOutputFormatCSV", "Output format is set as RDF");

			return false;
		}
	}

	// ========= utilities =========

	public void convertToHDT(String s) {
		if(inputDataset.contains(",")){
			System.out.println("Input Dataset: " + inputDataset);
			String[] inputs = inputDataset.split(",");
			if(checkInputDataset(inputs[0]) && checkInputDataset(inputs[1])) {
				new MyHDT(inputs[0], inputs[1], outputDirectory);
			}
		}
		else {
			if(checkInputDataset()){
				new MyHDT(inputDataset, outputDirectory);
			}
		}
	}

	// ========= functions =========

	public void outputDatasetStatistics() {
		MyHDT myHDT;
		DecimalFormat formatter = new DecimalFormat("#,###");

		if(doubleInputs == true) {
			String[] inputs = inputDataset.split(",");
			myHDT = new MyHDT(inputs[0], inputs[1], doubleInputs);
		} else {
			myHDT = new MyHDT(inputDataset);
		}

		int numberOfBirthEvents = myHDT.getNumberOfSubjects(TYPE_BIRTH_EVENT);
		LOG.outputConsole("--- 	# Birth Events: " + formatter.format(numberOfBirthEvents) + " ---");
		int numberOfMarriageEvents = myHDT.getNumberOfSubjects(TYPE_MARRIAGE_EVENT);
		LOG.outputConsole("--- 	# Marriage Events: " + formatter.format(numberOfMarriageEvents) + " ---");
		int numberOfDeathEvents = myHDT.getNumberOfSubjects(TYPE_DEATH_EVENT);
		LOG.outputConsole("--- 	# Death Events: " + formatter.format(numberOfDeathEvents) + " ---");
		int numberOfIndividuals = myHDT.getNumberOfSubjects(TYPE_PERSON);
		LOG.outputConsole("--- 	# Individuals: " + formatter.format(numberOfIndividuals) + " ---");

		myHDT.closeDataset();
	}

	public void Within(Process process) {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory,
                                                                      DIRECTORY_NAME_DICTIONARY);
			boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory,
                                                                    DIRECTORY_NAME_DATABASE);
			boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory,
                                                                   DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Within(myHDT, process, mainDirectory, maxLev, fixedLev,
                           ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError(process.toString(), "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main output directory");
		}
	}

	public void Between(Process process) {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Between(myHDT, process, mainDirectory, maxLev, fixedLev,
                            ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError(process.toString(), "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError(process.toString(), "Error in creating the main output directory");
		}
	}

	public void computeClosure(Process process) {
		String dirName = function;

		boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Closure(myHDT, process, outputDirectory, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Closure", "Error in creating the main output directory");
			}
		}
	}
}
