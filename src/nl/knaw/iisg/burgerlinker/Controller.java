package nl.knaw.iisg.burgerlinker;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nl.knaw.iisg.burgerlinker.processes.*;
import nl.knaw.iisg.burgerlinker.utilities.*;


public class Controller {
	@SuppressWarnings("unused")
	final private String[] FUNCTIONS_not_implemented = {"within_m_d", "within_b_b", "within_m_m", "within_d_d"};
	final private String[] FUNCTIONS = {"showDatasetStats", "convertToHDT", "closure", "within_b_m", "within_b_d",
			"between_b_m", "between_m_m", "between_d_m", "between_b_d"};
	private String function, inputDataset, outputDirectory;
	private int maxLev;
	private boolean fixedLev = false, ignoreDate = false, ignoreBlock = false, singleInd = false, outputFormatCSV = true, doubleInputs = false;

	public static final Logger lg = LogManager.getLogger(Controller.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	public Controller(String function, int maxlev, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd, String inputDataset, String outputDirectory, String outputFormat) {
		this.function = function;
		this.maxLev = maxlev;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.inputDataset = inputDataset;
		this.outputDirectory = outputDirectory;

		if(!outputFormat.equals("CSV")) {
			outputFormatCSV = false;
		}
	}

	public void runProgram() {
		if(checkInputFunction() == true) {
			switch (function) {
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
			case "within_b_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Within Births-Marriages (newborn -> bride/groom)");
					Within_B_M();
					LOG.outputTotalRuntime("Within Births-Marriages (newborn -> bride/groom)", startTime, true);
				}

				break;
			case "within_b_d":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Within Births-Deaths (newborn -> deceased)");
					Within_B_D();
					LOG.outputTotalRuntime("Within Births-Deaths (newborn -> deceased)", startTime, true);
				}

				break;
			case "between_b_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Births-Marriages (newborn parents -> bride + groom)");
					Between_B_M();
					LOG.outputTotalRuntime("Between Births-Marriages (newborn parents -> bride + groom)", startTime, true);
				}

				break;
			case "between_b_d":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Births-Deaths (parents of newborn -> deceased + partner)");
					Between_B_D();
					LOG.outputTotalRuntime("Between Births-Deaths (parents of newborn -> deceased + partner)", startTime, true);
				}

				break;
			case "between_d_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Deaths-Marriages (parents of deceased -> bride + groom)");
					Between_D_M();
					LOG.outputTotalRuntime("Between Deaths-Marriages (parents of deceased -> bride + groom)", startTime, true);
				}

				break;
			case "between_m_m":
				if(checkAllUserInputs()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Between Marriages-Marriages (parents of bride/groom -> bride + groom)");
					Between_M_M();
					LOG.outputTotalRuntime("Between Marriages-Marriages (parents of bride/groom -> bride + groom)", startTime, true);
				}

				break;
			case "closure":
				if(checkInputDataset() && checkInputDirectoryContents()) {
					long startTime = System.currentTimeMillis();
					LOG.outputConsole("START: Computing the transitive closure");
					computeClosure();
					LOG.outputTotalRuntime("Computing the transitive closure", startTime, true);
				}

				break;
			default:
				LOG.logError("runProgram", "User input is correct, but no corresponding function exists (error in code)");

				break;
			}
		}
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
					"Choose one of the following options: " + Arrays.toString(FUNCTIONS));
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
					"Choose one of the following options: " + Arrays.toString(FUNCTIONS));
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

	public void Within_B_M() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Within_B_M(myHDT, mainDirectory, maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Within_B_M", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Within_B_M", "Error in creating the main output directory");
		}
	}

	public void Within_B_D() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Within_B_D(myHDT, mainDirectory, maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Within_B_D", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Within_B_D", "Error in creating the main output directory");
		}
	}

	public void Between_B_M() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Between_B_M(myHDT, mainDirectory, maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Between_B_M", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Between_B_M", "Error in creating the main output directory");
		}
	}

	public void Between_B_D() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Between_B_D(myHDT, mainDirectory, maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Between_B_D", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Between_B_D", "Error in creating the main output directory");
		}
	}


	public void Between_D_M() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Between_D_M(myHDT, mainDirectory, maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Between_D_M", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Between_D_M", "Error in creating the main output directory");
		}
	}

	public void Between_M_M() {
		String options = LOG.getUserOptions(maxLev, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String dirName = function + options;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean dictionaryDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DICTIONARY);
			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(dictionaryDirCreated && databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Between_M_M(myHDT, mainDirectory,  maxLev, fixedLev, ignoreDate, ignoreBlock, singleInd, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Between_M_M", "Error in creating the three sub output directories");
			}
		} else {
			LOG.logError("Between_M_M", "Error in creating the main output directory");
		}
	}

	public void computeClosure() {
		String dirName = function;

		Boolean processDirCreated =  FILE_UTILS.createDirectory(outputDirectory, dirName);
		if(processDirCreated == true) {
			String mainDirectory = outputDirectory + "/" + dirName;

			Boolean databaseDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_DATABASE);
			Boolean resultsDirCreated = FILE_UTILS.createDirectory(mainDirectory, DIRECTORY_NAME_RESULTS);
			if(databaseDirCreated && resultsDirCreated) {
				MyHDT myHDT = new MyHDT(inputDataset);

				new Closure(myHDT, outputDirectory, outputFormatCSV);

				myHDT.closeDataset();
			} else {
				LOG.logError("Closure", "Error in creating the main output directory");
			}
		}
	}
}
