package nl.knaw.iisg.burgerlinker;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class App {
    // default arguments
    private String modelDefault = "CIV",  // use shorthand
                   rulesetDefault = "default",  // use shorthand
                   namespaceDefault = "_:";


	@Parameter(names = "--function")
	String function = null;

	@Parameter(names = "--input")
	String input = null;

	@Parameter(names = "--output")
	String output = null;

    @Parameter(names = "--model")
    String model = modelDefault;

    @Parameter(names = "--namespace")
    String namespace = namespaceDefault;

    @Parameter(names = "--ruleset")
    String ruleset = rulesetDefault;

    @Parameter(names = "--reload")
    boolean reload = false;

	@Parameter(names = "--maxLev")
	int maxLev = 4;

	@Parameter(names = "--fixedLev")
	boolean fixedLev = false;

	@Parameter(names = "--ignoreDate")
	boolean ignoreDate = false;

	@Parameter(names = "--ignoreBlock")
	boolean ignoreBlock = false;

	@Parameter(names = "--singleInd")
	boolean singleInd = false;

	@Parameter(names = "--format")
	String format = "CSV"; // or "RDF"

	@Parameter(names = "--help", help = true)
	boolean help;

	@Parameter(names = "--debug")
	String debug = "error";


	public static final Logger lg = LogManager.getLogger(App.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	public static void main(String[] argv) {
		App main = new App();
		JCommander.newBuilder()
		.addObject(main)
		.build()
		.parse(argv);
		main.run();
	}

	public void run() {
		LOG.outputConsole(".: Welcome to BurgerLinker");
		LOG.outputConsole(".: Documentation is available at www.github.com/CLARIAH/burgerLinker");
		long startTime = System.currentTimeMillis();


	//	// BasicConfigurator.configure();
	//	ClassLoader.getSystemResource("/res/log4j.properties");

	//	// default option is to show only errors
	//	Configurator.setRootLevel(Level.ERROR);
	//	Configurator.setAllLevels("com.github.liblevenshtein", Level.OFF);
	//	Configurator.setAllLevels("com.github.liblevenshtein.transducer.factory.TransducerBuilder", Level.OFF);


		if(help == false) {
			// show only error and warning logs if user enters: --debug warn
			if(debug.equals("warn")) {
				Configurator.setRootLevel(Level.ERROR);
			}
			// show all type of logs if user enters: --debug all
			if(debug.equals("all")) {
				Configurator.setRootLevel(Level.DEBUG);
			}
			Controller cntrl = new Controller(function, maxLev, fixedLev, ignoreDate,
                                              ignoreBlock, singleInd, input, output,
                                              format, model, ruleset, namespace,
                                              reload);
			cntrl.runProgram();
		} else {
			// do not run program and show some help message if user enter: --help
			String formatting =  "%-18s %15s %n";

			System.out.println("Parameters that can be provided as input to the linking tool:");
			System.out.printf(formatting, "--input:", "(required) Comma-separated path to one or more RDF graphs, or a web address to a SPARQL endpoint.");
			System.out.printf(formatting, "--output:", "(required) Path of the directory for saving the indices and the detected links");
			System.out.printf(formatting, "--function:", "(optional) One of the functionalities listed below, or all functions in sequence if omitted.");
			System.out.printf(formatting, "--model:", "(optional) Path to an appropriate data model specification (YAML) or its filename (shorthand). Defaults to CIV.");
            System.out.printf(formatting, "--ruleSet:", "(optional) Path to a rule set definition (YAML) or its filename (shorthand). Defaults to default.");
			System.out.printf(formatting, "--namespace:", "(optional) Namespace to use for reconstructed individuals. Defaults to blank nodes: '_:'.");
			System.out.printf(formatting, "--maxLev:", "(optional, default = 4) Integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link");
			System.out.printf(formatting, "--fixedLev:", "(optional, default = False) Add this flag without a value (i.e. True) for applying the same maximum Levenshtein distance independently from the string lengths");
			System.out.printf(formatting, "--format:", "(optional, default = CSV) One of the two Strings: 'RDF' or 'CSV', indicating the desired format for saving the detected links between certificates");
			System.out.printf(formatting, "--reload:", "(optional) Reload RDF data from graph(s) instead of reusing an existing RDF store.");
			System.out.printf(formatting, "--debug:", "(optional, default = error) One of the two Strings: 'error' (only display error messages in console) or 'all' (show all warning in console)");
			System.out.println("\n");

			System.out.println("Functionalities that are supported in the current version: (case insensitive)");
			System.out.printf(formatting, "Within_B_M:", "Link newborns in Birth Certificates to brides/grooms in Marriage Certificates (reconstructs life course)");
			System.out.printf(formatting, "Within_B_D:", "Link newborns in Birth Certificates to deceased individuals in Death Certificates (reconstructs life course)");
			System.out.printf(formatting, "Between_B_M:", "Link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)");
			System.out.printf(formatting, "Between_B_D:", "Link parents of newborns in Birth Certificates to deceased and their partner in Death Certificates (reconstructs family ties)");
			System.out.printf(formatting, "Between_M_M:", "Link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)");
			System.out.printf(formatting, "Between_D_M:", "Link parents of deceased in Death Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)");
			System.out.printf(formatting, "Closure:", "Compute the transitive closure of all detected links to get a unique identifier per individual");

			System.out.println("\n");
			System.out.println("------------------------");

			System.out.println("Example 1. Linking parents of newborns to brides and grooms:");
			System.out.println("--function Between_B_M --inputData data/myData.nq --output out/ --format CSV  --maxLev 3 --fixedLev");
			System.out.println("\nThese arguments indicate that the user wants to:\n "
					+ "\t \t [Between_B_M] link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates,\n "
					+ "\t \t [data/myData.nq] described in the dataset myData.nq,\n "
					+ "\t \t [.] save the detected links in the current directory,\n "
					+ "\t \t [CSV] as a CSV file,\n "
					+ "\t \t [3] allowing a maximum Levenshtein of 3 per name (first name or last name),\n "
					+ "\t \t [fixedLev] independently from the length of the name.");

			System.out.println("\n");
			System.out.println("------------------------");

			System.out.println("Example 2. Family Reconstruction:");
			System.out.println("--function closure --inputData data/myDataPart1.nq,data/myDataPart2.nq --outputDir myResultsDirectory ");
			System.out.println("\nThis command computes the transitive closure of all links existing in the directory myResultsDirectory, and generates a new finalDataset.nt.gz dataset in this directory "
					+ "\n by replacing all matched individuals' identifiers from the input datasets with the same unique identifier)");

			System.out.println("\n");
			System.out.println("------------------------");


			System.out.println("For further details, visit https://github.com/CLARIAH/burgerLinker");
		}
	}
}
