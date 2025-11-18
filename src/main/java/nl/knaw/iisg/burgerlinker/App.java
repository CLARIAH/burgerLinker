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
    private final String modelDefault = "CIV",  // use shorthand
                  rulesetDefault = "default",  // use shorthand
                  namespaceDefault = "_:";


	@Parameter(names = {"-f", "--function"}, required=false,
               description = """
               One of the functionalities listed below or all functions in sequence if omitted.

               FUNCTIONS:
               - Within_B_M:  Link newborns in Birth Certificates to brides/grooms in Marriage Certificates
               - Within_B_D:  Link newborns in Birth Certificates to deceased individuals in Death Certificates
               - Between_B_M: Link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates
               - Between_B_D: Link parents of newborns in Birth Certificates to deceased and their partner in Death Certificates
               - Between_M_M: Link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates
               - Between_D_M: Link parents of deceased in Death Certificates to brides and grooms in Marriage Certificates
                """)
	String function = null;

	@Parameter(names = {"-i", "--input"}, required=false,
               description="Comma-separated path(s) to one or more RDF graphs, or a web address to a SPARQL endpoint.")
	String input = null;

	@Parameter(names = {"-wd", "--workdir"}, required=true,
               description="Path of the directory for storing intermediate and final results.")
	String workdir = null;

    @Parameter(names = {"-m", "--model"}, required=false,
               description="Path to an appropriate data model specification (YAML) or its filename (shorthand). Defaults to " + modelDefault + ".")
    String model = modelDefault;

    @Parameter(names = {"-ns", "--namespace"}, required=false,
               description="Namespace to use for reconstructed individuals. Defaults to blank nodes: '_:'.")
    String namespace = namespaceDefault;

    @Parameter(names = {"-rs", "--ruleset"}, required=false,
               description="Path to a rule set definition (YAML) or its filename (shorthand). Defaults to " + rulesetDefault + ".")
    String ruleset = rulesetDefault;

    @Parameter(names = "--reload", required=false,
               description="Reload RDF data from graph(s) instead of reusing an existing RDF store.")
    boolean reload = false;

	@Parameter(names = "--maxLev", required=false,
               description="Integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link. Defaults to 4.")
	int maxLev = 4;

	@Parameter(names = "--fixedLev", required=false,
               description="Disable automatic adjustment of maximum Levenshtein distance to string length")
	boolean fixedLev = false;

	@Parameter(names = "--ignoreDate", required=false,
               description="Disable temporal validation checks between candidate links.")
	boolean ignoreDate = false;

	@Parameter(names = "--ignoreBlock", required=false)
	boolean ignoreBlock = false;

	@Parameter(names = "--singleInd", required=false,
               description="Link individuals by their names only.")
	boolean singleInd = false;

	@Parameter(names = "--format", required=false)
	String format = "CSV"; // or "RDF"

    @Parameter(names = "--query", required=false,
               description="Execute a custom SPARQL query on the RDF store and print the results.")
    String query = null;

	@Parameter(names = {"-h", "--help"}, required=false, help = true)
	boolean help;

	@Parameter(names = "--debug", required=false,
               description = "Enable debug messages.")
	boolean debug = false;


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

		Configurator.setRootLevel(Level.ERROR);
		if(help == false) {
			// show all type of logs
			if (debug) {
				Configurator.setRootLevel(Level.DEBUG);
			}

			Controller cntrl = new Controller(function, maxLev, fixedLev, ignoreDate,
                                              ignoreBlock, singleInd, input, workdir,
                                              format, model, ruleset, namespace, query,
                                              reload, debug);
			cntrl.runProgram();
		} else {
			// do not run program and show some help message if user enter: --help
			String formatting =  "%-18s %15s %n";

			System.out.println("Parameters that can be provided as input to the linking tool:");
			System.out.printf(formatting, "-i, --input:", "(optional) Comma-separated path to one or more RDF graphs, or a web address to a SPARQL endpoint.");
			System.out.printf(formatting, "-wd, --workdir:", "(required) Path of the directory for storing intermediate and final results.");
			System.out.printf(formatting, "-f, --function:", "(optional) One of the functionalities listed below, or all functions in sequence if omitted.");
			System.out.printf(formatting, "-m, --model:", "(optional) Path to an appropriate data model specification (YAML) or its filename (shorthand). Defaults to CIV.");
            System.out.printf(formatting, "-rs, --ruleSet:", "(optional) Path to a rule set definition (YAML) or its filename (shorthand). Defaults to default.");
			System.out.printf(formatting, "-ns, --namespace:", "(optional) Namespace to use for reconstructed individuals. Defaults to blank nodes: '_:'.");
			System.out.printf(formatting, "--maxLev:", "(optional, default = 4) Integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link");
			System.out.printf(formatting, "--fixedLev:", "(optional, default = False) Add this flag without a value (i.e. True) for applying the same maximum Levenshtein distance independently from the string lengths");
			System.out.printf(formatting, "--format:", "(optional, default = CSV) One of the two Strings: 'RDF' or 'CSV', indicating the desired format for saving the detected links between certificates");
			System.out.printf(formatting, "--reload:", "(optional) Reload RDF data from graph(s) instead of reusing an existing RDF store.");
			System.out.printf(formatting, "--query:", "(optional) Execute a custom SPARQL query on the RDF store and print the results.");
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
