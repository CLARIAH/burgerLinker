package nl.knaw.iisg.burgerlinker;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;

import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class App {
    // default arguments
    private final String modelDefault = "CIV",  // use shorthand
                  rulesetDefault = "default",  // use shorthand
                  namespaceDefault = "_:";


	@Parameter(names = {"-f", "--function"}, required=false,
               description = "One of the functionalities listed below or all functions in sequence if omitted.\n"
                           + "\t  FUNCTIONS:\n"
                           + "\t  - Within_B_M:  Link newborns in Birth Certificates to brides/grooms in Marriage Certificates\n"
                           + "\t  - Within_B_D:  Link newborns in Birth Certificates to deceased individuals in Death Certificates\n"
                           + "\t  - Between_B_M: Link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates\n"
                           + "\t  - Between_B_D: Link parents of newborns in Birth Certificates to deceased and their partner in Death Certificates\n"
                           + "\t  - Between_M_M: Link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates\n"
                           + "\t  - Between_D_M: Link parents of deceased in Death Certificates to brides and grooms in Marriage Certificates\n"
                           + "\t  - Closure:     Compute the transitive closure between the found links. The output is a set of reconstructed individuals")
	String function = null;

	@Parameter(names = {"-i", "--input"}, required=false,
               description="[OPTIONAL] Comma-separated path(s) to one or more RDF graphs, or a web address to a SPARQL endpoint.")
	String input = null;

	@Parameter(names = {"-wd", "--workdir"}, required=true,
               description="[REQUIRED] Path of the directory for storing intermediate and final results.")
	String workdir = null;

    @Parameter(names = {"-m", "--model"}, required=false,
               description="[OPTIONAL] Path to an appropriate data model specification (YAML) or its filename (shorthand). Defaults to " + modelDefault + ".")
    String model = modelDefault;

    @Parameter(names = {"-ns", "--namespace"}, required=false,
               description="[OPTIONAL] Namespace to use for reconstructed individuals. Defaults to blank nodes: '" + namespaceDefault +"'.")
    String namespace = namespaceDefault;

    @Parameter(names = {"-rs", "--ruleset"}, required=false,
               description="[OPTIONAL] Path to a rule set definition (YAML) or its filename (shorthand). Defaults to " + rulesetDefault + ".")
    String ruleset = rulesetDefault;

    @Parameter(names = "--reload", required=false,
               description="[OPTIONAL] Reload RDF data from graph(s) instead of reusing an existing RDF store.")
    boolean reload = false;

	@Parameter(names = "--max-lev", required=false,
               description="[OPTIONAL] Integer between 0 and 4 (default) indicating the maximum Levenshtein distance per first or last name allowed for accepting a link.")
	int maxLev = 4;

	@Parameter(names = "--fixed-lev", required=false,
               description="[OPTIONAL] Disable automatic adjustment of maximum Levenshtein distance to string length.")
	boolean fixedLev = false;

	@Parameter(names = "--ignore-date", required=false,
               description="[OPTIONAL] Disable temporal validation checks between candidate links.")
	boolean ignoreDate = false;

	@Parameter(names = "--ignore-block", required=false,
               description="[OPTIONAL] Disable filtering on first letter of family name prior to lexical comparison.")
	boolean ignoreBlock = false;

	@Parameter(names = "--ignore-relations", required=false,
               description="[OPTIONAL] Disable lexical comparison of related individuals (eg, parents of subject).")
	boolean singleInd = false;

	@Parameter(names = "--format", required=false,
               description="[OPTIONAL] Store the intermediate results as CSV (default) or RDF")
	String format = "CSV"; // or "RDF"

    @Parameter(names = "--query", required=false,
               description="[OPTIONAL] Execute a custom SPARQL query on the RDF store and print the results.")
    String query = null;

	@Parameter(names = {"-h", "--help"}, required=false, help = true,
               description="[OPTIONAL] Print this help and exit.")
	boolean help;

	@Parameter(names = "--debug", required=false,
               description = "[OPTIONAL] Enable debug messages.")
	boolean debug = false;


	public static final Logger lg = LogManager.getLogger(App.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	public static void main(String[] argv) {
		App main = new App();
		JCommander jc = JCommander.newBuilder().addObject(main).build();
		jc.parse(argv);
		main.run(jc);
	}

	public void run(JCommander jc) {
		Configurator.setRootLevel(Level.ERROR);
        if (help) {
            LOG.outputConsole("USAGE  : java -jar burgerlinker<VERSION>.jar [OPTIONS]");
            LOG.outputConsole("OPTIONS:");
            for (ParameterDescription param: jc.getParameters()) {
                LOG.outputConsole("\t" + param.getNames());
                LOG.outputConsole("\t  " + param.getDescription());
            }
            LOG.outputConsole("WWW    : www.github.com/CLARIAH/burgerLinker");
        } else {
            LOG.outputConsole(".: Welcome to BurgerLinker");
            LOG.outputConsole(".: Documentation is available at www.github.com/CLARIAH/burgerLinker");

			// show all type of logs
			if (debug) {
				Configurator.setRootLevel(Level.DEBUG);
			}

			Controller cntrl = new Controller(function, maxLev, fixedLev, ignoreDate,
                                              ignoreBlock, singleInd, input, workdir,
                                              format, model, ruleset, namespace, query,
                                              reload, debug);
			cntrl.runProgram();
		}
	}
}
