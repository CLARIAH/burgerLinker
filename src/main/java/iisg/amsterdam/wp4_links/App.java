package iisg.amsterdam.wp4_links;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import iisg.amsterdam.wp4_links.utilities.LoggingUtilities;



public class App 
{
	@Parameter(names = "--function")
	String function = null;

	@Parameter(names = "--maxLev")
	int maxLev = 4;

	@Parameter(names = "--fixedLev")
	boolean fixedLev = false;

	@Parameter(names = "--bestLink")
	boolean bestLink = false; 

	@Parameter(names = "--inputData")
	String inputData = null;

	@Parameter(names = "--outputDir")
	String outputDir = null;

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
		LOG.outputConsole("PROGRAM STARTED!!");
		LOG.outputConsole("-----------------");
		LOG.outputConsole("");
		long startTime = System.currentTimeMillis();

		// default option is to show only errors 
		// BasicConfigurator.configure();
		Configurator.setRootLevel(Level.ERROR);


		if(help == false) {
			// show only error and warning logs if user enters: --debug warn
			if(debug.equals("warn")) { 
				Configurator.setRootLevel(Level.ERROR);
			}
			// show all type of logs if user enters: --debug all
			if(debug.equals("all")) { 
				Configurator.setRootLevel(Level.DEBUG);
			}
			Controller cntrl = new Controller(function, maxLev, fixedLev, bestLink, inputData, outputDir, format);
			cntrl.runProgram();
		} else { 
			// do not run program and show some help message if user enter: --help	
			String formatting =  "%-18s %15s %n";

			System.out.println("Parameters that can be provided as input to the linking tool:");
			System.out.printf(formatting, "--function:", "(required) One of the functionalities listed below");
			System.out.printf(formatting, "--inputData:", "(required) Path of the HDT dataset");
			System.out.printf(formatting, "--outputDir:", "(required) Path of the directory for saving the indices and the detected links");
			System.out.printf(formatting, "--maxLev:", "(optional, default = 4) Integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link");
			System.out.printf(formatting, "--fixedLev:", "(optional, default = False) Add this flag without a value (i.e. True) for applying the same maximum Levenshtein distance independently from the string lengths");
			System.out.printf(formatting, "--format:", "(optional, default = CSV) One of the two Strings: 'RDF' or 'CSV', indicating the desired format for saving the detected links between certificates");
			System.out.printf(formatting, "--debug:", "(optional, default = error) One of the two Strings: 'error' (only display error messages in console) or 'all' (show all warning in console)");
			System.out.println("\n");

			System.out.println("Functionalities that are supported in the current version: (case insensitive)");
			System.out.printf(formatting, "ConvertToHDT:", "Convert an RDF dataset (given as --inputData) to an HDT file generated in the same directory. This function can also be used for merging two HDT files into one (see Example 3 below).");
			System.out.printf(formatting, "ShowDatasetStats:", "Display some general stats about the HDT dataset (given as --inputData)");
			System.out.printf(formatting, "Within_B_M:", "Link newborns in Birth Certificates to brides/grooms in Marriage Certificates (reconstructs life course)");
			System.out.printf(formatting, "Between_B_M:", "Link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)");
			System.out.printf(formatting, "Between_M_M:", "Link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)");	
			System.out.printf(formatting, "Closure:", "Compute the transitive closure of all detected links to get a unique identifier per individual");	
			
			System.out.println("\n");
			System.out.println("------------------------");

			System.out.println("Example 1. Linking parents of newborns to brides and grooms:");
			System.out.println("--function Between_B_M --inputData dataDirectory/myData.hdt --outputDir . --format CSV  --maxLev 3 --fixedLev");
			System.out.println("\nThese arguments indicate that the user wants to:\n "
					+ "\t \t [Between_B_M] link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates,\n "
					+ "\t \t [dataDirectory/myData.hdt] described in the dataset myData.hdt (according to CLARIAH's civil registries RDF schema),\n "
					+ "\t \t [.] save the detected links in the current directory,\n "
					+ "\t \t [CSV] as a CSV file,\n "
					+ "\t \t [3] allowing a maximum Levenshtein of 3 per name (first name or last name),\n "
					+ "\t \t [fixedLev] independently from the length of the name.");

			System.out.println("\n");
			System.out.println("------------------------");

			System.out.println("Example 2. Generate an HDT file and its index from an RDF dataset:");
			System.out.println("--function ConvertToHDT --inputData dataDirectory/myData.nq --outputDir . ");
			System.out.println("\nThis will generate the HDT file 'myData.hdt' and its index 'myData.hdt.index' in the same directory."
					+ "\nThe index should be kept in the same directory of the HDT file to speed up all queries.");

			System.out.println("\n");
			System.out.println("------------------------");

			System.out.println("Example 3. Merge two HDT files into one:");
			System.out.println("--function ConvertToHDT --inputData dataDirectory/hdt1.hdt,dataDirectory/hdt2.hdt --outputDir . ");
			System.out.println("\nThis will generate a third HDT file 'merged-dataset.hdt' and its index 'merged-dataset.hdt.index' in the same directory."
					+ "\nThe two input HDT files are separated by a comma ',' without a space)");

			// Add example of computing the closure			
			
		}

		LOG.outputConsole("");
		LOG.outputConsole("-----------------");
		LOG.outputTotalRuntime("PROGRAM", startTime, true);	
	}



}
