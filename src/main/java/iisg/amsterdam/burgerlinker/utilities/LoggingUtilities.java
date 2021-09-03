package iisg.amsterdam.burgerlinker.utilities;

import org.apache.logging.log4j.Logger;

public final class LoggingUtilities {

	private Logger LOG = null;


	public LoggingUtilities(Logger LOG) {
		this.LOG = LOG;
	}


	public void outputConsole(String message) {
		System.out.println(message);
	}

	public void logDebug(String function, String message) {
		LOG.debug("\n	FUNCTION -> " + function
				+ "\n	DEBUG -> " + message
				+ "\n -----");
	}


	public void logWarn(String function, String message) {
		LOG.warn( "\n	FUNCTION -> " + function
				+ "\n	WARNING -> " + message 
				+ "\n -----");
	}

	public void logWarn(String function, String message, String suggestion) {
		LOG.warn( "\n	FUNCTION -> " + function
				+ "\n	WARNING -> " + message
				+ "\n	FIX --> " + suggestion
				+ "\n -----");
	}


	public void logError(String function, String message) {
		LOG.error("\n	FUNCTION -> " + function
				+ "\n	ERROR -> " + message
				+ "\n -----");
	}

	public void logError(String function, String message, String suggestion) {
		LOG.error("\n	FUNCTION -> " + function
				+ "\n	ERROR -> " + message
				+ "\n	FIX -> " + suggestion
				+ "\n -----");
	}
	
	public String outputTotalRuntime(String processName, long startTime, Boolean output) {
		long endTime = System.currentTimeMillis();
		double totalTime = (endTime - startTime) / 1000.0;
		double rounded_totalTime = Math.round(totalTime * 100.0) / 100.0;
		double totalTime_minutes = totalTime / 60.0;
		double rounded_totalTime_minutes = Math.round(totalTime_minutes * 100.0) / 100.0;
		String message = "*DONE: " + processName;
		if(rounded_totalTime > 1) {
			if(rounded_totalTime_minutes > 1) {
				 message =  message + " [runtime: " + rounded_totalTime + " s / " + rounded_totalTime_minutes + " m)]";
			} else {
				 message = message + " [runtime: " + rounded_totalTime + " s]";
			}
		}
		if(output == true) {
			outputConsole(message);
		}
		return message;
	}
	
	
	public String getUserOptions(Integer maxLev, Boolean fixedLev, Boolean singleInd, Boolean ignoreDate, Boolean ignoreBlock) {
		String options = "-maxLev-" + maxLev;
		if(fixedLev == true) {
			options =  options + "-fixed";
		}
		if(singleInd == true) {
			options =  options + "-singleInd";
		}
		if(ignoreDate == true) {
			options =  options + "-ignoreDate";
		}
		if(ignoreBlock == true) {
			options =  options + "-ignoreBlock";
		} 
		return options;
	}
	
	


}
