package iisg.amsterdam.wp4_links.utilities;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUtilities {

	public static final Logger lg = LogManager.getLogger(FileUtilities.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);


	public Boolean checkIfFileExists(String path) {
		if(path != null) {
			File file = new File(path);
			if(!file.isDirectory()) {
				if(file.exists()) {
					return true;
				} else {
					LOG.logError("checkIfFileExists", "File does not exist");
				}
			} else {
				LOG.logError("checkIfFileExists", "A directory is chosen instead of a file");
			}
		} else {
			LOG.logError("checkIfFileExists", "No file path is specified");
		}
		return false;
	}


	public Boolean checkIfDirectoryExists(String path) {
		if(path != null) {
			File file = new File(path);
			if(file.isDirectory()) {
				return true;
			} else {
				LOG.logError("checkIfDirectoryExists", "Specified path is not a directory");
			}
		} else {
			LOG.logError("checkIfDirectoryExists", "No directory path is specified");
		}
		return false;
	}



	public Boolean createDirectory(String path, String directoryName) {
		try {
			File f = new File(path + "/" + directoryName);
			if(f.isDirectory()) {
				FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
				FileUtils.forceDelete(f); //delete directory
				FileUtils.forceMkdir(f); //create directory
			} else {
				FileUtils.forceMkdir(f); //create directory
			}		
			return true;
		} catch (IOException e) {
			LOG.logError("createDirectory", "Error creating directory " + path + "/" + directoryName);
			e.printStackTrace();
			return false;
		} 
	}

	public Boolean deleteFile(String filePath) {
		try {
			return Files.deleteIfExists(Paths.get(filePath));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}


	public BufferedOutputStream createFileStream(String path) throws IOException {
		try {
			FileOutputStream file = new FileOutputStream(path);
			BufferedOutputStream outStream = new BufferedOutputStream(file);
			LOG.logDebug("createFileStream", "File created successfully at: " + path) ;
			return outStream;
		} catch (IOException ex) {
			LOG.logError("createFileStream", "Error creating file stream");
			ex.printStackTrace();
			return null;
		}
	}



	public Boolean writeToOutputStream(BufferedOutputStream outStream, String message) {
		try {
			outStream.write(message.getBytes());
			outStream.write(System.lineSeparator().getBytes());
			return true;
		} catch (IOException e) {
			LOG.logError("writeToOutputStream", "Cannot write following message: " + message + " to stream: " + outStream);
			e.printStackTrace();
			return false;
		}
	}


	public int countLines(String filePath) {
		int countLines = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			while (reader.readLine() != null) countLines++;
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return countLines;
	}


	public ArrayList<String> getAllValidLinksFile(String directory, Boolean output) {
		ArrayList<String> consideredFiles = new ArrayList<String>();
		try (Stream<Path> walk = Files.walk(Paths.get(directory))) {
			List<String> result = walk.map(x -> x.toString()).filter(f -> f.endsWith(".csv")).collect(Collectors.toList());
			for(String fileName : result) {
				if(checkIfValidLinksFile(fileName)) {
					consideredFiles.add(fileName);
				}
			}
			if (!consideredFiles.isEmpty()) {
				if (output == true)
					LOG.outputConsole("Computing the transitive closure for the following files:");
				for (String consideredFile: consideredFiles) {
					if (output == true)
						LOG.outputConsole("\t" + consideredFile);
				}
				return consideredFiles;
			} else {
				LOG.logError("getAllValidLinksFile", "Missing a CSV file in the specified directory containing the detected links with the following format:"
						+ " (within|between)[-_][bmd][-_][bmd]");
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.logError("getAllValidLinksFile", "Missing a CSV file in the specified directory containing the detected links with the following format:"
					+ " (within|between)[-_][bmd][-_][bmd]");	
		}
		return null;
	}


	public String getFileName(String filePath) {
		Path path = Paths.get(filePath); 
		Path fileName = path.getFileName();
		return FilenameUtils.removeExtension(fileName.toString());
	}


	public Boolean checkIfValidLinksFile(String filePath) {
		Path fPath = Paths.get(filePath);
		String fileName = fPath.getFileName().toString();
		String fileNameLC = fileName.toLowerCase();
		Pattern p = Pattern.compile("(within|between)[-_][bmd][-_][bmd]");
		Matcher m = p.matcher(fileNameLC);
		return m.find();
	}

	public Boolean checkPattern(String name, String regex) {
		Path fPath = Paths.get(name);
		String fileName = fPath.getFileName().toString();
		String fileNameLC = fileName.toLowerCase();
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(fileNameLC);
		return m.find();
	}

	public Boolean check_Within_B_M(String filePath) {
		String regex = "(within)[-_](b)[-_](m)";
		return checkPattern(filePath, regex);
	}

	public Boolean check_Between_B_M(String filePath) {
		String regex = "(between)[-_](b)[-_](m)";
		return checkPattern(filePath, regex);
	}

	public Boolean check_Between_M_M(String filePath) {
		String regex = "(between)[-_](m)[-_](m)";
		return checkPattern(filePath, regex);
	}






}
