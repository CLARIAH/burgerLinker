package nl.knaw.iisg.burgerlinker.processes;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import nl.knaw.iisg.burgerlinker.Index;
import nl.knaw.iisg.burgerlinker.LinksCSV;
import nl.knaw.iisg.burgerlinker.MyHDT;
import nl.knaw.iisg.burgerlinker.Person;
import nl.knaw.iisg.burgerlinker.Properties;
import nl.knaw.iisg.burgerlinker.utilities.*;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


// Between_M_M: link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)
public class Closure {
	// output directory specified by the user + name of the called function
	private String inputDirectoryPath, outputDirectoryPath;
	private MyHDT myHDT;
	private final int updateInterval = 5000;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Closure.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

    LinksCSV LINKS;
	String dirClassToIndivs, dirIndivToClass;

    HashMap<String, HashSet<String>> dbClassToIndivs;
	HashMap<String, String> dbIndivToClass;

    String namespacePerson = "<https://iisg.amsterdam/links/person/";

	public Closure(MyHDT hdt, String directoryPath, Boolean formatCSV) {
		this.inputDirectoryPath = directoryPath;
		this.outputDirectoryPath = directoryPath + "/closure";
		this.myHDT = hdt;

		String resultsFileName = "links-individuals";
		if(formatCSV == true) {
			String header = "";
			LINKS = new LinksCSV(resultsFileName, outputDirectoryPath, header);

			this.dirClassToIndivs = outputDirectoryPath + "/" + DIRECTORY_NAME_DATABASE + "/ClassToIndivs";
			this.dirIndivToClass = outputDirectoryPath + "/" + DIRECTORY_NAME_DATABASE + "/IndivToClass";
		}

		computeClosure();
	}

	public void computeClosure() {
		Boolean success = false;

		try {
			ArrayList<String> linkFiles = FILE_UTILS.getAllValidLinksFile(inputDirectoryPath, false);

			for (String linkFile : linkFiles) {
				success = success | saveIndividualLinksToFile(linkFile);
			}
			LOG.outputConsole("");

			if(success) {
				String sortedFile = sortFile(LINKS.getLinksFilePath());

				transitiveClosure(sortedFile);
				verifyClosure();
				saveClosureToFile();
				reconstructDataset();
		//		FILE_UTILS.deleteFile(sortedFile);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void reconstructDataset() {
		try {
			String taskName = "Reconstructing dataset after transitive closure";
			int cntAll =0 ;
			// iterate through the marriage certificates to link it to the marriage dictionaries
			IteratorTripleString it = myHDT.dataset.search("", "", "");

			long estNumber = it.estimatedNumResults();
			LOG.outputConsole("Estimated number of certificates to be processed is: " + estNumber);

			LinksCSV datasetAfterClosure = new LinksCSV("finalDataset", outputDirectoryPath, true);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(estNumber)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();
			try {
				while(it.hasNext()) {
					TripleString ts = it.next();
					cntAll++;
					// skip blank nodes
					if(!ts.getSubject().toString().startsWith("_")) {
						String sbjPersonID = getEqClassOfPerson(ts.getSubject().toString());
						String predicate = "<" + ts.getPredicate().toString() + ">";
						String objPersonID = getEqClassOfPerson(ts.getObject().toString());

						if(predicate.contains("age")) {
							String ageInString = myHDT.getStringValueFromLiteral(objPersonID);
							int age = Integer.valueOf(ageInString);

							String birthYear = myHDT.getBirthYearFromAge(ts.getSubject().toString(), age);
							if(birthYear != null) {
								datasetAfterClosure.addToStream(sbjPersonID + " <" + Properties.BIRTH_YEAR + "> " + birthYear + ".");
							}
						} else {
							datasetAfterClosure.addToStream(sbjPersonID + " " + predicate + " " + objPersonID + ".");
						}
					}
					if(cntAll % 10000 == 0) {
						pb.stepBy(10000);
					}
				} pb.stepTo(estNumber);
			} finally {
				pb.close();
				datasetAfterClosure.closeStream();
			}
		} catch (Exception e) {
			LOG.logError("reconstructDataset", "Error in reconstructing the dataset after transitive closure");
			e.printStackTrace();
		} finally {
			LINKS.closeStream();
		}
	}

	public String getEqClassOfPerson(String someURI) {
		String personID = myHDT.getIDofPerson(someURI);

		if(personID != null) {
			String eqClass = dbIndivToClass.get(personID);
			if(eqClass != null) {
				eqClass = namespacePerson + eqClass + ">";
				// linked person
				return eqClass;
			}
		}
		// not linked
		if(someURI.startsWith("http")) {
			// URI but not person
			return "<" + someURI + ">";
		} else {
			// literal
			return someURI;
		}
	}

	public String sortFile(String filePath) {
		return filePath;
	}

	public void createDB() {
		dbClassToIndivs = new HashMap<String, HashSet<String>>();
		dbIndivToClass = new HashMap<String, String>();
	}

	public void verifyClosure() {
		int countIndivsClassToIndivs = 0, max = 0;
		for(Entry<String, HashSet<String>> entry: dbClassToIndivs.entrySet()) {
			int n = entry.getValue().size();
			countIndivsClassToIndivs = countIndivsClassToIndivs + n;
			if(n > max) {
				max = n;
			}
		}

		LOG.outputConsole("Number of individuals from Invidual to Class: " + dbIndivToClass.size());
		LOG.outputConsole("Number of individuals from Class to Individuals: " + countIndivsClassToIndivs);
		LOG.outputConsole("Number of max individuals in Eq Class: " + max);
	}

	public void saveClosureToFile() {
		LOG.outputConsole("Saving closure results to file...");

		LinksCSV closureTerms = new LinksCSV("closureTerms", outputDirectoryPath, "");
		LinksCSV closureDist = new LinksCSV("closureSizeDist", outputDirectoryPath, "");
		for(Entry<String, HashSet<String>> entry: dbClassToIndivs.entrySet()) {
			String lineTerms = entry.getKey();

			for(String val : entry.getValue()) {
				lineTerms = lineTerms + "," + val;
			}

			closureTerms.addToStream(lineTerms);
			closureDist.addToStream(Integer.toString(entry.getValue().size()));
		}

		LOG.outputConsole("Finished saving closure results to file!");
		closureTerms.closeStream();
		closureDist.closeStream();
	}

	public void transitiveClosure(String linksFilePath) {
		try {
			String taskName = "Closure";
			LOG.outputConsole("Starting: " + taskName + " of links in file: " + linksFilePath);
			LOG.outputConsole("");

            createDB();

			String [] nextLine;
			int nbLines = FILE_UTILS.countLines(linksFilePath) , eqID = 0 ;

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(linksFilePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					try {
						String idInvidual1 = nextLine[0];
						String idInvidual2 = nextLine[1];

						String eqIDIndividual1 = dbIndivToClass.get(idInvidual1);
						String eqIDIndividual2 = dbIndivToClass.get(idInvidual2);
						if(eqIDIndividual1 == null) {
							if(eqIDIndividual2 == null) {
								String newEqID = "i-" + eqID;
								eqID++;

								HashSet<String> eqValues = new HashSet<String>();

								eqValues.add(idInvidual1);
								eqValues.add(idInvidual2);

								dbClassToIndivs.put(newEqID, eqValues);
								dbIndivToClass.put(idInvidual1, newEqID);
								dbIndivToClass.put(idInvidual2, newEqID);
							} else {
								addObjectToExistingSet(dbClassToIndivs, eqIDIndividual2, idInvidual1);
								dbIndivToClass.put(idInvidual1, eqIDIndividual2);
							}
						} else {
							if(eqIDIndividual2 == null) {
								addObjectToExistingSet(dbClassToIndivs, eqIDIndividual1, idInvidual2);
								dbIndivToClass.put(idInvidual2, eqIDIndividual1);
							} else {
								if(!eqIDIndividual1.equals(eqIDIndividual2)) {
									HashSet<String> valuesEqIDIndividual1 = dbClassToIndivs.get(eqIDIndividual1);
									HashSet<String> valuesEqIDIndividual2 = dbClassToIndivs.get(eqIDIndividual2);
									if(valuesEqIDIndividual1.size() > valuesEqIDIndividual2.size()) {
										mergeEqSets(eqIDIndividual1, valuesEqIDIndividual1, eqIDIndividual2, valuesEqIDIndividual2);
									} else {
										mergeEqSets(eqIDIndividual2, valuesEqIDIndividual2, eqIDIndividual1, valuesEqIDIndividual1);
									}
								}
							}
						}
						if(countProgress % 1000 == 0) {
							pb.stepBy(1000);
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}
	}

	public void addObjectToExistingSet(HashMap<String, HashSet<String>> db, String key, String object) {
		HashSet<String> X = db.get(key);
		X.add(object);
		db.put(key, X);
	}

	public void mergeEqSets(String eqIDIndividualLarger, HashSet<String> valuesLarger, String eqIDIndividualSmaller, HashSet<String> valuesSmaller) {
		valuesLarger.addAll(valuesSmaller);
		dbClassToIndivs.put(eqIDIndividualLarger, valuesLarger);
		for(String valueEq2: valuesSmaller) {
			dbIndivToClass.put(valueEq2, eqIDIndividualLarger);
		}
		dbClassToIndivs.remove(eqIDIndividualSmaller);
	}

	public Boolean saveIndividualLinksToFile(String filePath) {
		Boolean success = true;
		if(FILE_UTILS.check_Within_B_M(filePath)) {
			success = success & saveLinksIndividuals_Within_B_M(filePath);
		}
		if(FILE_UTILS.check_Within_B_D(filePath)) {
			success = success & saveLinksIndividuals_Within_B_D(filePath);
		}
		if(FILE_UTILS.check_Between_B_M(filePath)) {
			success = success & saveLinksIndividuals_Between_B_M(filePath);
		}
		if(FILE_UTILS.check_Between_D_M(filePath)) {
			success = success & saveLinksIndividuals_Between_D_M(filePath);
		}
		if(FILE_UTILS.check_Between_B_D(filePath)) {
			success = success & saveLinksIndividuals_Between_B_D(filePath);
		}
		if(FILE_UTILS.check_Between_M_M(filePath)) {
			success = success & saveLinksIndividuals_Between_M_M(filePath);
		}
		LINKS.flushLinks();

		return success;
	}

	public Boolean saveLinksIndividuals_Within_B_M(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Within_B_M";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;

			String linktype = "sameAs", linkProv = "W_B_M";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names
				int countProgress = 1;

				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					int matchedIndiv = 1;
					Boolean fatherMatched = false,  motherMatched = false;

					String idBirth = nextLine[0];
					String idMarriage = nextLine[1];
					String familyLine = nextLine[2];
					String idNewborn = myHDT.getPersonID(idBirth, ROLE_NEWBORN);
					String idPartner = myHDT.getPersonID(idMarriage, ROLE_BRIDE, ROLE_GROOM, familyLine);
					String idFatherNewborn = null, idFatherPartner = null, idMotherNewborn = null, idMotherPartner = null;
					if(!nextLine[7].equals("N.A")) { // if there is a match for the fathers
						idFatherNewborn = myHDT.getPersonID(idBirth, ROLE_FATHER);

						if(idFatherNewborn != null) {
							idFatherPartner = myHDT.getPersonID(idMarriage, ROLE_BRIDE_FATHER, ROLE_GROOM_FATHER, familyLine);
							if(idFatherPartner != null) {
								matchedIndiv++;

								fatherMatched = true;
							}
						}
					}
					if(!nextLine[5].equals("N.A")) { // if there is a match for the mothers
						idMotherNewborn = myHDT.getPersonID(idBirth, ROLE_MOTHER);
						if(idMotherNewborn != null) {
							idMotherPartner = myHDT.getPersonID(idMarriage, ROLE_BRIDE_MOTHER, ROLE_GROOM_MOTHER, familyLine);

							if(idMotherPartner != null) {
								matchedIndiv++;
								motherMatched = true;
							}
						}
					}
					String meta_newborn = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idMarriage + ","
							+ nextLine[3] + "," + nextLine[4] + ","  + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[18];
					LINKS.saveIndividualLink(idNewborn, idPartner, meta_newborn);

					if(fatherMatched) {
						String meta_fathers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idMarriage + ","
								+ nextLine[7] + "," + nextLine[8] + "," + nextLine[13] + "," + nextLine[14] + "," + nextLine[15] + "," + nextLine[18];
						LINKS.saveIndividualLink(idFatherNewborn, idFatherPartner, meta_fathers);
					}

					if(motherMatched) {
						String meta_mothers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idMarriage + ","
								+ nextLine[5] + "," + nextLine[6] + "," + nextLine[11] + "," + nextLine[12] + "," + nextLine[13] + "," + nextLine[18];
						LINKS.saveIndividualLink(idMotherNewborn, idMotherPartner, meta_mothers);
					}

					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				//LINKS.flushLinks();
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}

	public Boolean saveLinksIndividuals_Within_B_D(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Within_B_D";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", linkProv = "W_B_D";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

			CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					int matchedIndiv = 1;
					Boolean fatherMatched = false,  motherMatched = false;

					String idBirth = nextLine[0];
					String idDeath = nextLine[1];
					String familyLine = nextLine[2];
					String idNewborn = myHDT.getPersonID(idBirth, ROLE_NEWBORN);
					String idDeceased = myHDT.getPersonID(idDeath, ROLE_DECEASED);
					String idFatherNewborn = null, idFatherDeceased = null, idMotherNewborn = null, idMotherDeceased = null;
					if(!nextLine[7].equals("N.A")) { // if there is a match for the fathers
						idFatherNewborn = myHDT.getPersonID(idBirth, ROLE_FATHER);

						if(idFatherNewborn != null) {
							idFatherDeceased = myHDT.getPersonID(idDeath, ROLE_FATHER);

							if(idFatherDeceased != null) {
								matchedIndiv++;
								fatherMatched = true;
							}
						}
					}
					if(!nextLine[5].equals("N.A")) { // if there is a match for the mothers
						idMotherNewborn = myHDT.getPersonID(idBirth, ROLE_MOTHER);
						if(idMotherNewborn != null) {
							idMotherDeceased = myHDT.getPersonID(idDeath, ROLE_MOTHER);

							if(idMotherDeceased != null) {
								matchedIndiv++;
								motherMatched = true;
							}
						}
					}
					String meta_newborn = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idDeath + ","
							+ nextLine[3] + "," + nextLine[4] + ","  + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[18];
					LINKS.saveIndividualLink(idNewborn, idDeceased, meta_newborn);

					if(fatherMatched) {
						String meta_fathers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idDeath + ","
								+ nextLine[7] + "," + nextLine[8] + "," + nextLine[13] + "," + nextLine[14] + "," + nextLine[15] + "," + nextLine[18];
						LINKS.saveIndividualLink(idFatherNewborn, idFatherDeceased, meta_fathers);
					}

					if(motherMatched) {
						String meta_mothers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idDeath + ","
								+ nextLine[5] + "," + nextLine[6] + "," + nextLine[11] + "," + nextLine[12] + "," + nextLine[13] + "," + nextLine[18];
						LINKS.saveIndividualLink(idMotherNewborn, idMotherDeceased, meta_mothers);
					}


					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}

	public Boolean saveLinksIndividuals_Between_B_M(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Between_B_M";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", familyLine = "", linkProv = "B_B_M", matchedIndiv = "2";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					String idBirth = nextLine[0];
					String idMarriage = nextLine[1];
					String idFather = myHDT.getPersonID(idBirth, ROLE_FATHER);
					String idMother = myHDT.getPersonID(idBirth, ROLE_MOTHER);
					String idGroom = myHDT.getPersonID(idMarriage, ROLE_GROOM);
					String idBride = myHDT.getPersonID(idMarriage, ROLE_BRIDE);

					String meta_father_groom = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idMarriage + ","
							+ nextLine[4] + "," + nextLine[5] + "," + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[12];
					String meta_mother_bride = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idMarriage + ","
							+ nextLine[2] + "," + nextLine[3] + "," + nextLine[6] + "," + nextLine[7] + "," + nextLine[8] + "," + nextLine[12];

					LINKS.saveIndividualLink(idFather, idGroom, meta_father_groom);
					LINKS.saveIndividualLink(idMother, idBride, meta_mother_bride);
					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}

	public Boolean saveLinksIndividuals_Between_B_D(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Between_B_D";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", familyLine = "", linkProv = "B_B_D", matchedIndiv = "2";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					String idBirth = nextLine[0];
					String idDeath = nextLine[1];
					String idFather = myHDT.getPersonID(idBirth, ROLE_FATHER);
					String idMother = myHDT.getPersonID(idBirth, ROLE_MOTHER);
					String idDeceased = myHDT.getPersonID(idDeath, ROLE_DECEASED);
					String idPartner = myHDT.getPersonID(idDeath, ROLE_PARTNER);

					String meta_father = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idDeath + ","
							+ nextLine[4] + "," + nextLine[5] + "," + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[12];
					String meta_mother = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idBirth + "," + idDeath + ","
							+ nextLine[2] + "," + nextLine[3] + "," + nextLine[6] + "," + nextLine[7] + "," + nextLine[8] + "," + nextLine[12];

					String deathURI = myHDT.getEventURIfromID(idDeath);
					Person deceased = myHDT.getPersonInfo(deathURI, ROLE_DECEASED);
					if(deceased.isFemale()) {
						LINKS.saveIndividualLink(idFather, idPartner, meta_father);
						LINKS.saveIndividualLink(idMother, idDeceased, meta_mother);
					} else {
						LINKS.saveIndividualLink(idFather, idDeceased, meta_father);
						LINKS.saveIndividualLink(idMother, idPartner, meta_mother);
					}
					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}

	public Boolean saveLinksIndividuals_Between_D_M(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Between_D_M";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", familyLine = "", linkProv = "B_D_M", matchedIndiv = "2";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					String idDeath = nextLine[0];
					String idMarriage = nextLine[1];
					String idFather = myHDT.getPersonID(idDeath, ROLE_FATHER);
					String idMother = myHDT.getPersonID(idDeath, ROLE_MOTHER);
					String idGroom = myHDT.getPersonID(idMarriage, ROLE_GROOM);
					String idBride = myHDT.getPersonID(idMarriage, ROLE_BRIDE);

					String meta_father_groom = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idDeath + "," + idMarriage + ","
							+ nextLine[4] + "," + nextLine[5] + "," + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[12];
					String meta_mother_bride = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idDeath + "," + idMarriage + ","
							+ nextLine[2] + "," + nextLine[3] + "," + nextLine[6] + "," + nextLine[7] + "," + nextLine[8] + "," + nextLine[12];

					LINKS.saveIndividualLink(idFather, idGroom, meta_father_groom);
					LINKS.saveIndividualLink(idMother, idBride, meta_mother_bride);
					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}

	public Boolean saveLinksIndividuals_Between_M_M(String filePath) {
		Boolean success = false;
		try {
            String taskName = "Between_M_M";
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", linkProv = "B_M_M", matchedIndiv = "2";
			int nbLines = FILE_UTILS.countLines(filePath);

            ProgressBar pb = new ProgressBarBuilder()
                .setTaskName(taskName)
                .setInitialMax(nbLines)
                .setUpdateIntervalMillis(updateInterval)
                .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                .build();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names
				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					String idMarriageParents = nextLine[0];
					String idMarriageCouple = nextLine[1];
					String familyLine = nextLine[2];
					String idFather = myHDT.getPersonID(idMarriageParents, ROLE_BRIDE_FATHER, ROLE_GROOM_FATHER, familyLine);
					String idMother = myHDT.getPersonID(idMarriageParents, ROLE_BRIDE_MOTHER, ROLE_GROOM_MOTHER, familyLine);
					String idGroom = myHDT.getPersonID(idMarriageCouple, ROLE_GROOM);
					String idBride = myHDT.getPersonID(idMarriageCouple, ROLE_BRIDE);

					String meta_father_groom = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idMarriageParents + "," + idMarriageCouple + ","
							+ nextLine[5] + "," + nextLine[6] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[12] + "," + nextLine[13];
					String meta_mother_bride = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + "," + idMarriageParents + "," + idMarriageCouple + ","
							+ nextLine[3] + "," + nextLine[4] + "," + nextLine[7] + "," + nextLine[8] + "," + nextLine[9] + "," + nextLine[13];

					LINKS.saveIndividualLink(idFather, idGroom, meta_father_groom);
					LINKS.saveIndividualLink(idMother, idBride, meta_mother_bride);
					if(countProgress % 1000 == 0) {
						pb.stepBy(1000);
					}
				}
				pb.stepTo(nbLines);
			} finally {
				pb.close();
				reader.close();
				success = true;
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		}

		return success;
	}
}
