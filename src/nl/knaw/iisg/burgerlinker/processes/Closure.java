package nl.knaw.iisg.burgerlinker.processes;


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
import nl.knaw.iisg.burgerlinker.Properties;  // TODO: rmv after birthYear change
import nl.knaw.iisg.burgerlinker.utilities.*;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


// Between_M_M: link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)
public class Closure {
	// output directory specified by the user + name of the called function
	public final static String DIRECTORY_NAME_DATABASE = "databases";
	private String inputDirectoryPath, outputDirectoryPath;
	private MyHDT myHDT;
    private Process process;
	private final int updateInterval = 5000;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Closure.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

    LinksCSV LINKS;
	String dirClassToIndivs, dirIndivToClass;

    HashMap<String, HashSet<String>> dbClassToIndivs;
	HashMap<String, String> dbIndivToClass;

    String namespacePerson = "<https://iisg.amsterdam/links/person/";  // FIXME: allow custom

	public Closure(MyHDT hdt, Process process, String directoryPath, boolean formatCSV) {
		this.inputDirectoryPath = directoryPath;
		this.outputDirectoryPath = directoryPath + "/closure";
		this.myHDT = hdt;
        this.process = process;

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
		boolean success = false;

		try {
			ArrayList<String> linkFiles = FILE_UTILS.getAllValidLinksFile(inputDirectoryPath, false);

			for (String linkFile : linkFiles) {
				success = success | saveIndividualLinksToFile(linkFile);
			}
			LOG.outputConsole("");

			if(success) {
				String filepath = LINKS.getLinksFilePath();

				transitiveClosure(filepath);
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
			int cntAll = 0;

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
							if(birthYear != null) {  // FIXME
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
			int nbLines = FILE_UTILS.countLines(linksFilePath), eqID = 0 ;

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
										mergeEqSets(eqIDIndividual1, valuesEqIDIndividual1,
                                                    eqIDIndividual2, valuesEqIDIndividual2);
									} else {
										mergeEqSets(eqIDIndividual2, valuesEqIDIndividual2,
                                                    eqIDIndividual1, valuesEqIDIndividual1);
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

	public void mergeEqSets(String eqIDIndividualLarger, HashSet<String> valuesLarger,
                            String eqIDIndividualSmaller, HashSet<String> valuesSmaller) {
		valuesLarger.addAll(valuesSmaller);
		dbClassToIndivs.put(eqIDIndividualLarger, valuesLarger);
		for(String valueEq2: valuesSmaller) {
			dbIndivToClass.put(valueEq2, eqIDIndividualLarger);
		}
		dbClassToIndivs.remove(eqIDIndividualSmaller);
	}

	public boolean saveIndividualLinksToFile(String filePath) {
		boolean success = true;
		if(FILE_UTILS.check_Within_B_M(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_MARIAGE,
                                   Process.RelationType.WITHIN);

			success = success & saveLinksIndividuals_Within(filePath);
		}
		if(FILE_UTILS.check_Within_B_D(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_DECEASED,
                                   Process.RelationType.WITHIN);

			success = success & saveLinksIndividuals_Within(filePath);
		}
		if(FILE_UTILS.check_Between_B_M(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_MARIAGE,
                                   Process.RelationType.BETWEEN);

			success = success & saveLinksIndividuals_Between(filePath);
		}
		if(FILE_UTILS.check_Between_D_M(filePath)) {
            this.process.setValues(Process.ProcessType.DECEASED_MARIAGE,
                                   Process.RelationType.BETWEEN);

			success = success & saveLinksIndividuals_Between(filePath);
		}
		if(FILE_UTILS.check_Between_B_D(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_DECEASED,
                                   Process.RelationType.BETWEEN);

			success = success & saveLinksIndividuals_Between(filePath);
		}
		if(FILE_UTILS.check_Between_M_M(filePath)) {
            this.process.setValues(Process.ProcessType.MARIAGE_MARIAGE,
                                   Process.RelationType.BETWEEN);

			success = success & saveLinksIndividuals_Between(filePath);
		}

		LINKS.flushLinks();

		return success;
	}

	public boolean saveLinksIndividuals_Within(String filePath) {
		boolean success = false;
		try {
            String taskName = this.process.toString();
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;

			String linktype = "sameAs", linkProv = this.process.abbr();
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
					boolean fatherMatched = false,  motherMatched = false;

					String idEventA = nextLine[0];
					String idEventB = nextLine[1];
					String familyLine = nextLine[2];
					String idSubjectA = myHDT.getPersonID(idEventA, this.process.roleASubject);
                    String idSubjectB;
                    if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                        idSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubject);
                    } else {
                        idSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubject,
                                                       this.process.roleBSubjectPartner, familyLine);
                    }
					String idFatherSubjectA = null, idFatherSubjectB = null, idMotherSubjectA = null, idMotherSubjectB = null;

					if(!nextLine[7].equals("N.A")) { // if there is a match for the fathers
						idFatherSubjectA = myHDT.getPersonID(idEventA, process.roleASubjectFather);

						if(idFatherSubjectA != null) {
                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                idFatherSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubjectFather);
                            } else {
                                idFatherSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubjectFather,
                                                                     this.process.roleBSubjectPartnerFather, familyLine);
                            }
							if(idFatherSubjectB != null) {
								matchedIndiv++;

								fatherMatched = true;
							}
						}
					}

					if(!nextLine[5].equals("N.A")) { // if there is a match for the mothers
						idMotherSubjectA = myHDT.getPersonID(idEventA, this.process.roleASubjectMother);
						if(idMotherSubjectA != null) {
                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                idMotherSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubjectMother);
                            } else {
                                idMotherSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubjectMother,
                                                                     this.process.roleBSubjectPartnerMother, familyLine);
                            }

							if(idMotherSubjectB != null) {
								matchedIndiv++;
								motherMatched = true;
							}
						}
					}

					String meta_newborn = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                          + idEventA + "," + idEventB + "," + nextLine[3] + "," + nextLine[4] + ","
                                          + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[18];
					LINKS.saveIndividualLink(idSubjectA, idSubjectB, meta_newborn);

					if(fatherMatched) {
						String meta_fathers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                              + idEventA + "," + idEventB + "," + nextLine[7] + "," + nextLine[8] + ","
                                              + nextLine[15] + "," + nextLine[16] + "," + nextLine[17] + "," + nextLine[18];
						LINKS.saveIndividualLink(idFatherSubjectA, idFatherSubjectB, meta_fathers);
					}

					if(motherMatched) {
						String meta_mothers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                              + idEventA + "," + idEventB + "," + nextLine[5] + "," + nextLine[6] + ","
                                              + nextLine[12] + "," + nextLine[13] + "," + nextLine[14] + "," + nextLine[18];
						LINKS.saveIndividualLink(idMotherSubjectA, idMotherSubjectB, meta_mothers);
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

	public boolean saveLinksIndividuals_Between(String filePath) {
		Boolean success = false;
		try {
            String taskName = this.process.toString();
			LOG.outputConsole("");
			LOG.outputConsole("Saving individual links (" + taskName + ") for file: " + filePath);

			String [] nextLine;
			String linktype = "sameAs", linkProv = this.process.abbr(), matchedIndiv = "2";
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

					String idEventA = nextLine[0];
					String idEventB = nextLine[1];
					String familyLine = nextLine[2];
					String idSubjectB = myHDT.getPersonID(idEventB, this.process.roleBSubject);
					String idSubjectBPartner = myHDT.getPersonID(idEventB, this.process.roleBSubjectPartner);
                    String idFather, idMother, meta_father, meta_mother;

					String meta = linktype + "," + linkProv + "," + familyLine + ","
                                  + matchedIndiv + "," + idEventA + "," + idEventB;
                    if (this.process.type == Process.ProcessType.MARIAGE_MARIAGE) {
                        idFather = myHDT.getPersonID(idEventA, this.process.roleASubjectFather,
                                                     this.process.roleBSubjectPartnerFather, familyLine);
                        idMother = myHDT.getPersonID(idEventA, this.process.roleASubjectMother,
                                                     this.process.roleBSubjectPartnerMother, familyLine);
                    } else {
                        idFather = myHDT.getPersonID(idEventA, this.process.roleASubjectFather);
    					idMother = myHDT.getPersonID(idEventA, this.process.roleASubjectMother);
                    }

                    meta_father = meta + ","
                                  + nextLine[7] + "," + nextLine[8] + "," + nextLine[15] + ","
                                  + nextLine[16] + "," + nextLine[17] + "," + nextLine[18];
                    meta_mother = meta + ","
                                  + nextLine[5] + "," + nextLine[6] + "," + nextLine[12] + ","
                                  + nextLine[13] + "," + nextLine[14] + "," + nextLine[18];

                    String idSubjectFemale = idSubjectB;
                    String idSubjectMale = idSubjectBPartner;
                    if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                        String eventBURI = myHDT.getEventURIfromID(idEventB);
                        Person subjectB = myHDT.getPersonInfo(eventBURI, this.process.roleBSubject);
                        if (subjectB.isMale()) {
                            idSubjectMale = idSubjectB;
                            idSubjectFemale = idSubjectBPartner;
                        }
                    }

					LINKS.saveIndividualLink(idMother, idSubjectFemale, meta_mother);
					LINKS.saveIndividualLink(idFather, idSubjectMale, meta_father);

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
