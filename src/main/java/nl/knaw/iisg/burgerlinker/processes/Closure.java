package nl.knaw.iisg.burgerlinker.processes;

import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.QueryResults;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import nl.knaw.iisg.burgerlinker.core.Index;
import nl.knaw.iisg.burgerlinker.data.LinksCSV;
import nl.knaw.iisg.burgerlinker.data.MyRDF;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.Properties;
import nl.knaw.iisg.burgerlinker.utilities.*;
import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;


// Between_M_M: link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)
public class Closure {
	// output directory specified by the user + name of the called function
	private String namespace;
	private MyRDF myRDF;
    private Process process;
    private File inputDirectoryPath, outputDirectoryPath;
	Index indexBride, indexGroom;

    private static final String RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    private static final String PICOM_NS = "https://personsincontext.org/model#";
    private static final String PROV_NS = "http://www.w3.org/ns/prov#";
    private static final String SDO_NS = "https://schema.org/";
    private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";

	public static final Logger lg = LogManager.getLogger(Closure.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

    LinksCSV LINKS;
	String dirClassToIndivs, dirIndivToClass;

    HashMap<String, HashSet<String>> dbClassToIndivs;
	HashMap<String, String> dbIndivToClass;

	public Closure(MyRDF myRDF, Process process, String namespace,
                   File directoryPath, boolean formatCSV) throws java.io.IOException {
		this.inputDirectoryPath = directoryPath;
		this.outputDirectoryPath = new File(directoryPath.getCanonicalPath());
		this.myRDF = myRDF;
        this.process = process;
        this.namespace = namespace;

		String resultsFileName = "links-individuals";
		if(formatCSV == true) {
			String header = "id_certificate_A,"
                          + "id_certificate_B,"
                          + "link,"
                          + "process,"
                          + "family_line,"
                          + "number_certificate_matches,"
                          + "id_event_A,"
                          + "id_event_B,"
                          + "levenshtein_total_AB,"
                          + "levenshtein_max_AB,"
                          + "number_names_matches_AB,"
                          + "number_names_A,"
                          + "number_names_B,"
                          + "year_diff";

			LINKS = new LinksCSV(resultsFileName, outputDirectoryPath, header);

			this.dirClassToIndivs = outputDirectoryPath + "/" + DIRECTORY_NAME_DATABASE + "/ClassToIndivs";
			this.dirIndivToClass = outputDirectoryPath + "/" + DIRECTORY_NAME_DATABASE + "/IndivToClass";
		}
	}

	public void computeClosure() {
		boolean success = false;

		try {
			ArrayList<String> linkFiles = FILE_UTILS.getAllValidLinksFile(inputDirectoryPath.getParentFile(), false);
            linkFiles.sort(null);

			for (String linkFile : linkFiles) {
				success = success | saveIndividualLinksToFile(linkFile);
			}

			if(success) {
				String filepath = LINKS.getLinksFilePath();

				transitiveClosure(filepath);
				verifyClosure();
				saveClosureToFile();
				reconstructDataset(this.namespace);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void reconstructDataset(String namespace) {
		try {

			int cntAll = 0;

			LinksCSV reconstruction = new LinksCSV("dataset_reconstructed", outputDirectoryPath, true);

            ActivityIndicator spinner = new ActivityIndicator(".: Reconstructing Individuals");
			try {
                spinner.start();

                String activityDateBegin = LocalDateTime.now().toString();
                String activityIRI;
                if (namespace.equals("_:")) {
                    activityIRI = namespace + UUID.nameUUIDFromBytes((activityDateBegin.getBytes()));
                } else {
                    activityIRI = "<" + namespace + UUID.nameUUIDFromBytes((activityDateBegin.getBytes())) + ">";
                }
                reconstruction.addToStream(activityIRI + " " + RDF_TYPE + " <" + PROV_NS + "Activity> .");

                String username = System.getProperty("user.name");
                if (username != null && username.length() > 0) {
                    String usernameNode = "_:" + UUID.nameUUIDFromBytes((username.getBytes()));

                    reconstruction.addToStream(activityIRI + " <" + PROV_NS + "wasAssociatedWith> " + usernameNode + " .");
                    reconstruction.addToStream(usernameNode + " " + RDF_TYPE + " <" + PROV_NS + "Agent> .");
                    reconstruction.addToStream(usernameNode + " <" + SDO_NS + "name> \"" + username + "\" .");
                }

                String activityDateBeginLit = "\"" + activityDateBegin + "\"^^<" + XSD_NS + "dateTime>";
                reconstruction.addToStream(activityIRI + " <" + PROV_NS + "startedAtTime> " + activityDateBeginLit + " .");

                for (String eqClass: dbClassToIndivs.keySet()) {
                    String reconstructedIRI;
                    if (namespace.equals("_:")) {
                        reconstructedIRI = namespace + eqClass;
                    } else {
                        reconstructedIRI = "<" + namespace + eqClass + ">";
                    }

                    reconstruction.addToStream(reconstructedIRI + " " + RDF_TYPE + " <" + PICOM_NS + "PersonReconstruction> .");
                    reconstruction.addToStream(reconstructedIRI + " <" + PROV_NS + "wasGeneratedBy> " + activityIRI + " .");
                    for (String personIRI: dbClassToIndivs.get(eqClass)) {
                        reconstruction.addToStream(reconstructedIRI + " <" + PROV_NS + "wasDerivedFrom> <" + personIRI + "> .");
                    }

                    cntAll++;
                    if (cntAll % 5000 == 0) {
                        spinner.update(cntAll);
                    }

                }

                String activityDateEnd = LocalDateTime.now().toString();
                String activityDateEndLit = "\"" + activityDateEnd + "\"^^<" + XSD_NS + "dateTime>";
                reconstruction.addToStream(activityIRI + " <" + PROV_NS + "endedAtTime> " + activityDateEndLit + " .");

            } finally {
				reconstruction.closeStream();
                spinner.terminate();
                spinner.join();
			}
		} catch (Exception e) {
			LOG.logError("reconstructDataset", "Error in reconstructing the dataset after transitive closure");
			e.printStackTrace();
		} finally {
			LINKS.closeStream();
		}
	}

	public String getEqClassOfPerson(String personIRI, String namespace) {
		if (personIRI != null) {
			String eqClass = dbIndivToClass.get(personIRI);
			if (eqClass != null) {
                if (namespace.equals("_:")) {
                    eqClass = namespace + eqClass;
                } else {
                    eqClass = "<" + namespace + eqClass + ">";
                }

				// linked person
				return eqClass;
			}
		}

        return null;
    }

	public void createDB() {
		dbClassToIndivs = new HashMap<String, HashSet<String>>();
		dbIndivToClass = new HashMap<String, String>();
	}

	public void verifyClosure() {
		int countIndivsClassToIndivs = 0, max = 0;
		for (Entry<String, HashSet<String>> entry: dbClassToIndivs.entrySet()) {
			int n = entry.getValue().size();
			countIndivsClassToIndivs = countIndivsClassToIndivs + n;
			if (n > max) {
				max = n;
			}
		}

        DecimalFormat formatter = new DecimalFormat("#,###");
        Map<String, String> summary = new HashMap<>();
        summary.put("Number of individuals from Invidual to Class", formatter.format(dbIndivToClass.size()));
        summary.put("Number of individuals from Class to Individuals", formatter.format(countIndivsClassToIndivs));
        summary.put("Number of max individuals in Eq Class", formatter.format(max));

        int keyLenMax = 0, valLenMax = 0;
        for (String key: summary.keySet()) {
            String val = summary.get(key);
            if (val.length() > valLenMax) {
                valLenMax -= val.length();
            }
            if (key.length() > keyLenMax) {
                keyLenMax = key.length();
            }
        }

        LOG.outputConsole(".: Closure Summary");
        for (String key: summary.keySet()) {
            String val = summary.get(key);
            LOG.outputConsole("   - " + String.format("%-" + keyLenMax + "s", key)
                              + "   " + String.format("%" + valLenMax + "s", val));
        }
	}

	public void saveClosureToFile() {
        ActivityIndicator spinner = new ActivityIndicator(".: Saving Closure Results");
        spinner.start();

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

		closureTerms.closeStream();
		closureDist.closeStream();

        spinner.terminate();
        try {
            spinner.join();
        } catch (Exception e) {
            LOG.logError("saveClosureToFile", "Error waiting for ActivityIndicator to stop: " + e);
        }
	}

	public void transitiveClosure(String linksFilePath) {
		try {
			String taskName = "Closure";

            createDB();

			String [] nextLine;
			int nbLines = FILE_UTILS.countLines(linksFilePath);

            ActivityIndicator spinner = new ActivityIndicator(".: Computing Closure");
            spinner.start();

            CSVReader reader = new CSVReader(new FileReader(linksFilePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					try {
                        String newEqID;
						String invidual1 = nextLine[0];
						String invidual2 = nextLine[1];

                        // ensure deterministic ID from birth event (if available)
                        UUID uuid = UUID.nameUUIDFromBytes((invidual1.getBytes()));
                        String linkProv = nextLine[3];
                        newEqID = linkProv.charAt(2) == 'B' ? "I-" + uuid : "U-" + uuid;

						String eqIDIndividual1 = dbIndivToClass.get(invidual1);
						String eqIDIndividual2 = dbIndivToClass.get(invidual2);
						if(eqIDIndividual1 == null) {
							if(eqIDIndividual2 == null) {
								HashSet<String> eqValues = new HashSet<String>();

								eqValues.add(invidual1);
								eqValues.add(invidual2);

								dbClassToIndivs.put(newEqID, eqValues);
								dbIndivToClass.put(invidual1, newEqID);
								dbIndivToClass.put(invidual2, newEqID);
							} else {
                                HashSet<String> members = dbClassToIndivs.get(eqIDIndividual2);
                                members.add(invidual1);

                                if (newEqID.startsWith("I-")) {
                                    // primary ID found; replace in map
                                    dbClassToIndivs.remove(eqIDIndividual2);
                                    dbClassToIndivs.put(newEqID, members);

                                    for (String individual:members) {
                                        dbIndivToClass.put(individual, newEqID);
                                    }
                                } else {
                                    dbClassToIndivs.put(eqIDIndividual2, members);
                                    dbIndivToClass.put(invidual1, eqIDIndividual2);
                                }
							}
						} else {
							if(eqIDIndividual2 == null) {
                                HashSet<String> members = dbClassToIndivs.get(eqIDIndividual1);
                                members.add(invidual2);

                                if (newEqID.startsWith("I-")) {
                                    // primary ID found; replace in map
                                    dbClassToIndivs.remove(eqIDIndividual1);
                                    dbClassToIndivs.put(newEqID, members);

                                    for (String individual:members) {
                                        dbIndivToClass.put(individual, newEqID);
                                    }
                                } else {
                                    dbClassToIndivs.put(eqIDIndividual1, members);
                                    dbIndivToClass.put(invidual2, eqIDIndividual1);
                                }
							} else {
								if(!eqIDIndividual1.equals(eqIDIndividual2)) {
									HashSet<String> valuesEqIDIndividual1 = dbClassToIndivs.get(eqIDIndividual1);
									HashSet<String> valuesEqIDIndividual2 = dbClassToIndivs.get(eqIDIndividual2);

                                    if (eqIDIndividual1.startsWith("I-")) {
										mergeEqSets(eqIDIndividual1, valuesEqIDIndividual1,
                                                    eqIDIndividual2, valuesEqIDIndividual2);
                                    } else if (eqIDIndividual2.startsWith("i-")) {
										mergeEqSets(eqIDIndividual2, valuesEqIDIndividual2,
                                                    eqIDIndividual1, valuesEqIDIndividual1);
                                    } else {  // merge on size as fallback
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
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
                    if (countProgress % 5000 == 0) {
                        spinner.update(countProgress);
                    }
				}
			} finally {
				reader.close();
                spinner.terminate();
                spinner.join();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void mergeEqSets(String eqIDIndividualA, HashSet<String> valuesA,
                            String eqIDIndividualB, HashSet<String> valuesB) {
		valuesA.addAll(valuesB);

		dbClassToIndivs.remove(eqIDIndividualB);
		dbClassToIndivs.put(eqIDIndividualA, valuesA);

        for(String individual: valuesB) {
			dbIndivToClass.put(individual, eqIDIndividualA);
		}
	}

	public boolean saveIndividualLinksToFile(String filePath) {
		boolean success = true;
		if(FILE_UTILS.check_Within_B_M(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_MARRIAGE,
                                   Process.RelationType.WITHIN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Within(filePath, queryEventA, queryEventB);
		}
		if(FILE_UTILS.check_Within_B_D(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_DECEASED,
                                   Process.RelationType.WITHIN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Within(filePath, queryEventA, queryEventB);
		}
		if(FILE_UTILS.check_Between_B_M(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_MARRIAGE,
                                   Process.RelationType.BETWEEN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Between(filePath, queryEventA, queryEventB);
		}
		if(FILE_UTILS.check_Between_D_M(filePath)) {
            this.process.setValues(Process.ProcessType.DECEASED_MARRIAGE,
                                   Process.RelationType.BETWEEN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Between(filePath, queryEventA, queryEventB);
		}
		if(FILE_UTILS.check_Between_B_D(filePath)) {
            this.process.setValues(Process.ProcessType.BIRTH_DECEASED,
                                   Process.RelationType.BETWEEN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Between(filePath, queryEventA, queryEventB);
		}
		if(FILE_UTILS.check_Between_M_M(filePath)) {
            this.process.setValues(Process.ProcessType.MARRIAGE_MARRIAGE,
                                   Process.RelationType.BETWEEN);

            String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
            String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

			success = success & saveLinksIndividuals_Between(filePath, queryEventA, queryEventB);
		}

		LINKS.flushLinks();

		return success;
	}

	public boolean saveLinksIndividuals_Within(String filePath, String qEventA, String qEventB) {
		boolean success = false;
		try {
            String taskName = this.process.toString();
			String [] nextLine;

			String linktype = "sameAs", linkProv = this.process.abbr();
			int nbLines = FILE_UTILS.countLines(filePath);

            ActivityIndicator spinner = new ActivityIndicator(".: Consolidating " + taskName + " Links");
            spinner.start();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					int matchedIndiv = 1;
					boolean fatherMatched = false,  motherMatched = false;

                    String familyLine = nextLine[2];  // 21: bride, 22: groom

                    // event A participants
					String eventA = nextLine[0];
                    Map<String, Value> bindingsA = new HashMap<>();
                    bindingsA.put("event", MyRDF.mkIRI(eventA));
                    BindingSet qResultA = myRDF.getQueryResultsAsList(qEventA, bindingsA).get(0);

                    String subjectA = qResultA.getValue("subject").stringValue();
                    String subjectAFather = null, subjectAMother = null;
					if (!nextLine[7].equals("N.A.")) { // if there is a match for the fathers
                        subjectAFather = qResultA.getValue("subjectFather").stringValue();
                    }
                    if (!nextLine[5].equals("N.A.")) { // if there is a match for the mothers
                        subjectAMother = qResultA.getValue("subjectMother").stringValue();
                    }

                    // event B participants
					String eventB = nextLine[1];
                    Map<String, Value> bindingsB = new HashMap<>();
                    bindingsB.put("event", MyRDF.mkIRI(eventB));
                    BindingSet qResultB = myRDF.getQueryResultsAsList(qEventB, bindingsB).get(0);

                    String subjectB;
                    if (this.process.type == Process.ProcessType.BIRTH_DECEASED || familyLine.equals("21")) {
                        subjectB = qResultB.getValue("subject").stringValue();
                    } else {  // familyLine.equals("22")
                        subjectB = qResultB.getValue("partner").stringValue();
                    }

					String subjectBFather = null;
					if(subjectAFather != null) { // if there is a match for the fathers
                        if (this.process.type == Process.ProcessType.BIRTH_DECEASED || familyLine.equals("21")) {
                            subjectBFather = qResultB.getValue("subjectFather").stringValue();
                        } else {
                            subjectBFather = qResultB.getValue("partnerFather").stringValue();
                        }

                        if (subjectBFather != null) {
                            matchedIndiv++;

                            fatherMatched = true;
                        }
					}

					String subjectBMother = null;
                    if(subjectAMother != null) { // if there is a match for the mothers
                        if (this.process.type == Process.ProcessType.BIRTH_DECEASED || familyLine.equals("21")) {
                            subjectBMother = qResultB.getValue("subjectMother").stringValue();
                        } else {
                            subjectBMother = qResultB.getValue("partnerMother").stringValue();
                        }

                        if (subjectBMother != null) {
                            matchedIndiv++;

                            motherMatched = true;
                        }
                    }

                    // metadata
					String meta_newborn = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                          + eventA + "," + eventB + "," + nextLine[3] + "," + nextLine[4] + ","
                                          + nextLine[9] + "," + nextLine[10] + "," + nextLine[11] + "," + nextLine[18];
					LINKS.saveIndividualLink(subjectA, subjectB, meta_newborn);

					if(fatherMatched) {
						String meta_fathers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                              + eventA + "," + eventB + "," + nextLine[7] + "," + nextLine[8] + ","
                                              + nextLine[15] + "," + nextLine[16] + "," + nextLine[17] + "," + nextLine[18];
						LINKS.saveIndividualLink(subjectAFather, subjectBFather, meta_fathers);
					}

					if(motherMatched) {
						String meta_mothers = linktype + "," + linkProv + "," + familyLine + "," + matchedIndiv + ","
                                              + eventA + "," + eventB + "," + nextLine[5] + "," + nextLine[6] + ","
                                              + nextLine[12] + "," + nextLine[13] + "," + nextLine[14] + "," + nextLine[18];
						LINKS.saveIndividualLink(subjectAMother, subjectBMother, meta_mothers);
					}

                    if (countProgress % 5000 == 0) {
                        spinner.update(countProgress);
                    }
				}
			} finally {
				reader.close();
                spinner.terminate();
                spinner.join();
				success = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return success;
	}

	public boolean saveLinksIndividuals_Between(String filePath, String qEventA, String qEventB) {
		Boolean success = false;
		try {
            String taskName = this.process.toString();

			String [] nextLine;
			String linktype = "sameAs", linkProv = this.process.abbr(), matchedIndiv = "2";
			int nbLines = FILE_UTILS.countLines(filePath);

            ActivityIndicator spinner = new ActivityIndicator(".: Consolidating " + taskName + " Links");
            spinner.start();

            CSVReader reader = new CSVReader(new FileReader(filePath));
			try {
				reader.readNext(); // skip the column names

				int countProgress = 1;
				while ((nextLine = reader.readNext()) != null) {
					countProgress++;

					String familyLine = nextLine[2];

                    // event A participants
					String eventA = nextLine[0];
                    Map<String, Value> bindingsA = new HashMap<>();
                    bindingsA.put("event", MyRDF.mkIRI(eventA));
                    BindingSet bindingSetA = myRDF.getQueryResultsAsList(qEventA, bindingsA).get(0);

                    String fatherIRI, motherIRI;
                    if (this.process.type != Process.ProcessType.MARRIAGE_MARRIAGE
                        || familyLine.equals("21")) {
                        fatherIRI = bindingSetA.getValue("subjectFather").stringValue();
                        motherIRI = bindingSetA.getValue("subjectMother").stringValue();
                    } else {  // familyLine == 22 // groom
                        fatherIRI = bindingSetA.getValue("partnerFather").stringValue();
                        motherIRI = bindingSetA.getValue("partnerMother").stringValue();
                    }

                    // event B participants
					String eventB = nextLine[1];
                    Map<String, Value> bindingsB = new HashMap<>();
                    bindingsB.put("event", MyRDF.mkIRI(eventB));
                    BindingSet bindingSetB = myRDF.getQueryResultsAsList(qEventB, bindingsB).get(0);

					String subjectBIRI = bindingSetB.getValue("subject").stringValue();
					String subjectBPartnerIRI = bindingSetB.getValue("partner").stringValue();

                    // metadata
					String meta = linktype + "," + linkProv + "," + familyLine + ","
                                  + matchedIndiv + "," + eventA + "," + eventB;

                    String meta_father, meta_mother;
                    meta_father = meta + ","
                                  + nextLine[7] + "," + nextLine[8] + "," + nextLine[15] + ","
                                  + nextLine[16] + "," + nextLine[17] + "," + nextLine[18];
                    meta_mother = meta + ","
                                  + nextLine[5] + "," + nextLine[6] + "," + nextLine[12] + ","
                                  + nextLine[13] + "," + nextLine[14] + "," + nextLine[18];

                    // determine order based on gender
                    String subjectFemale = subjectBIRI;
                    String subjectMale = subjectBPartnerIRI;
                    if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                        Person subjectB = new Person(eventB,
                                                 bindingSetB.getValue("subjectGivenName"),
                                                 bindingSetB.getValue("subjectFamilyName"),
                                                 bindingSetB.getValue("subjectGender"));

                        if (subjectB.isMale()) {
                            subjectMale = subjectBIRI;
                            subjectFemale = subjectBPartnerIRI;
                        }
                    }

					LINKS.saveIndividualLink(motherIRI, subjectFemale, meta_mother);
					LINKS.saveIndividualLink(fatherIRI, subjectMale, meta_father);

                    if (countProgress % 5000 == 0) {
                        spinner.update(countProgress);
                    }
				}
			} finally {
				reader.close();
                spinner.terminate();
                spinner.join();
				success = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return success;
	}
}
