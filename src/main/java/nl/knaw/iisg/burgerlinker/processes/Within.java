package nl.knaw.iisg.burgerlinker.processes;

import java.io.File;
import java.lang.Math;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rule;
import org.eclipse.rdf4j.model.Value;
import static org.eclipse.rdf4j.model.util.Values.iri;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;

import nl.knaw.iisg.burgerlinker.core.CandidateList;
import nl.knaw.iisg.burgerlinker.core.Dictionary;
import nl.knaw.iisg.burgerlinker.core.Index;
import nl.knaw.iisg.burgerlinker.data.LinksCSV;
import nl.knaw.iisg.burgerlinker.data.MyRDF;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class Within {
	private File mainDirectoryPath;
    private String processName;
	private MyRDF myRDF;
    private Process process;
    private Map<String, Rule> rules;
	private final int linkingUpdateInterval = 10000;
	private int maxLev;
	private boolean fixedLev, ignoreDate, ignoreBlock, singleInd;
	Index indexSubjectB, indexMother, indexFather;

    public static final Logger lg = LogManager.getLogger(Within.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	LinksCSV LINKS;

	public Within(MyRDF myRDF, Process process, Map<String, Rule> rules, File directoryPath,
                  Integer maxLevenshtein, boolean fixedLev, boolean ignoreDate,
                  boolean ignoreBlock, boolean singleInd, boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.myRDF = myRDF;
        this.process = process;
        this.processName = this.process.toString();
        this.rules = rules;

		String options = LOG.getUserOptions(this.maxLev, this.fixedLev, singleInd,
                                            this.ignoreDate, this.ignoreBlock);
		String resultsFileName = this.processName + options;
		if(formatCSV == true) {
			LINKS = new LinksCSV(resultsFileName, this.mainDirectoryPath, this.process.csvHeader);
		}
	}

	public void link_within(String gender, boolean closeStream) {
        String familyCode = (gender == "m") ? "22" : "21";

        String querySubjectA = MyRDF.generalizeQuery(MyRDF.qNewbornInfoFromEventURI);

        String querySubjectB = "";
        boolean genderFilter = false;
        switch (this.process.type) {
            case BIRTH_DECEASED:
                querySubjectB = MyRDF.generalizeQuery(MyRDF.qDeceasedInfoFromEventURI, gender);
                genderFilter = true;

                break;
            case BIRTH_MARIAGE:
                querySubjectB = MyRDF.generalizeQuery(MyRDF.qMarriageInfoFromEventURI, gender);
                genderFilter = false;

                break;
        }

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
		if (dict.generateDictionaryThreeWay(myRDF, querySubjectB, genderFilter, gender)) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			try {
				int cntAll = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator("Linking " + processName + " (" + gender + ")");
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(querySubjectA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();
						String eventID = bindingSetA.getValue("eventID").stringValue();

						Person newborn = new Person(event,
                                                bindingSetA.getValue("givenNameSubject").stringValue(),
                                                bindingSetA.getValue("familyNameSubject").stringValue(),
                                                bindingSetA.getValue("genderSubject").stringValue());
						if (newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
                                Person mother = new Person(event,
                                                       bindingSetA.getValue("givenNameSubjectMother").stringValue(),
                                                       bindingSetA.getValue("familyNameSubjectMother").stringValue(),
                                                       bindingSetA.getValue("genderSubjectMother").stringValue());
                                Person father = new Person(event,
                                                       bindingSetA.getValue("givenNameSubjectFather").stringValue(),
                                                       bindingSetA.getValue("familyNameSubjectFather").stringValue(),
                                                       bindingSetA.getValue("genderSubjectFather").stringValue());

								CandidateList candidatesSubjectB=null, candidatesMother=null, candidatesFather=null;
								if (mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, eventID, ignoreBlock);

									if (candidatesSubjectB.candidates.isEmpty() == false) {
										if (mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, eventID, ignoreBlock);

											if (candidatesMother.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMother = candidatesSubjectB.findIntersectionCandidates(candidatesMother);

												for (String finalCandidate: finalCandidatesMother) {
													boolean link = true;
													if (father.isValidWithFullName()){
														if(candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("F")) {
															link = false; // if both have fathers, but their names did not match
														}
													}

													if (link == true) { // if one of them does not have a father, we match on two individuals
                                                        Map<String, Value> bindings = new HashMap<>();
                                                        bindings.put("eventID", iri(finalCandidate));

                                                        TupleQueryResult qResultB = myRDF.getQueryResults(querySubjectB, bindings);
                                                        for (BindingSet bindingSetB: qResultB) {
                                                            String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                                            int yearDifference = 0;
                                                            if (ignoreDate == false) {
                                                                int eventADate = myRDF.valueToInt(bindingSetA.getValue("eventDate"));
                                                                int eventBDate = myRDF.valueToInt(bindingSetB.getValue("eventDate"));
                                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                                            }
                                                            if (yearDifference < 999) { // if it fits the time line
                                                                Person subjectB = new Person(subjectBEventURI,
                                                                                         bindingSetB.getValue("givenNameSubject").stringValue(),
                                                                                         bindingSetB.getValue("familyNameSubject").stringValue(),
                                                                                         bindingSetB.getValue("genderSubject").stringValue());
                                                                Person subjectBMother = new Person(subjectBEventURI,
                                                                                               bindingSetB.getValue("givenNameSubjectMother").stringValue(),
                                                                                               bindingSetB.getValue("familyNameSubjectMother").stringValue(),
                                                                                               bindingSetB.getValue("genderSubjectMother").stringValue());

                                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                                    int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectMother"));
                                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                                        checkTimeConsistencyWithAge(yearDifference, subjectBMotherAge)) {
                                                                        LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, finalCandidate,
                                                                                                      subjectB, subjectBMother, familyCode, yearDifference);
                                                                        }
                                                                } else {
                                                                    LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, finalCandidate,
                                                                                                  subjectB, subjectBMother, familyCode, yearDifference);
                                                                }
                                                            }
                                                        }
                                                        qResultB.close();
													}
												}
											}
										}
										if (father.isValidWithFullName()){
											candidatesFather = indexFather.searchForCandidate(father, eventID, ignoreBlock);

											if (candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesFather = candidatesSubjectB.findIntersectionCandidates(candidatesFather);

												for (String finalCandidate: finalCandidatesFather) {
													boolean link = true;
													if (mother.isValidWithFullName()){
														if(candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("M")) {
															link = false; // if both have mothers, but their names did not match
														}
													}

													if (link == true) { // if one of them does not have a mother, we match on two individuals
                                                        Map<String, Value> bindings = new HashMap<>();
                                                        bindings.put("eventID", iri(finalCandidate));

                                                        TupleQueryResult qResultB = myRDF.getQueryResults(querySubjectB, bindings);
                                                        for (BindingSet bindingSetB: qResultB) {
                                                            String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                                            int yearDifference = 0;
                                                            if (ignoreDate == false) {
                                                                int eventADate = myRDF.valueToInt(bindingSetA.getValue("eventDate"));
                                                                int eventBDate = myRDF.valueToInt(bindingSetB.getValue("eventDate"));
                                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                                            }
                                                            if (yearDifference < 999) { // if it fits the time line
                                                                Person subjectB = new Person(subjectBEventURI,
                                                                                         bindingSetB.getValue("givenNameSubject").stringValue(),
                                                                                         bindingSetB.getValue("familyNameSubject").stringValue(),
                                                                                         bindingSetB.getValue("genderSubject").stringValue());
                                                                Person subjectBFather = new Person(subjectBEventURI,
                                                                                               bindingSetB.getValue("givenNameSubjectFather").stringValue(),
                                                                                               bindingSetB.getValue("familyNameSubjectFather").stringValue(),
                                                                                               bindingSetB.getValue("genderSubjectFather").stringValue());

                                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                                    int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectFather"));
                                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                                        checkTimeConsistencyWithAge(yearDifference, subjectBFatherAge)) {
                                                                        LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, finalCandidate,
                                                                                                      subjectB, subjectBFather, familyCode, yearDifference);
                                                                    }
                                                                } else {
                                                                    LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, finalCandidate,
                                                                                                  subjectB, subjectBFather, familyCode, yearDifference);
                                                                }
                                                            }
                                                        }
                                                        qResultB.close();
													}
												}
											}
										}
										if (mother.isValidWithFullName() && father.isValidWithFullName()) {
											if (candidatesMother.candidates.isEmpty() == false && candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMotherFather = candidatesSubjectB.findIntersectionCandidates(candidatesMother,
                                                                                                                                        candidatesFather);

												for (String finalCandidate: finalCandidatesMotherFather) {
                                                    Map<String, Value> bindings = new HashMap<>();
                                                    bindings.put("eventID", iri(finalCandidate));

                                                    TupleQueryResult qResultB = myRDF.getQueryResults(querySubjectB, bindings);
                                                    for (BindingSet bindingSetB: qResultB) {
                                                        String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                                        int yearDifference = 0;
                                                        if (ignoreDate == false) {
                                                            int eventADate = myRDF.valueToInt(bindingSetA.getValue("eventDate"));
                                                            int eventBDate = myRDF.valueToInt(bindingSetB.getValue("eventDate"));
                                                            yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                                        }
                                                        if (yearDifference < 999) { // if it fits the time line
                                                            Person subjectB = new Person(subjectBEventURI,
                                                                                     bindingSetB.getValue("givenNameSubject").stringValue(),
                                                                                     bindingSetB.getValue("familyNameSubject").stringValue(),
                                                                                     bindingSetB.getValue("genderSubject").stringValue());
                                                            Person subjectBMother = new Person(subjectBEventURI,
                                                                                           bindingSetB.getValue("givenNameSubjectMother").stringValue(),
                                                                                           bindingSetB.getValue("familyNameSubjectMother").stringValue(),
                                                                                           bindingSetB.getValue("genderSubjectMother").stringValue());
                                                            Person subjectBFather = new Person(subjectBEventURI,
                                                                                           bindingSetB.getValue("givenNameSubjectFather").stringValue(),
                                                                                           bindingSetB.getValue("familyNameSubjectFather").stringValue(),
                                                                                           bindingSetB.getValue("genderSubjectFather").stringValue());

                                                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                                int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                                int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectMother"));
                                                                int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectFather"));
                                                                if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                                    checkTimeConsistencyWithAge(yearDifference, subjectBMotherAge) &&
                                                                    checkTimeConsistencyWithAge(yearDifference, subjectBFatherAge)) {
                                                                        LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather,
                                                                                               finalCandidate, subjectB, subjectBMother, subjectBFather,
                                                                                               familyCode, yearDifference);
                                                                }
                                                            } else {
                                                                LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather,
                                                                                       finalCandidate, subjectB, subjectBMother, subjectBFather,
                                                                                       familyCode, yearDifference);
                                                            }
                                                        }
                                                    }
                                                    qResultB.close();
												}
											}
										}
									}
								}
							}
						}
                        if (cntAll % 10000 == 0) {
							spinner.update(cntAll);
						}
					}
				} finally {
					qResultA.close();
                    spinner.terminate();
				}
			} catch (Exception e) {
				LOG.logError("link_between", "Error in linking parents of newborns to partners in process " + this.processName);
				e.printStackTrace();
			} finally {
				if(closeStream == true) {
					LINKS.closeStream();
				}
			}
		}
	}

	public void link_within_single(String gender, boolean closeStream) {
        String familyCode = (gender == "m") ? "22" : "21";

        String querySubjectA = MyRDF.generalizeQuery(MyRDF.qNewbornInfoFromEventURI);

        String querySubjectB = "";
        boolean genderFilter = false;
        switch (this.process.type) {
            case BIRTH_DECEASED:
                querySubjectB = MyRDF.generalizeQuery(MyRDF.qDeceasedInfoFromEventURI, gender);
                genderFilter = true;

                break;
            case BIRTH_MARIAGE:
                querySubjectB = MyRDF.generalizeQuery(MyRDF.qMarriageInfoFromEventURI, gender);
                genderFilter = false;

                break;
        }

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
		if(dict.generateDictionaryOneWay(myRDF, querySubjectB, genderFilter, gender)) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			try {
				String taskName = "Linking Single " + processName + " (" + gender + ")";
				int cntAll = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator("Linking " + taskName);
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(querySubjectA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").toString();
						String eventID = bindingSetA.getValue("eventID").toString();

						Person newborn = new Person(event,
                                                bindingSetA.getValue("givenNameSubject").stringValue(),
                                                bindingSetA.getValue("familyNameSubject").stringValue(),
                                                bindingSetA.getValue("genderSubject").stringValue());
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								CandidateList candidatesSubjectB=null;

								candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, eventID, ignoreBlock);
								if(candidatesSubjectB.candidates.isEmpty() == false) {
									for(String finalCandidate: candidatesSubjectB.candidates.keySet()) {
                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("eventID", iri(finalCandidate));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(querySubjectB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            String subjectBEventURI = bindingSetB.getValue("event").toString();

                                            int yearDifference = 0;
                                            if(ignoreDate == false) {
                                                int eventADate = myRDF.valueToInt(bindingSetA.getValue("eventDate"));
                                                int eventBDate = myRDF.valueToInt(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if(yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("givenNameSubject").stringValue(),
                                                                         bindingSetB.getValue("familyNameSubject").stringValue(),
                                                                         bindingSetB.getValue("genderSubject").stringValue());

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                    if(checkTimeConsistencyWithAge(yearDifference, subjectBAge)) {
                                                        LINKS.saveLinks_Within_single(candidatesSubjectB, finalCandidate, subjectB,
                                                                                      familyCode, yearDifference);
                                                    }
                                                } else {
                                                    LINKS.saveLinks_Within_single(candidatesSubjectB, finalCandidate, subjectB,
                                                                                  familyCode, yearDifference);
                                                }
                                            }
                                        }
                                        qResultB.close();
									}
								}
							}
						}
                        if (cntAll % 10000 == 0) {
							spinner.update(cntAll);
						}
					}
				} finally {
					qResultA.close();
                    spinner.terminate();
				}
			} catch (Exception e) {
				LOG.logError("link_between", "Error in linking parents of newborns to partners in process " + this.processName);
				e.printStackTrace();
			} finally {
				if(closeStream == true) {
					LINKS.closeStream();
				}
			}
		}
	}

	/**
	 * Check whether the time span between related events is plausable
	 */
	public int checkTimeConsistency(int eventADate, int eventBDate) {
		int diff = eventBDate - eventADate;  // direction matters

        Facts facts = new Facts();
        facts.put("diff", diff);

        Rule rule = this.rules.get("timegapdiff");
        if (rule == null || rule.evaluate(facts)) {
            return diff;
        }

        return 999;
    }

	public boolean checkTimeConsistencyWithAge(int registrationDifference, int age) {
		if(age >= 0) {
            int diff = Math.abs(registrationDifference - age);

            Facts facts = new Facts();
            facts.put("diff", diff);

            Rule rule = this.rules.get("consistencytolerance");
            if (rule != null && !rule.evaluate(facts)) {
				return false;
			}
		}

        return true;
	}
}
