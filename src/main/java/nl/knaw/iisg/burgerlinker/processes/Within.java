package nl.knaw.iisg.burgerlinker.processes;

import java.io.File;
import java.lang.Math;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rule;
import org.eclipse.rdf4j.model.Value;
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
        String familyCode = (gender.equals("Male")) ? "22" : "21";

        String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
        String queryEventB = MyRDF.generalizeQuery(process.queryEventB, gender);
        boolean genderFilter = (this.process.type == Process.ProcessType.BIRTH_DECEASED);

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
		if (dict.generateDictionaryThreeWay(myRDF, queryEventB, genderFilter, gender)) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			try {
				int cntAll = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(".: Linking " + processName + " (" + gender + ")");
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();
						String eventID = bindingSetA.getValue("eventID").stringValue();

						Person newborn = new Person(event,
                                                bindingSetA.getValue("givenNameSubject"),
                                                bindingSetA.getValue("familyNameSubject"),
                                                bindingSetA.getValue("genderSubject"));
						if (!newborn.isValidWithFullName() || !newborn.hasGender(gender)) {
                            // incomplete or wrong gender
                            continue;
                        }

                        Person mother = new Person(event,
                                               bindingSetA.getValue("givenNameSubjectMother"),
                                               bindingSetA.getValue("familyNameSubjectMother"),
                                               bindingSetA.getValue("genderSubjectMother"));
                        Person father = new Person(event,
                                               bindingSetA.getValue("givenNameSubjectFather"),
                                               bindingSetA.getValue("familyNameSubjectFather"),
                                               bindingSetA.getValue("genderSubjectFather"));

                        CandidateList candidatesSubjectB=null, candidatesMother=null, candidatesFather=null;
                        if (mother.isValidWithFullName() || father.isValidWithFullName()) {
                            candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, eventID, ignoreBlock);
                            if (candidatesSubjectB.candidates.isEmpty()) {
                                continue;
                            }

                            int subjectAMotherAge = myRDF.valueToInt(bindingSetA.getValue("ageSubjectMother"));
                            int subjectAFatherAge = myRDF.valueToInt(bindingSetA.getValue("ageSubjectFather"));

                            if (mother.isValidWithFullName()) {
                                candidatesMother = indexMother.searchForCandidate(mother, eventID, ignoreBlock);
                                if (!candidatesMother.candidates.isEmpty()) {
                                    Set<String> finalCandidatesMother = candidatesSubjectB.findIntersectionCandidates(candidatesMother);
                                    for (String finalCandidate: finalCandidatesMother) {
                                        if (father.isValidWithFullName()
                                            && candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("F")) {
                                            continue; // if both have fathers, but their names did not match
                                        }

                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("eventID", MyRDF.mkLiteral(finalCandidate, "int"));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("givenNameSubject"),
                                                                         bindingSetB.getValue("familyNameSubject"),
                                                                         bindingSetB.getValue("genderSubject"));
                                                Person subjectBMother = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("givenNameSubjectMother"),
                                                                               bindingSetB.getValue("familyNameSubjectMother"),
                                                                               bindingSetB.getValue("genderSubjectMother"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                    int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectMother"));
                                                    int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff)) {
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

                            if (father.isValidWithFullName()) {
                                candidatesFather = indexFather.searchForCandidate(father, eventID, ignoreBlock);
                                if (!candidatesFather.candidates.isEmpty()) {
                                    Set<String> finalCandidatesFather = candidatesSubjectB.findIntersectionCandidates(candidatesFather);
                                    for (String finalCandidate: finalCandidatesFather) {
                                        if (mother.isValidWithFullName()
                                            && candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("M")) {
                                            continue; // if both have mothers, but their names did not match
                                        }

                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("eventID", MyRDF.mkLiteral(finalCandidate, "int"));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("givenNameSubject"),
                                                                         bindingSetB.getValue("familyNameSubject"),
                                                                         bindingSetB.getValue("genderSubject"));
                                                Person subjectBFather = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("givenNameSubjectFather"),
                                                                               bindingSetB.getValue("familyNameSubjectFather"),
                                                                               bindingSetB.getValue("genderSubjectFather"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                    int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectFather"));
                                                    int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
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

                            if (mother.isValidWithFullName() && father.isValidWithFullName()) {
                                if (!candidatesMother.candidates.isEmpty() && !candidatesFather.candidates.isEmpty()) {
                                    Set<String> finalCandidatesMotherFather = candidatesSubjectB.findIntersectionCandidates(candidatesMother,
                                                                                                                            candidatesFather);
                                    for (String finalCandidate: finalCandidatesMotherFather) {
                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("eventID", MyRDF.mkLiteral(finalCandidate, "int"));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("givenNameSubject"),
                                                                         bindingSetB.getValue("familyNameSubject"),
                                                                         bindingSetB.getValue("genderSubject"));
                                                Person subjectBMother = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("givenNameSubjectMother"),
                                                                               bindingSetB.getValue("familyNameSubjectMother"),
                                                                               bindingSetB.getValue("genderSubjectMother"));
                                                Person subjectBFather = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("givenNameSubjectFather"),
                                                                               bindingSetB.getValue("familyNameSubjectFather"),
                                                                               bindingSetB.getValue("genderSubjectFather"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));

                                                    int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectMother"));
                                                    int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("ageSubjectFather"));

                                                    int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                                    int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
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
                        if (cntAll % 5000 == 0) {
							spinner.update(cntAll);
						}
					}
				} finally {
					qResultA.close();
                    spinner.terminate();
                    spinner.join();
				}
			} catch (Exception e) {
				LOG.logError("link_within", "Error in linking subjects in event A to subjects in event B in process " + this.processName);
				e.printStackTrace();
			} finally {
				if(closeStream == true) {
					LINKS.closeStream();
				}
			}
		}
	}

	public void link_within_single(String gender, boolean closeStream) {
        String familyCode = (gender.equals("Male")) ? "22" : "21";

        String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
        String queryEventB = MyRDF.generalizeQuery(process.queryEventB, gender);
        boolean genderFilter = (this.process.type == Process.ProcessType.BIRTH_DECEASED) ? true : false;

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
		if(dict.generateDictionaryOneWay(myRDF, queryEventB, genderFilter, gender)) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			try {
				String taskName = ".: Linking Single " + processName + " (" + gender + ")";
				int cntAll = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(taskName);
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();
						String eventID = bindingSetA.getValue("eventID").stringValue();

						Person newborn = new Person(event,
                                                bindingSetA.getValue("givenNameSubject"),
                                                bindingSetA.getValue("familyNameSubject"),
                                                bindingSetA.getValue("genderSubject"));
						if (newborn.isValidWithFullName() && newborn.hasGender(gender)) {
                            CandidateList candidatesSubjectB=null;

                            candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, eventID, ignoreBlock);
                            if (!candidatesSubjectB.candidates.isEmpty()) {
                                for (String finalCandidate: candidatesSubjectB.candidates.keySet()) {
                                    Map<String, Value> bindings = new HashMap<>();
                                    bindings.put("eventID", MyRDF.mkLiteral(finalCandidate, "int"));

                                    TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                    for (BindingSet bindingSetB: qResultB) {
                                        String subjectBEventURI = bindingSetB.getValue("event").stringValue();

                                        int yearDifference = 0;
                                        if (!ignoreDate) {
                                            LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                            LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                            yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                        }
                                        if (yearDifference < 999) { // if it fits the time line
                                            Person subjectB = new Person(subjectBEventURI,
                                                                     bindingSetB.getValue("givenNameSubject"),
                                                                     bindingSetB.getValue("familyNameSubject"),
                                                                     bindingSetB.getValue("genderSubject"));

                                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("ageSubject"));
                                                if (checkTimeConsistencyWithAge(yearDifference, subjectBAge)) {
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
                        if (cntAll % 5000 == 0) {
							spinner.update(cntAll);
						}
					}
				} finally {
					qResultA.close();
                    spinner.terminate();
                    spinner.join();
				}
			} catch (Exception e) {
				LOG.logError("link_within", "Error in linking subjects in event A to subjects in event B in process " + this.processName);
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
   	public int checkTimeConsistency(LocalDate eventADate, LocalDate eventBDate) {
        int diff = (int) ChronoUnit.YEARS.between(eventADate, eventBDate);

        Facts facts = new Facts();
        facts.put("diff", diff);

        Rule rule = this.rules.get("timegapdiff");
        if (rule == null || rule.evaluate(facts)) {
            return diff;
        }

        return 999;
    }

	public boolean checkTimeConsistencyWithAge(int registrationDifference, int age) {
		if (age >= 0) {
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

    public int ageDifference(int ageBefore, int ageAfter) {
        int out = -1;
        if (ageBefore >= 0 && ageAfter >=0 && ageBefore <= ageAfter) {
            out = ageAfter - ageBefore;
        }

        return out;
    }
}
