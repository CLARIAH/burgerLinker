package nl.knaw.iisg.burgerlinker.processes;

import java.io.File;
import java.lang.Math;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
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

	public void link_within(Person.Gender gender, boolean closeStream) {
        String familyCode = (gender == Person.Gender.MALE) ? "22" : "21";

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
                int cntLinkParents = 0;
                int cntLinkFather = 0;
                int cntLinkMother = 0;
                int cntLinksFailed = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(".: Linking " + processName + " (" + gender + ")");
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();

						Person newborn = new Person(event,
                                                bindingSetA.getValue("subjectGivenName"),
                                                bindingSetA.getValue("subjectFamilyName"),
                                                bindingSetA.getValue("subjectGender"));
						if (!newborn.isValidWithFullName() || !newborn.hasGender(gender)) {
                            // incomplete or wrong gender
                            continue;
                        }

                        Person mother = new Person(event,
                                               bindingSetA.getValue("subjectMotherGivenName"),
                                               bindingSetA.getValue("subjectMotherFamilyName"),
                                               bindingSetA.getValue("subjectMotherGender"));
                        Person father = new Person(event,
                                               bindingSetA.getValue("subjectFatherGivenName"),
                                               bindingSetA.getValue("subjectFatherFamilyName"),
                                               bindingSetA.getValue("subjectFatherGender"));

                        CandidateList candidatesSubjectB=null, candidatesMother=null, candidatesFather=null;
                        if (mother.isValidWithFullName() || father.isValidWithFullName()) {
                            // collate events of who the main partcipant match the subject's name
                            candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, event, ignoreBlock);
                            if (candidatesSubjectB.candidates.isEmpty()) {
                                continue;
                            }

                            int subjectAMotherAge = myRDF.valueToInt(bindingSetA.getValue("subjectMotherAge"));
                            int subjectAFatherAge = myRDF.valueToInt(bindingSetA.getValue("subjectFatherage"));

                            if (mother.isValidWithFullName()) {
                                // collate events of who the female partcipant match the mother's name
                                candidatesMother = indexMother.searchForCandidate(mother, event, ignoreBlock);
                                if (!candidatesMother.candidates.isEmpty()) {
                                    // events of who both the subject's and its mother's name match those of the participants
                                    Set<String> finalCandidatesMother = candidatesSubjectB.findIntersectionCandidates(candidatesMother);
                                    for (String subjectBEventURI: finalCandidatesMother) {
                                        if (father.isValidWithFullName()
                                            && candidatesSubjectB.candidates.get(subjectBEventURI).individualsInCertificate.contains("F")) {
                                            continue; // if both have fathers, but their names did not match
                                        }

                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("subjectGivenName"),
                                                                         bindingSetB.getValue("subjectFamilyName"),
                                                                         bindingSetB.getValue("subjectGender"));
                                                Person subjectBMother = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("subjectMotherGivenName"),
                                                                               bindingSetB.getValue("subjectMotherFamilyName"),
                                                                               bindingSetB.getValue("subjectMotherGender"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));
                                                    int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("subjectMotherAge"));
                                                    int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff)) {
                                                        LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, subjectBEventURI,
                                                                                      subjectB, subjectBMother, familyCode, yearDifference);
                                                        cntLinkMother++;
                                                        }
                                                } else {
                                                    LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, subjectBEventURI,
                                                                                  subjectB, subjectBMother, familyCode, yearDifference);
                                                    cntLinkMother++;
                                                }
                                            } else {
                                                cntLinksFailed++;
                                            }
                                        }
                                        qResultB.close();
                                    }
                                }
                            }

                            if (father.isValidWithFullName()) {
                                // collate events of who the male partcipant match the father's name
                                candidatesFather = indexFather.searchForCandidate(father, event, ignoreBlock);
                                if (!candidatesFather.candidates.isEmpty()) {
                                    // events of who both the subject's and its father's name match those of the participants
                                    Set<String> finalCandidatesFather = candidatesSubjectB.findIntersectionCandidates(candidatesFather);
                                    for (String subjectBEventURI: finalCandidatesFather) {
                                        if (mother.isValidWithFullName()
                                            && candidatesSubjectB.candidates.get(subjectBEventURI).individualsInCertificate.contains("M")) {
                                            continue; // if both have mothers, but their names did not match
                                        }

                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("subjectGivenName"),
                                                                         bindingSetB.getValue("subjectFamilyName"),
                                                                         bindingSetB.getValue("subjectGender"));
                                                Person subjectBFather = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("subjectFatherGivenName"),
                                                                               bindingSetB.getValue("subjectFatherFamilyName"),
                                                                               bindingSetB.getValue("subjectFatherGender"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));
                                                    int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("subjectFatherAge"));
                                                    int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
                                                        LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, subjectBEventURI,
                                                                                      subjectB, subjectBFather, familyCode, yearDifference);
                                                        cntLinkFather++;
                                                    }
                                                } else {
                                                    LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, subjectBEventURI,
                                                                                  subjectB, subjectBFather, familyCode, yearDifference);
                                                    cntLinkFather++;
                                                }
                                            } else {
                                                cntLinksFailed++;
                                            }
                                        }
                                        qResultB.close();
                                    }
								}
							}

                            if (mother.isValidWithFullName() && father.isValidWithFullName()) {
                                if (!candidatesMother.candidates.isEmpty() && !candidatesFather.candidates.isEmpty()) {
                                    // collate events of who the subject and its parents match names
                                    Set<String> finalCandidatesMotherFather = candidatesSubjectB.findIntersectionCandidates(candidatesMother,
                                                                                                                            candidatesFather);
                                    for (String subjectBEventURI: finalCandidatesMotherFather) {
                                        Map<String, Value> bindings = new HashMap<>();
                                        bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                        TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                        for (BindingSet bindingSetB: qResultB) {
                                            int yearDifference = 0;
                                            if (!ignoreDate) {
                                                LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                                LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                            }
                                            if (yearDifference < 999) { // if it fits the time line
                                                Person subjectB = new Person(subjectBEventURI,
                                                                         bindingSetB.getValue("subjectGivenName"),
                                                                         bindingSetB.getValue("subjectFamilyName"),
                                                                         bindingSetB.getValue("subjectGender"));
                                                Person subjectBMother = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("subjectMotherGivenName"),
                                                                               bindingSetB.getValue("subjectMotherFamilyName"),
                                                                               bindingSetB.getValue("subjectMotherGender"));
                                                Person subjectBFather = new Person(subjectBEventURI,
                                                                               bindingSetB.getValue("subjectFatherGivenName"),
                                                                               bindingSetB.getValue("subjectFatherFamilyName"),
                                                                               bindingSetB.getValue("subjectFatherGender"));

                                                if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));

                                                    int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("subjectMotherAge"));
                                                    int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("subjectFatherAge"));

                                                    int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                                    int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectBAge) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff) &&
                                                        checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
                                                            LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather,
                                                                                   subjectBEventURI, subjectB, subjectBMother, subjectBFather,
                                                                                   familyCode, yearDifference);
                                                            cntLinkParents++;
                                                    }
                                                } else {
                                                    LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather,
                                                                           subjectBEventURI, subjectB, subjectBMother, subjectBFather,
                                                                           familyCode, yearDifference);
                                                    cntLinkParents++;
                                                }
                                            } else {
                                                cntLinksFailed++;
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

                    DecimalFormat formatter = new DecimalFormat("#,###");
                    LinkedHashMap<String, String> summary = new LinkedHashMap<>();

                    int cntLinkTotal = cntLinkParents + cntLinkFather + cntLinkMother;
                    summary.put("Candidate Links Discovered", formatter.format(cntLinkTotal+cntLinksFailed));
                    if (!ignoreDate) {
                        summary.put(" which passed validation", formatter.format(cntLinkTotal));
                    }
                    summary.put("  matching both parents", formatter.format(cntLinkParents));
                    summary.put("  matching mothers only", formatter.format(cntLinkMother));
                    summary.put("  matching fathers only", formatter.format(cntLinkFather));

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

                    LOG.outputConsole(".: Process Summary");
                    for (String key: summary.keySet()) {
                        String val = summary.get(key);
                        LOG.outputConsole("   - " + String.format("%-" + keyLenMax + "s", key)
                                          + "   " + String.format("%" + valLenMax + "s", val));
                    }
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

	public void link_within_single(Person.Gender gender, boolean closeStream) {
        String familyCode = (gender == Person.Gender.MALE) ? "22" : "21";

        String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
        String queryEventB = MyRDF.generalizeQuery(process.queryEventB, gender);
        boolean genderFilter = (this.process.type == Process.ProcessType.BIRTH_DECEASED);

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

						Person newborn = new Person(event,
                                                bindingSetA.getValue("subjectGivenName"),
                                                bindingSetA.getValue("subjectFamilyName"),
                                                bindingSetA.getValue("subjectGender"));
						if (newborn.isValidWithFullName() && newborn.hasGender(gender)) {
                            CandidateList candidatesSubjectB=null;

                            candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, event, ignoreBlock);
                            if (!candidatesSubjectB.candidates.isEmpty()) {
                                for (String subjectBEventURI: candidatesSubjectB.candidates.keySet()) {
                                    Map<String, Value> bindings = new HashMap<>();
                                    bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                    TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                    for (BindingSet bindingSetB: qResultB) {
                                        int yearDifference = 0;
                                        if (!ignoreDate) {
                                            LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));
                                            LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                            yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                        }
                                        if (yearDifference < 999) { // if it fits the time line
                                            Person subjectB = new Person(subjectBEventURI,
                                                                     bindingSetB.getValue("subjectGivenName"),
                                                                     bindingSetB.getValue("subjectFamilyName"),
                                                                     bindingSetB.getValue("subjectGender"));

                                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));
                                                if (checkTimeConsistencyWithAge(yearDifference, subjectBAge)) {
                                                    LINKS.saveLinks_Within_single(candidatesSubjectB, subjectBEventURI, subjectB,
                                                                                  familyCode, yearDifference);
                                                }
                                            } else {
                                                LINKS.saveLinks_Within_single(candidatesSubjectB, subjectBEventURI, subjectB,
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
        if (eventADate == null || eventBDate == null) {
            return 999;
        }

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
