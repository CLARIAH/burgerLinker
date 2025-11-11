package nl.knaw.iisg.burgerlinker.processes;


import java.io.File;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rule;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;

import nl.knaw.iisg.burgerlinker.data.LinksCSV;
import nl.knaw.iisg.burgerlinker.data.MyRDF;
import nl.knaw.iisg.burgerlinker.core.CandidateList;
import nl.knaw.iisg.burgerlinker.core.Dictionary;
import nl.knaw.iisg.burgerlinker.core.Index;
import nl.knaw.iisg.burgerlinker.structs.Couple;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class Between {
	private MyRDF myRDF;
    private Process process;
    private Map<String, Rule> rules;
	private File mainDirectoryPath;
    private String processName;
	private int maxLev;
	private boolean fixedLev, ignoreDate, ignoreBlock;
	Index indexFemale, indexMale;

	public static final Logger lg = LogManager.getLogger(Between.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	LinksCSV LINKS;

	public Between(MyRDF myRDF, Process process, Map<String, Rule> rules, File directoryPath,
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

        // setup output format
		String options = LOG.getUserOptions(this.maxLev, this.fixedLev, singleInd,
                                            this.ignoreDate, this.ignoreBlock);
		String resultsFileName = this.processName + options;
		if(formatCSV == true) {
			LINKS = new LinksCSV(resultsFileName, this.mainDirectoryPath, this.process.csvHeader);
		}
	}

	public void link_between() {
        String queryEventA = MyRDF.generalizeQuery(process.queryEventA);
        String queryEventB = MyRDF.generalizeQuery(process.queryEventB);

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
        if (dict.generateDictionaryTwoWay(myRDF, queryEventB)) {
			indexMale = dict.indexMale;
			indexFemale = dict.indexFemale;
			indexMale.createTransducer();
			indexFemale.createTransducer();
			try {
				int cntAll = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(".: Linking " + processName);
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();
						String eventID = bindingSetA.getValue("eventID").stringValue();

                        Set<Couple> couples = new HashSet<>();
                        if (process.type == Process.ProcessType.MARRIAGE_MARRIAGE) {
                            Couple otherCouple = new Couple(
                                    new Person(event,
                                           bindingSetA.getValue("givenNamePartnerMother"),
                                           bindingSetA.getValue("familyNamePartnerMother"),
                                           bindingSetA.getValue("genderPartnerMother")),
                                    new Person(event,
                                           bindingSetA.getValue("givenNamePartnerFather"),
                                           bindingSetA.getValue("familyNamePartnerFather"),
                                           bindingSetA.getValue("genderPartnerFather")),
                                    "22");  // groom
                            couples.add(otherCouple);
                        }

                        // default couple
                        Couple couple = new Couple(
                                new Person(event,
                                       bindingSetA.getValue("givenNameSubjectMother"),
                                       bindingSetA.getValue("familyNameSubjectMother"),
                                       bindingSetA.getValue("genderSubjectMother")),
                                new Person(event,
                                       bindingSetA.getValue("givenNameSubjectFather"),
                                       bindingSetA.getValue("familyNameSubjectFather"),
                                       bindingSetA.getValue("genderSubjectFather")),
                                "21");  // bride (only relevant if event A is a marriage)
                        couples.add(couple);

						Person mother, father;
                        for (Couple c: couples) {
                            mother = c.mother;
                            father = c.father;

                            if (mother.isValidWithFullName() && father.isValidWithFullName()) {
                                // start linking here
                                CandidateList candidatesMale = indexMale.searchForCandidate(father, eventID, this.ignoreBlock);
                                if (!candidatesMale.candidates.isEmpty()) {
                                    CandidateList candidatesFemale = indexFemale.searchForCandidate(mother, eventID, this.ignoreBlock);

                                    if (!candidatesFemale.candidates.isEmpty()) {
                                        Set<String> finalCandidatesList = candidatesFemale.findIntersectionCandidates(candidatesMale);

                                        for (String finalCandidate: finalCandidatesList) {
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
                                                    Person subjectBPartner = new Person(subjectBEventURI,
                                                            bindingSetB.getValue("givenNamePartner"),
                                                            bindingSetB.getValue("familyNamePartner"),
                                                            bindingSetB.getValue("genderPartner"));

                                                    // determine order
                                                    Person subjectBFemale, subjectBMale;
                                                    if (subjectB.isFemale()) {
                                                        subjectBFemale = subjectB;
                                                        subjectBMale = subjectBPartner;
                                                    } else if (subjectB.isMale()) {
                                                        subjectBFemale = subjectBPartner;
                                                        subjectBMale = subjectB;
                                                    } else {
                                                        continue;
                                                    }

                                                    String familyCode = "N.A.";
                                                    if (process.type == Process.ProcessType.MARRIAGE_MARRIAGE) {
                                                        familyCode = c.familyCode;
                                                    }

                                                    LINKS.saveLinks_Between(candidatesFemale, candidatesMale, finalCandidate,
                                                                            subjectBFemale, subjectBMale, familyCode,
                                                                            yearDifference);
                                                }
                                            }
                                            qResultB.close();
                                        }
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
				LOG.logError("link_between", "Linking Error in Process " + processName);
				e.printStackTrace();
			} finally {
				LINKS.closeStream();
			}
		}
	}

	/**
	 * Check whether the time span between related events is plausable
	 */
	public int checkTimeConsistency(LocalDate eventADate, LocalDate eventBDate) {
        int diff;
        if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
            diff = (int) ChronoUnit.YEARS.between(eventADate, eventBDate);
        } else {
            diff = (int) ChronoUnit.YEARS.between(eventBDate, eventADate);
        }

        Facts facts = new Facts();
        facts.put("diff", diff);

        Rule rule = this.rules.get("timegapdiff");
        if (rule == null || rule.evaluate(facts)) {
            return diff;
        }

        return 999;
    }
}
