package nl.knaw.iisg.burgerlinker.processes;


import java.io.File;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
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
                int cntLinks = 0;
                int cntLinksTimeInconsistent = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(".: Linking " + processName);
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;

                        String event = bindingSetA.getValue("event").stringValue();

                        Set<Couple> couples = new HashSet<>();
                        if (process.type == Process.ProcessType.MARRIAGE_MARRIAGE) {
                            Couple otherCouple = new Couple(
                                    new Person(event,
                                           bindingSetA.getValue("partnerMotherGivenName"),
                                           bindingSetA.getValue("partnerMotherFamilyName"),
                                           bindingSetA.getValue("partnerMotherGender")),
                                    new Person(event,
                                           bindingSetA.getValue("partnerFatherGivenName"),
                                           bindingSetA.getValue("partnerFatherFamilyName"),
                                           bindingSetA.getValue("partnerFatherGender")),
                                    "22");  // groom
                            couples.add(otherCouple);
                        }

                        // default couple
                        Couple couple = new Couple(
                                new Person(event,
                                       bindingSetA.getValue("subjectMotherGivenName"),
                                       bindingSetA.getValue("subjectMotherFamilyName"),
                                       bindingSetA.getValue("subjectMotherGender")),
                                new Person(event,
                                       bindingSetA.getValue("subjectFatherGivenName"),
                                       bindingSetA.getValue("subjectFatherFamilyName"),
                                       bindingSetA.getValue("subjectFatherGender")),
                                "21");  // bride (only relevant if event A is a marriage)
                        couples.add(couple);

						Person mother, father;
                        for (Couple c: couples) {
                            mother = c.mother;
                            father = c.father;

                            if (mother.isValidWithFullName() && father.isValidWithFullName()) {
                                // collate events of who the male partcipant match the father's name
                                CandidateList candidatesMale = indexMale.searchForCandidate(father, event, this.ignoreBlock);
                                if (!candidatesMale.candidates.isEmpty()) {
                                    // collate events of who the male partcipant match the mother's name
                                    CandidateList candidatesFemale = indexFemale.searchForCandidate(mother, event, this.ignoreBlock);

                                    if (!candidatesFemale.candidates.isEmpty()) {
                                        // events of who both the male and female names match the subject parents' names
                                        Set<String> finalCandidatesList = candidatesFemale.findIntersectionCandidates(candidatesMale);

                                        for (String subjectBEventURI: finalCandidatesList) {  // candidate events
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
                                                    Person subjectBPartner = new Person(subjectBEventURI,
                                                            bindingSetB.getValue("partnerGivenName"),
                                                            bindingSetB.getValue("partnerFamilyName"),
                                                            bindingSetB.getValue("partnerGender"));

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

                                                    LINKS.saveLinks_Between(candidatesFemale, candidatesMale, subjectBEventURI,
                                                                            subjectBFemale, subjectBMale, familyCode,
                                                                            yearDifference);

                                                    cntLinks++;
                                                } else {
                                                    cntLinksTimeInconsistent++;
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

                    DecimalFormat formatter = new DecimalFormat("#,###");
                    LinkedHashMap<String, String> summary = new LinkedHashMap<>();

                    int cntLinksTotal = cntLinks + cntLinksTimeInconsistent;
                    summary.put("Candidate Links Discovered", formatter.format(cntLinksTotal));
                    if (!ignoreDate) {
                        summary.put(" which passed validation", formatter.format(cntLinks));
                    }

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
        if (eventADate == null || eventBDate == null) {
            return 999;
        }

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
