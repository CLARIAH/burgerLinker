package nl.knaw.iisg.burgerlinker.processes;

import java.io.File;
import java.lang.Math;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.ArrayList;
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
	Index indexSubjectB, indexMother, indexFather, indexPartner, indexPartnerMother, indexPartnerFather;

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
                                            // this event has a known father so skip single parent check
                                            continue;
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
                                            // this event has a known mother so skip single parent check
                                            continue;
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

	public void link_within_couple(Person.Gender gender, boolean closeStream) {
        String familyCode = (gender == Person.Gender.MALE) ? "22" : "21";

        // assume spouse genders remain constant between events
        String queryEventA = MyRDF.generalizeQuery(process.queryEventA, gender);
        String queryEventB = MyRDF.generalizeQuery(process.queryEventB, gender);

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);
		if (dict.generateDictionarySixWay(myRDF, queryEventB, gender)) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			indexPartner = dict.indexPartner; indexPartner.createTransducer();
			indexPartnerMother = dict.indexPartnerMother; indexPartnerMother.createTransducer();
			indexPartnerFather = dict.indexPartnerFather; indexPartnerFather.createTransducer();
			try {
				int cntAll = 0;
                int cntLinkFailed = 0;
                int cntLinkDuplicates = 0;
                int cntLinkDivorceWeakMatch = 0;
                int cntLinkDivorceStrongMatch = 0;
                int cntLinkRemarriageWeakMatch = 0;
                int cntLinkRemarriageStrongMatch = 0;

                TupleQueryResult qResultA = null;
                ActivityIndicator spinner = new ActivityIndicator(".: Linking " + processName + " (" + gender + ")");
				try {
                    spinner.start();

                    qResultA = myRDF.getQueryResults(queryEventA);
                    for (BindingSet bindingSetA: qResultA) {
						cntAll++;
                        if (cntAll % 5000 == 0) {
                            spinner.update(cntAll);
                        }

                        String subjectAEventURI = bindingSetA.getValue("event").stringValue();

						Person subjectA = new Person(subjectAEventURI,
                                                bindingSetA.getValue("subjectGivenName"),
                                                bindingSetA.getValue("subjectFamilyName"),
                                                bindingSetA.getValue("subjectGender"));
						if (!subjectA.isValidWithFullName() || !subjectA.hasGender(gender)) {
                            // incomplete or wrong gender
                            continue;
                        }

                        Person subjectAMother = new Person(subjectAEventURI,
                                               bindingSetA.getValue("subjectMotherGivenName"),
                                               bindingSetA.getValue("subjectMotherFamilyName"),
                                               bindingSetA.getValue("subjectMotherGender"));
                        Person subjectAFather = new Person(subjectAEventURI,
                                               bindingSetA.getValue("subjectFatherGivenName"),
                                               bindingSetA.getValue("subjectFatherFamilyName"),
                                               bindingSetA.getValue("subjectFatherGender"));
                        Person subjectAPartner = new Person(subjectAEventURI,
                                               bindingSetA.getValue("partnerGivenName"),
                                               bindingSetA.getValue("partnerFamilyName"),
                                               bindingSetA.getValue("partnerGender"));
                        Person subjectAPartnerMother = new Person(subjectAEventURI,
                                               bindingSetA.getValue("partnerMotherGivenName"),
                                               bindingSetA.getValue("partnerMotherFamilyName"),
                                               bindingSetA.getValue("partnerMotherGender"));
                        Person subjectAPartnerFather = new Person(subjectAEventURI,
                                               bindingSetA.getValue("partnerFatherGivenName"),
                                               bindingSetA.getValue("partnerFatherFamilyName"),
                                               bindingSetA.getValue("partnerFatherGender"));

                        if (subjectAMother.isValidWithFullName() || subjectAFather.isValidWithFullName() || subjectAPartner.isValidWithFullName()) {
                            // collate events of who the main partcipant match the subject's name
                            CandidateList candidatesSubjectB = indexSubjectB.searchForCandidate(subjectA, subjectAEventURI, ignoreBlock);
                            if (candidatesSubjectB.candidates.isEmpty()) {
                                continue;
                            }

                            LocalDate eventADate = myRDF.valueToDate(bindingSetA.getValue("eventDate"));

                            int subjectAAge = myRDF.valueToInt(bindingSetA.getValue("subjectAge"));
                            int subjectAMotherAge = -1, subjectAFatherAge = -1;

                            if (subjectAPartner.isValidWithFullName()) {
                                int subjectAPartnerAge = myRDF.valueToInt(bindingSetA.getValue("partnerAge"));
                                int subjectAPartnerMotherAge = -1, subjectAPartnerFatherAge = -1;

                                // collate events of who the partner match on name
                                CandidateList candidatesPartner = indexPartner.searchForCandidate(subjectAPartner, subjectAEventURI, ignoreBlock);
                                if (!candidatesPartner.candidates.isEmpty()) {
                                    Set<String> candidateDivorceEvents = candidatesSubjectB.findIntersectionCandidates(candidatesPartner);
                                    if (!candidateDivorceEvents.isEmpty()) {  // divorce
                                        // collate events of who the relatives match on name
                                        CandidateList candidatesMother = null;
                                        if (subjectAMother.isValidWithFullName()) {
                                            candidatesMother = indexMother.searchForCandidate(subjectAMother, subjectAEventURI, ignoreBlock);
                                            subjectAMotherAge = myRDF.valueToInt(bindingSetA.getValue("subjectMotherAge"));
                                        }
                                        CandidateList candidatesFather = null;
                                        if (subjectAFather.isValidWithFullName()) {
                                            candidatesFather = indexFather.searchForCandidate(subjectAFather, subjectAEventURI, ignoreBlock);
                                            subjectAFatherAge = myRDF.valueToInt(bindingSetA.getValue("subjectFatherage"));
                                        }
                                        CandidateList candidatesPartnerMother = null;
                                        if (subjectAPartnerMother.isValidWithFullName()) {
                                            candidatesPartnerMother = indexPartnerMother.searchForCandidate(subjectAPartnerMother, subjectAEventURI, ignoreBlock);
                                            subjectAPartnerMotherAge = myRDF.valueToInt(bindingSetA.getValue("partnerMotherAge"));
                                        }
                                        CandidateList candidatesPartnerFather = null;
                                        if (subjectAPartnerFather.isValidWithFullName()) {
                                            candidatesPartnerFather = indexPartnerFather.searchForCandidate(subjectAPartnerFather, subjectAEventURI, ignoreBlock);
                                            subjectAPartnerFatherAge = myRDF.valueToInt(bindingSetA.getValue("partnerFatherage"));
                                        }

                                        // find events that are shared by all participants
                                        List<CandidateList> candidateLists = new ArrayList<>();
                                        for (CandidateList candidateList: Arrays.asList(candidatesMother,
                                                                                        candidatesFather,
                                                                                        candidatesPartner,
                                                                                        candidatesPartnerMother,
                                                                                        candidatesPartnerFather)) {
                                            if (!(candidateList == null || candidateList.candidates.isEmpty())) {
                                                candidateLists.add(candidateList);
                                            }
                                        }
                                        Set<String> candidateEvents = candidatesSubjectB.findIntersectionCandidates(candidateLists);

                                        // for all events that include at least the marriage couple
                                        for (String subjectBEventURI: candidateEvents) {
                                            if (subjectBEventURI.equals(subjectAEventURI)) {
                                                continue;
                                            }

                                            Map<String, Value> bindings = new HashMap<>();
                                            bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                            TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                            for (BindingSet bindingSetB: qResultB) {
                                                Person subjectB = null, subjectBPartner = null,
                                                       subjectBMother = null, subjectBFather = null,
                                                       subjectBPartnerMother = null, subjectBPartnerFather= null;

                                                int yearDifference = 0;
                                                if (!ignoreDate) {
                                                    // check if events fit the time line
                                                    LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                                    yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                                    if (eventADate.equals(eventBDate)) {
                                                        cntLinkDuplicates += 1;
                                                    }
                                                }
                                                if (yearDifference < 999) {
                                                    // check time range of subject between events
                                                    int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));
                                                    int subjectAgeDiff = ageDifference(subjectAAge, subjectBAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectAgeDiff)) {
                                                        // valid match
                                                        subjectB = new Person(subjectBEventURI,
                                                                            bindingSetB.getValue("subjectGivenName"),
                                                                            bindingSetB.getValue("subjectFamilyName"),
                                                                            bindingSetB.getValue("subjectGender"));
                                                    }
                                                    if (subjectB == null) {
                                                        cntLinkFailed += 1;

                                                        continue;
                                                    }

                                                    // check time range of partner between events
                                                    int subjectBPartnerAge = myRDF.valueToInt(bindingSetB.getValue("partnerAge"));
                                                    int subjectPartnerAgeDiff = ageDifference(subjectAPartnerAge, subjectBPartnerAge);
                                                    if (checkTimeConsistencyWithAge(yearDifference, subjectPartnerAgeDiff)) {
                                                        // valid match
                                                        subjectBPartner = new Person(subjectBEventURI,
                                                                            bindingSetB.getValue("partnerGivenName"),
                                                                            bindingSetB.getValue("partnerFamilyName"),
                                                                            bindingSetB.getValue("partnerGender"));
                                                    }
                                                    if (subjectBPartner == null) {
                                                        cntLinkFailed += 1;

                                                        continue;
                                                    }

                                                    // check time range of relatives between events
                                                    if (!(candidatesMother == null || candidatesMother.candidates.isEmpty())) {
                                                        int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("subjectMotherAge"));
                                                        int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                                        if (checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff)) {
                                                            // valid match
                                                            subjectBMother = new Person(subjectBEventURI,
                                                                                bindingSetB.getValue("subjectMotherGivenName"),
                                                                                bindingSetB.getValue("subjectMotherFamilyName"),
                                                                                bindingSetB.getValue("subjectMotherGender"));
                                                        }
                                                    }
                                                    if (!(candidatesFather == null || candidatesFather.candidates.isEmpty())) {
                                                        int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("subjectFatherAge"));
                                                        int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                                        if (checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
                                                            // valid match
                                                            subjectBFather = new Person(subjectBEventURI,
                                                                                bindingSetB.getValue("subjectFatherGivenName"),
                                                                                bindingSetB.getValue("subjectFatherFamilyName"),
                                                                                bindingSetB.getValue("subjectFatherGender"));
                                                        }
                                                    }
                                                    if (!(candidatesPartnerMother == null || candidatesPartnerMother.candidates.isEmpty())) {
                                                        int subjectBPartnerMotherAge = myRDF.valueToInt(bindingSetB.getValue("partnerMotherAge"));
                                                        int subjectPartnerMotherAgeDiff = ageDifference(subjectAPartnerMotherAge, subjectBPartnerMotherAge);
                                                        if (checkTimeConsistencyWithAge(yearDifference, subjectPartnerMotherAgeDiff)) {
                                                            // valid match
                                                            subjectBPartnerMother = new Person(subjectBEventURI,
                                                                                bindingSetB.getValue("partnerMotherGivenName"),
                                                                                bindingSetB.getValue("partnerMotherFamilyName"),
                                                                                bindingSetB.getValue("partnerMotherGender"));
                                                        }
                                                    }
                                                    if (!(candidatesPartnerFather == null || candidatesPartnerFather.candidates.isEmpty())) {
                                                        int subjectBPartnerFatherAge = myRDF.valueToInt(bindingSetB.getValue("partnerFatherAge"));
                                                        int subjectPartnerFatherAgeDiff = ageDifference(subjectAPartnerFatherAge, subjectBPartnerFatherAge);
                                                        if (checkTimeConsistencyWithAge(yearDifference, subjectPartnerFatherAgeDiff)) {
                                                            // valid match
                                                            subjectBPartnerFather = new Person(subjectBEventURI,
                                                                                bindingSetB.getValue("partnerFatherGivenName"),
                                                                                bindingSetB.getValue("partnerFatherFamilyName"),
                                                                                bindingSetB.getValue("partnerFatherGender"));
                                                        }
                                                    }

                                                    // save matches to file
                                                    // NOTE: this procedure is executes twice, once per spouse, yet can be completed in a
                                                    //       single run by uncommenting the saveLinks calls below and by iterating over
                                                    //       both spouses in the remarriage section. This requires a lot of tweaking of the
                                                    //       variable names so that is a task for a future upgrade.
                                                    if (subjectBMother != null &&
                                                        subjectBFather != null &&
                                                        subjectBPartnerMother != null &&
                                                        subjectBPartnerFather != null) {
                                                        // full relatives match
                                                        LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather, subjectBEventURI,
                                                                               subjectB, subjectBMother, subjectBFather, familyCode, yearDifference);
                                                        // LINKS.saveLinks_Within(candidatesPartner, candidatesPartnerMother, candidatesPartnerFather,
                                                        //                        subjectBEventURI, subjectBPartner, subjectBPartnerMother, subjectBPartnerFather,
                                                        //                        familyCode, yearDifference);

                                                        cntLinkDivorceStrongMatch += 1;
                                                    } else if (subjectBMother != null) {
                                                        // at least one matching relative per person
                                                        if (subjectBPartnerMother != null) {
                                                            LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, subjectBEventURI,
                                                                                          subjectB, subjectBMother, familyCode, yearDifference);
                                                            // LINKS.saveLinks_Within_mother(candidatesPartner, candidatesPartnerMother, subjectBEventURI,
                                                            //                               subjectBPartner, subjectBPartnerMother, familyCode,
                                                            //                               yearDifference);

                                                            cntLinkDivorceWeakMatch += 1;
                                                        }
                                                        if (subjectBPartnerFather != null) {
                                                            LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, subjectBEventURI,
                                                                                          subjectB, subjectBMother, familyCode, yearDifference);
                                                            // LINKS.saveLinks_Within_father(candidatesPartner, candidatesPartnerFather, subjectBEventURI,
                                                            //                               subjectBPartner, subjectBPartnerFather, familyCode,
                                                            //                               yearDifference);

                                                            cntLinkDivorceWeakMatch += 1;
                                                        }
                                                    } else if (subjectBFather != null) {
                                                        // at least one matching relative per person
                                                        if (subjectBPartnerMother != null) {
                                                            LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, subjectBEventURI,
                                                                                          subjectB, subjectBFather, familyCode, yearDifference);
                                                            // LINKS.saveLinks_Within_mother(candidatesPartner, candidatesPartnerMother, subjectBEventURI,
                                                            //                               subjectBPartner, subjectBPartnerMother, familyCode,
                                                            //                               yearDifference);

                                                            cntLinkDivorceWeakMatch += 1;
                                                        }
                                                        if (subjectBPartnerFather != null) {
                                                            LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, subjectBEventURI,
                                                                                          subjectB, subjectBFather, familyCode, yearDifference);
                                                            // LINKS.saveLinks_Within_father(candidatesPartner, candidatesPartnerFather, subjectBEventURI,
                                                            //                               subjectBPartner, subjectBPartnerFather, familyCode,
                                                            //                               yearDifference);

                                                            cntLinkDivorceWeakMatch += 1;
                                                        }
                                                    } else {
                                                        cntLinkFailed += 1;
                                                    }
                                                }
                                            }
                                            qResultB.close();
                                        }
                                        // TODO people cannot both divorce and remarriage (to avoid duplicate matches) this should be changed
                                        continue;
                                    }
                                }
                            }

                            // re-marriage

                            // collate events of who the relatives match on name
                            CandidateList candidatesMother = null;
                            if (subjectAMother.isValidWithFullName()) {
                                candidatesMother = indexMother.searchForCandidate(subjectAMother, subjectAEventURI, ignoreBlock);
                                subjectAMotherAge = myRDF.valueToInt(bindingSetA.getValue("subjectMotherAge"));
                            }
                            CandidateList candidatesFather = null;
                            if (subjectAFather.isValidWithFullName()) {
                                candidatesFather = indexFather.searchForCandidate(subjectAFather, subjectAEventURI, ignoreBlock);
                                subjectAFatherAge = myRDF.valueToInt(bindingSetA.getValue("subjectFatherage"));
                            }

                            // find events that are shared by all participants
                            List<CandidateList> candidateLists = new ArrayList<>();
                            for (CandidateList candidateList: Arrays.asList(candidatesMother,
                                                                            candidatesFather)) {
                                if (!(candidateList == null || candidateList.candidates.isEmpty())) {
                                    candidateLists.add(candidateList);
                                }
                            }
                            Set<String> candidateEvents = candidatesSubjectB.findIntersectionCandidates(candidateLists);

                            // for all events that include at least one spouse from the original marriage couple
                            for (String subjectBEventURI: candidateEvents) {
                                if (subjectBEventURI.equals(subjectAEventURI)) {
                                    continue;
                                }

                                Map<String, Value> bindings = new HashMap<>();
                                bindings.put("event", MyRDF.mkIRI(subjectBEventURI));

                                TupleQueryResult qResultB = myRDF.getQueryResults(queryEventB, bindings);
                                for (BindingSet bindingSetB: qResultB) {
                                    Person subjectB = null, subjectBMother = null, subjectBFather = null;

                                    int yearDifference = 0;
                                    if (!ignoreDate) {
                                        // check if events fit the time line
                                        LocalDate eventBDate = myRDF.valueToDate(bindingSetB.getValue("eventDate"));
                                        yearDifference = checkTimeConsistency(eventADate, eventBDate);
                                        if (eventADate.equals(eventBDate)) {
                                            cntLinkDuplicates += 1;
                                        }
                                    }
                                    if (yearDifference < 999) {
                                        // check time range of subject between events
                                        int subjectBAge = myRDF.valueToInt(bindingSetB.getValue("subjectAge"));
                                        int subjectAgeDiff = ageDifference(subjectAAge, subjectBAge);
                                        if (checkTimeConsistencyWithAge(yearDifference, subjectAgeDiff)) {
                                            // valid match
                                            subjectB = new Person(subjectBEventURI,
                                                                bindingSetB.getValue("subjectGivenName"),
                                                                bindingSetB.getValue("subjectFamilyName"),
                                                                bindingSetB.getValue("subjectGender"));
                                        }
                                        if (subjectB == null) {
                                            cntLinkFailed += 1;

                                            continue;
                                        }

                                        // check time range of relatives between events
                                        if (!(candidatesMother == null || candidatesMother.candidates.isEmpty())) {
                                            int subjectBMotherAge = myRDF.valueToInt(bindingSetB.getValue("subjectMotherAge"));
                                            int subjectMotherAgeDiff = ageDifference(subjectAMotherAge, subjectBMotherAge);
                                            if (checkTimeConsistencyWithAge(yearDifference, subjectMotherAgeDiff)) {
                                                // valid match
                                                subjectBMother = new Person(subjectBEventURI,
                                                                    bindingSetB.getValue("subjectMotherGivenName"),
                                                                    bindingSetB.getValue("subjectMotherFamilyName"),
                                                                    bindingSetB.getValue("subjectMotherGender"));
                                            }
                                        }
                                        if (!(candidatesFather == null || candidatesFather.candidates.isEmpty())) {
                                            int subjectBFatherAge = myRDF.valueToInt(bindingSetB.getValue("subjectFatherAge"));
                                            int subjectFatherAgeDiff = ageDifference(subjectAFatherAge, subjectBFatherAge);
                                            if (checkTimeConsistencyWithAge(yearDifference, subjectFatherAgeDiff)) {
                                                // valid match
                                                subjectBFather = new Person(subjectBEventURI,
                                                                    bindingSetB.getValue("subjectFatherGivenName"),
                                                                    bindingSetB.getValue("subjectFatherFamilyName"),
                                                                    bindingSetB.getValue("subjectFatherGender"));
                                            }
                                        }

                                        // save matches to file
                                        if (subjectBMother != null && subjectBFather != null) {
                                            // full relatives match
                                            LINKS.saveLinks_Within(candidatesSubjectB, candidatesMother, candidatesFather,
                                                                   subjectBEventURI, subjectB, subjectBMother, subjectBFather,
                                                                   familyCode, yearDifference);

                                            cntLinkRemarriageStrongMatch += 1;
                                        } else if (subjectBMother != null) {
                                            // at least one matching relative
                                            LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, subjectBEventURI,
                                                                          subjectB, subjectBMother, familyCode, yearDifference);

                                            cntLinkRemarriageWeakMatch += 1;
                                        } else if (subjectBFather != null) {
                                            // at least one matching relative
                                            LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, subjectBEventURI,
                                                                          subjectB, subjectBFather, familyCode, yearDifference);

                                            cntLinkRemarriageWeakMatch += 1;
                                        } else {
                                            cntLinkFailed += 1;
                                        }
                                    }
                                }
                                qResultB.close();
                            }
						}
					}
				} finally {
					qResultA.close();
                    spinner.terminate();
                    spinner.join();

                    DecimalFormat formatter = new DecimalFormat("#,###");
                    LinkedHashMap<String, String> summary = new LinkedHashMap<>();

                    int cntLinkDivorce = cntLinkDivorceWeakMatch + cntLinkDivorceStrongMatch;
                    int cntLinkRemarriage = cntLinkRemarriageWeakMatch + cntLinkRemarriageStrongMatch;
                    int cntLinkTotal = cntLinkDivorce + cntLinkRemarriage;
                    summary.put("Candidate Links Discovered", formatter.format(cntLinkTotal+cntLinkFailed));
                    if (!ignoreDate) {
                        summary.put(" which passed validation", formatter.format(cntLinkTotal));
                    }
                    summary.put("  of which duplicates", formatter.format(cntLinkDuplicates));
                    summary.put("  of which remarriages", formatter.format(cntLinkRemarriage));
                    summary.put("    with full info on relatives", formatter.format(cntLinkRemarriageStrongMatch));
                    summary.put("    with partial info on relatives", formatter.format(cntLinkRemarriageWeakMatch));
                    summary.put("  of which divorces", formatter.format(cntLinkDivorce));
                    summary.put("    with full info on relatives ", formatter.format(cntLinkDivorceStrongMatch));
                    summary.put("    with partial info on relatives ", formatter.format(cntLinkDivorceWeakMatch));

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
        if (ageBefore >= 0 && ageAfter >= 0 && ageBefore <= ageAfter) {
            out = ageAfter - ageBefore;
        }

        return out;
    }
}
