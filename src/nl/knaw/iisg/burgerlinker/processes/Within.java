package nl.knaw.iisg.burgerlinker.processes;

import java.lang.Math;
import java.util.Set;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rule;

import nl.knaw.iisg.burgerlinker.core.CandidateList;
import nl.knaw.iisg.burgerlinker.core.Dictionary;
import nl.knaw.iisg.burgerlinker.core.Index;
import nl.knaw.iisg.burgerlinker.data.LinksCSV;
import nl.knaw.iisg.burgerlinker.data.MyHDT;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


public class Within {
	private String mainDirectoryPath, processName;
	private MyHDT myHDT;
    private Process process;
    private Map<String, Rule> rules;
	private final int linkingUpdateInterval = 10000;
	private int maxLev;
	private boolean fixedLev, ignoreDate, ignoreBlock, singleInd;
	Index indexSubjectB, indexMother, indexFather;

    public static final Logger lg = LogManager.getLogger(Within.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	LinksCSV LINKS;

	public Within(MyHDT hdt, Process process, Map<String, Rule> rules, String directoryPath,
                  Integer maxLevenshtein, boolean fixedLev, boolean ignoreDate,
                  boolean ignoreBlock, boolean singleInd, boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.myHDT = hdt;
        this.process = process;
        this.processName = this.process.toString();
        this.rules = rules;

		String options = LOG.getUserOptions(this.maxLev, this.fixedLev, singleInd,
                                            this.ignoreDate, this.ignoreBlock);
		String resultsFileName = this.processName + options;
		if(formatCSV == true) {
			LINKS = new LinksCSV(resultsFileName, this.mainDirectoryPath, this.process.csvHeader);
		}

		if(this.singleInd == false) {
			link_within("f", false); // false = do not close stream
			link_within("m", true); // true = close stream
		} else {
			link_within_single("f", false);
			link_within_single("m", true);
		}
	}

	public void link_within(String gender, boolean closeStream) {
        String familyCode;
        String roleBSubject = this.process.roleBSubject;
        String roleBSubjectMother = this.process.roleBSubjectMother;
        String roleBSubjectFather = this.process.roleBSubjectFather;
        boolean genderFilter = (this.process.type == Process.ProcessType.BIRTH_DECEASED);
        if(gender == "f") {
            familyCode = "21";
        } else if (gender == "m") {
            familyCode = "22";
            if (this.process.type == Process.ProcessType.BIRTH_MARIAGE) {
                roleBSubject = this.process.roleBSubjectPartner;
                roleBSubjectMother = this.process.roleBSubjectPartnerMother;
                roleBSubjectFather = this.process.roleBSubjectPartnerFather;
            }
        } else {
            LOG.logError("link_within", "Found gender '" + gender + "'. Excpect value in ['m', 'f']");

            return;
        }

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath, this.maxLev, this.fixedLev);
		boolean success = dict.generateDictionary(this.myHDT, roleBSubject, roleBSubjectMother, roleBSubjectFather,
                                                  genderFilter, gender);
		if(success == true) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			try {
				int cntAll = 0;
				// iterate through the birth certificates to link it to the death dictionaries
				IteratorTripleString it = myHDT.dataset.search("", this.process.roleASubject, "");
				long estNumber = it.estimatedNumResults();
				String taskName = "Linking " + this.process.roleASubject + " to " + roleBSubject
                                  + " (" + gender + ")";

                ProgressBar pb = new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(estNumber)
                    .setUpdateIntervalMillis(linkingUpdateInterval)
                    .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                    .build();
				try {
					while(it.hasNext()) {
						TripleString ts = it.next();
						cntAll++;

						String birthEvent = ts.getSubject().toString();
						String birthEventID = myHDT.getIDofEvent(birthEvent);

						Person newborn = myHDT.getPersonInfo(birthEvent, this.process.roleASubject);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								Person mother, father;
								mother = myHDT.getPersonInfo(birthEvent, this.process.roleASubjectMother);
								father = myHDT.getPersonInfo(birthEvent, this.process.roleASubjectFather);

								CandidateList candidatesSubjectB=null, candidatesMother=null, candidatesFather=null;
								if(mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, birthEventID, ignoreBlock);

									if(candidatesSubjectB.candidates.isEmpty() == false) {
										if(mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, birthEventID, ignoreBlock);

											if(candidatesMother.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMother = candidatesSubjectB.findIntersectionCandidates(candidatesMother);

												for(String finalCandidate: finalCandidatesMother) {
													boolean link = true;

													if(father.isValidWithFullName()){
														if(candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("F")) {
															link = false; // if both have fathers, but their names did not match
														}
													}

													if(link == true) { // if one of them does not have a father, we match on two individuals
														String subjectBEventURI = myHDT.getEventURIfromID(finalCandidate);

														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency(birthYear, subjectBEventURI);
														}
														if(yearDifference < 999) { // if it fits the time line
															Person subjectB = myHDT.getPersonInfo(subjectBEventURI, roleBSubject);
															Person subjectBMother = myHDT.getPersonInfo(subjectBEventURI,
                                                                                                        roleBSubjectMother);

															if(this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                                if (checkTimeConsistencyWithAge(yearDifference, subjectB) &&
															        checkTimeConsistencyWithAge(yearDifference, subjectBMother)) {
                                                                    LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, finalCandidate,
                                                                                                  subjectB, subjectBMother, familyCode, yearDifference);
                                                                    }
                                                            } else {
                                                                LINKS.saveLinks_Within_mother(candidatesSubjectB, candidatesMother, finalCandidate,
                                                                                              subjectB, subjectBMother, familyCode, yearDifference);
                                                            }
														}
													}
												}
											}
										}
										if(father.isValidWithFullName()){
											candidatesFather = indexFather.searchForCandidate(father, birthEventID, ignoreBlock);

											if(candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesFather = candidatesSubjectB.findIntersectionCandidates(candidatesFather);

												for(String finalCandidate: finalCandidatesFather) {
													boolean link = true;
													if(mother.isValidWithFullName()){
														if(candidatesSubjectB.candidates.get(finalCandidate).individualsInCertificate.contains("M")) {
															link = false; // if both have mothers, but their names did not match
														}
													}

													if(link == true) { // if one of them does not have a mother, we match on two individuals
														String subjectBEventURI = myHDT.getEventURIfromID(finalCandidate);

														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency(birthYear, subjectBEventURI);
														}
														if(yearDifference < 999) { // if it fits the time line
															Person subjectB = myHDT.getPersonInfo(subjectBEventURI, roleBSubject);
															Person subjectBFather = myHDT.getPersonInfo(subjectBEventURI, roleBSubjectFather);

															if(this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                                if (checkTimeConsistencyWithAge(yearDifference, subjectB) &&
																    checkTimeConsistencyWithAge(yearDifference, subjectBFather)) {
																	LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, finalCandidate,
                                                                                                  subjectB, subjectBFather, familyCode, yearDifference);
																}
                                                            } else {
                                                                LINKS.saveLinks_Within_father(candidatesSubjectB, candidatesFather, finalCandidate,
                                                                                              subjectB, subjectBFather, familyCode, yearDifference);
                                                            }
														}
													}
												}
											}
										}
										if(mother.isValidWithFullName() && father.isValidWithFullName()) {
											if(candidatesMother.candidates.isEmpty() == false && candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMotherFather = candidatesSubjectB.findIntersectionCandidates(candidatesMother,
                                                                                                                                        candidatesFather);

												for(String finalCandidate: finalCandidatesMotherFather) {
													String subjectBEventURI = myHDT.getEventURIfromID(finalCandidate);

													int yearDifference = 0;
													if(ignoreDate == false) {
														int birthYear = myHDT.getEventDate(birthEvent);
														yearDifference = checkTimeConsistency(birthYear, subjectBEventURI);
													}
													if(yearDifference < 999) { // if it fits the time line
														Person subjectB = myHDT.getPersonInfo(subjectBEventURI, roleBSubject);
														Person subjectBMother = myHDT.getPersonInfo(subjectBEventURI, roleBSubjectMother);
														Person subjectBFather = myHDT.getPersonInfo(subjectBEventURI, roleBSubjectFather);

                                                        if(this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                            if(checkTimeConsistencyWithAge(yearDifference, subjectB) &&
                                                               checkTimeConsistencyWithAge(yearDifference, subjectBMother) &&
                                                               checkTimeConsistencyWithAge(yearDifference, subjectBFather)) {
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
											}
										}
									}
								}
							}
						}
						if(cntAll % 10000 == 0) {
							pb.stepBy(10000);
						}
					} pb.stepTo(estNumber);
				} finally {
					pb.close();
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
        boolean genderFilter = (this.process.type == Process.ProcessType.BIRTH_DECEASED);
        String familyCode;
        String roleBSubject = this.process.roleBSubject;
        if (gender == "f") {
			familyCode = "21";
        } else if (gender == "m") {
			familyCode = "22";
            if (this.process.type == Process.ProcessType.BIRTH_MARIAGE) {
                roleBSubject = this.process.roleBSubjectPartner;
            }
        } else {
            LOG.logError("link_within_single", "Found gender '" + gender + "'. Excpect value in ['m', 'f']");

            return;
        }

		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath, this.maxLev, this.fixedLev);
		boolean success = dict.generateDictionary(myHDT, roleBSubject, genderFilter, gender);
		if(success == true) {
			indexSubjectB = dict.indexMain; indexSubjectB.createTransducer();
			try {
				String taskName = "Linking Single " + this.process.roleASubject + " to " + roleBSubject
                                  + " (" + gender + ")";
				int cntAll = 0;

				// iterate through the birth certificates to link it to the death dictionaries
				IteratorTripleString it = myHDT.dataset.search("", this.process.roleASubject, "");
				long estNumber = it.estimatedNumResults();

                ProgressBar pb = new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(estNumber)
                    .setUpdateIntervalMillis(linkingUpdateInterval)
                    .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                    .build();
				try {
					while(it.hasNext()) {
						TripleString ts = it.next();
						cntAll++;

						String birthEvent = ts.getSubject().toString();
						String birthEventID = myHDT.getIDofEvent(birthEvent);

						Person newborn = myHDT.getPersonInfo(birthEvent, this.process.roleASubject);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								CandidateList candidatesSubjectB=null;

								candidatesSubjectB = indexSubjectB.searchForCandidate(newborn, birthEventID, ignoreBlock);
								if(candidatesSubjectB.candidates.isEmpty() == false) {
									for(String finalCandidate: candidatesSubjectB.candidates.keySet()) {
										String subjectBEventURI = myHDT.getEventURIfromID(finalCandidate);

										int yearDifference = 0;
										if(ignoreDate == false) {
											int birthYear = myHDT.getEventDate(birthEvent);
											yearDifference = checkTimeConsistency(birthYear, subjectBEventURI);
										}
										if(yearDifference < 999) { // if it fits the time line
											Person subjectB = myHDT.getPersonInfo(subjectBEventURI, roleBSubject);

                                            if (this.process.type == Process.ProcessType.BIRTH_DECEASED) {
                                                if(checkTimeConsistencyWithAge(yearDifference, subjectB)) {
                                                    LINKS.saveLinks_Within_single(candidatesSubjectB, finalCandidate, subjectB,
                                                                                  familyCode, yearDifference);
                                                }
                                            } else {
                                                LINKS.saveLinks_Within_single(candidatesSubjectB, finalCandidate, subjectB,
                                                                              familyCode, yearDifference);
                                            }
										}
									}
								}
							}
						}
						if(cntAll % 10000 == 0) {
							pb.stepBy(10000);
						}
					} pb.stepTo(estNumber);
				} finally {
					pb.close();
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
	public int checkTimeConsistency(int eventYear, String referenceEventName) {
		int referenceYear = myHDT.getEventDate(referenceEventName);
		int diff = referenceYear - eventYear;

        Facts facts = new Facts();
        facts.put("diff", diff);

        Rule rule = this.rules.get("timegapdiff");
        if (rule == null || rule.evaluate(facts)) {
            return diff;
        }

        return 999;
    }

	public boolean checkTimeConsistencyWithAge(int registrationDifference, Person personURI) {
		int ageDeceased = myHDT.getAgeFromHDT(personURI.getURI());
		if(ageDeceased < 999) {
            int diff = Math.abs(registrationDifference - ageDeceased);

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
