package nl.knaw.iisg.burgerlinker.processes;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import nl.knaw.iisg.burgerlinker.CandidateList;
import nl.knaw.iisg.burgerlinker.Dictionary;
import nl.knaw.iisg.burgerlinker.Index;
import nl.knaw.iisg.burgerlinker.LinksCSV;
import nl.knaw.iisg.burgerlinker.MyHDT;
import nl.knaw.iisg.burgerlinker.Person;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


public class Within_B_M {
	private String mainDirectoryPath, processName = "";;
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 14, MAX_YEAR_DIFF = 80,  linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, ignoreDate, ignoreBlock, singleInd;
	Index indexPartner, indexMother, indexFather;

	public static final Logger lg = LogManager.getLogger(Within_B_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Within_B_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.myHDT = hdt;

		String options = LOG.getUserOptions(maxLevenshtein, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String resultsFileName = "within-B-M" + options;
		if(formatCSV == true) {
			String header = "id_certificate_newborn,"
					+ "id_certificate_partner,"
					+ "family_line,"
					+ "levenshtein_total_newborn,"
					+ "levenshtein_max_newborn,"
					+ "levenshtein_total_mother,"
					+ "levenshtein_max_mother,"
					+ "levenshtein_total_father,"
					+ "levenshtein_max_father,"
					+ "matched_names_newborn,"
					+ "number_names_newborn,"
					+ "number_names_partner,"
					+ "matched_names_mother,"
					+ "number_names_newbornMother,"
					+ "number_names_partnerMother,"
					+ "matched_names_father,"
					+ "number_names_newbornFather,"
					+ "number_names_partnerFather,"
					+ "year_diff";
			LINKS = new LinksCSV(resultsFileName, mainDirectoryPath, header);
		}

		if(this.singleInd == false) {
			link_within_B_M("f", false); // false = do not close stream
			link_within_B_M("m", true); // true = close stream
		} else {
			link_within_B_M_single("f", false);
			link_within_B_M_single("m", true);
		}

	}

	public void link_within_B_M(String gender, Boolean closeStream) {
		String rolePartner, rolePartnerMother, rolePartnerFather, familyCode;
		if(gender == "f") {
			familyCode = "21";
			rolePartner = ROLE_BRIDE;
			rolePartnerMother = ROLE_BRIDE_MOTHER;
			rolePartnerFather = ROLE_BRIDE_FATHER;
			processName = "Brides";
		} else {
			familyCode = "22";
			rolePartner = ROLE_GROOM;
			rolePartnerMother = ROLE_GROOM_MOTHER;
			rolePartnerFather = ROLE_GROOM_FATHER;
			processName = "Grooms";
		}

		Dictionary dict = new Dictionary("within-B-M", mainDirectoryPath, maxLev, fixedLev);
		Boolean success = dict.generateDictionary(myHDT, rolePartner, rolePartnerMother, rolePartnerFather, false, "");
		if(success == true) {
			indexPartner = dict.indexMain; indexPartner.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			try {
				int cntAll =0 ;
				// iterate through the marriage certificates to link it to the marriage dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_NEWBORN, "");
				long estNumber = it.estimatedNumResults();
				String taskName = "Linking Newborns to " + processName;

                ProgressBar pb = new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(estNumber)
                    .setUpdateIntervalMillis(linkingUpdateInterval)
                    .setStyle(ProgressBarStyle.UNICODE_BLOCK)
                    .build();
				try {
					while(it.hasNext()) {
						TripleString ts = it.next();

                        String birthEvent = ts.getSubject().toString();
						String birthEventID = myHDT.getIDofEvent(birthEvent);

						cntAll++;

                        Person newborn = myHDT.getPersonInfo(birthEvent, ROLE_NEWBORN);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								Person mother, father;
								mother = myHDT.getPersonInfo(birthEvent, ROLE_MOTHER);
								father = myHDT.getPersonInfo(birthEvent, ROLE_FATHER);
								CandidateList candidatesPartner=null, candidatesMother=null, candidatesFather=null;
								if(mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesPartner = indexPartner.searchForCandidate(newborn, birthEventID, ignoreBlock);

									if(candidatesPartner.candidates.isEmpty() == false) {
										if(mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, birthEventID, ignoreBlock);

											if(candidatesMother.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMother = candidatesPartner.findIntersectionCandidates(candidatesMother);

												for(String finalCandidate: finalCandidatesMother) {
													Boolean link = true;

													if(father.isValidWithFullName()){
														if(candidatesPartner.candidates.get(finalCandidate).individualsInCertificate.contains("F")) {
															link = false; // if both have fathers, but their names did not match
														}
													}

													if(link == true) { // if one of them does not have a father, we match on two individuals
														String marriageEventURI = myHDT.getEventURIfromID(finalCandidate);

														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
														}
														if(yearDifference < 999) { // if it fits the time line
															Person partner = myHDT.getPersonInfo(marriageEventURI, rolePartner);
															Person partner_mother = myHDT.getPersonInfo(marriageEventURI, rolePartnerMother);

															LINKS.saveLinks_Within_B_M_mother(candidatesPartner, candidatesMother, finalCandidate, partner, partner_mother, familyCode, yearDifference);
														}
													}
												}
											}
										}
										if(father.isValidWithFullName()){
											candidatesFather = indexFather.searchForCandidate(father, birthEventID, ignoreBlock);

											if(candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesFather = candidatesPartner.findIntersectionCandidates(candidatesFather);

												for(String finalCandidate: finalCandidatesFather) {
													Boolean link = true;
													if(mother.isValidWithFullName()){
														if(candidatesPartner.candidates.get(finalCandidate).individualsInCertificate.contains("M")) {
															link = false; // if both have mothers, but their names did not match
														}
													}

													if(link == true) { // if one of them does not have a mother, we match on two individuals
														String marriageEventURI = myHDT.getEventURIfromID(finalCandidate);

														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
														}

														if(yearDifference < 999) { // if it fits the time line
															Person partner = myHDT.getPersonInfo(marriageEventURI, rolePartner);
															Person partner_father = myHDT.getPersonInfo(marriageEventURI, rolePartnerFather);

															LINKS.saveLinks_Within_B_M_father(candidatesPartner, candidatesFather, finalCandidate, partner, partner_father, familyCode, yearDifference);
														}
													}
												}
											}
										}
										if(mother.isValidWithFullName() && father.isValidWithFullName()) {
											if(candidatesMother.candidates.isEmpty() == false && candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMotherFather = candidatesPartner.findIntersectionCandidates(candidatesMother, candidatesFather);

												for(String finalCandidate: finalCandidatesMotherFather) {
													String marriageEventURI = myHDT.getEventURIfromID(finalCandidate);

													int yearDifference = 0;
													if(ignoreDate == false) {
														int birthYear = myHDT.getEventDate(birthEvent);
														yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
													}
													if(yearDifference < 999) { // if it fits the time line
														Person partner = myHDT.getPersonInfo(marriageEventURI, rolePartner);
														Person partner_mother = myHDT.getPersonInfo(marriageEventURI, rolePartnerMother);
														Person partner_father = myHDT.getPersonInfo(marriageEventURI, rolePartnerFather);

														LINKS.saveLinks_Within_B_M(candidatesPartner, candidatesMother, candidatesFather, finalCandidate, partner, partner_mother, partner_father, familyCode, yearDifference);
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
				LOG.logError("link_between_B_M", "Error in linking parents of newborns to partners in process " + processName);
				e.printStackTrace();
			} finally {
				if(closeStream == true) {
					LINKS.closeStream();
				}
			}
		}
	}

	public void link_within_B_M_single(String gender, Boolean closeStream) {
		String rolePartner, familyCode;
		if(gender == "f") {
			familyCode = "21";
			rolePartner = ROLE_BRIDE;
			processName = "Brides";
		} else {
			familyCode = "22";
			rolePartner = ROLE_GROOM;
			processName = "Grooms";
		}

		Dictionary dict = new Dictionary("within-B-M", mainDirectoryPath, maxLev, fixedLev);

		Boolean success = dict.generateDictionary(myHDT, rolePartner, false, "");
		if(success == true) {
			indexPartner = dict.indexMain; indexPartner.createTransducer();
			try {
				int cntAll =0 ;
				// iterate through the marriage certificates to link it to the marriage dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_NEWBORN, "");
				long estNumber = it.estimatedNumResults();
				String taskName = "Linking Newborns to " + processName;

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

						Person newborn = myHDT.getPersonInfo(birthEvent, ROLE_NEWBORN);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								CandidateList candidatesPartner=null;
								candidatesPartner = indexPartner.searchForCandidate(newborn, birthEventID, ignoreBlock);
								if(candidatesPartner.candidates.isEmpty() == false) {
									for(String finalCandidate: candidatesPartner.candidates.keySet()) {
										String marriageEventURI = myHDT.getEventURIfromID(finalCandidate);

										int yearDifference = 0;
										if(ignoreDate == false) {
											int birthYear = myHDT.getEventDate(birthEvent);
											yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
										}
										if(yearDifference < 999) { // if it fits the time line
											Person partner = myHDT.getPersonInfo(marriageEventURI, rolePartner);
											LINKS.saveLinks_Within_B_M_single(candidatesPartner, finalCandidate, partner, familyCode, yearDifference);
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
                LOG.logError("link_between_B_M", "Error in linking parents of newborns to partners in process " + processName);
                e.printStackTrace();
            } finally {
                if(closeStream == true) {
                    LINKS.closeStream();
                }
            }
        }
    }

    /**
     * Given the year of a birth event, check whether this marriage event fits the timeline of a possible match
     *
     * @param birthYear
     *            year of birth
     * @param candidateMarriageEvent
     *            marriage event URI
     */
    public int checkTimeConsistency_Within_B_M(int birthYear, String candidateMarriageEvent) {
        int marriageYear = myHDT.getEventDate(candidateMarriageEvent);
        int diff = marriageYear - birthYear;
        if(diff >= MIN_YEAR_DIFF && diff < MAX_YEAR_DIFF) {
            return diff;
        } else {
            return 999;
        }
    }
}
