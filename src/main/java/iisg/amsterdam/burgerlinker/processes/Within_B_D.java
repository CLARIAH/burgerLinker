package iisg.amsterdam.burgerlinker.processes;


import static iisg.amsterdam.burgerlinker.Properties.*;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import iisg.amsterdam.burgerlinker.CandidateList;
import iisg.amsterdam.burgerlinker.Dictionary;
import iisg.amsterdam.burgerlinker.Index;
import iisg.amsterdam.burgerlinker.LinksCSV;
import iisg.amsterdam.burgerlinker.MyHDT;
import iisg.amsterdam.burgerlinker.Person;
import iisg.amsterdam.burgerlinker.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;

public class Within_B_D {

	private String mainDirectoryPath, processName = "";;
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 0, MAX_YEAR_DIFF = 110,  linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev;
	Index indexDeceased, indexMother, indexFather;

	public static final Logger lg = LogManager.getLogger(Within_B_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Within_B_D(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean bestLink, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.myHDT = hdt;
		String fixed = "", best = "";
		if(fixedLev == true) {
			fixed = "-fixed";
		}
		if(bestLink == true) {
			best = "-best";
		}
		String resultsFileName = "within-B-D-maxLev-" + maxLevenshtein + fixed + best;
		if(formatCSV == true) {
			String header = "id_certificate_newborn,"
					+ "id_certificate_deceased,"
					+ "family_line,"
					+ "levenshtein_total_newborn,"
					+ "levenshtein_max_newborn,"
					+ "levenshtein_total_mother,"
					+ "levenshtein_max_mother,"
					+ "levenshtein_total_father,"
					+ "levenshtein_max_father,"
					+ "matched_names_newborn,"
					+ "number_names_newborn,"
					+ "number_names_deceased,"
					+ "matched_names_mother,"
					+ "number_names_newbornMother,"
					+ "number_names_partnerMother,"
					+ "matched_names_father,"
					+ "number_names_newbornFather,"
					+ "number_names_partnerFather,"
					+ "year_diff";
			LINKS = new LinksCSV(resultsFileName, mainDirectoryPath, header);
		}
		link_within_B_D("f", false); // false = do not close stream
		link_within_B_D("m", true); // true = close stream
	}


	public void link_within_B_D(String gender, Boolean closeStream) {
		Dictionary dict = new Dictionary("within-B-D", mainDirectoryPath, maxLev, fixedLev);
		Boolean success = dict.generateDictionary(myHDT, ROLE_DECEASED, ROLE_MOTHER, ROLE_FATHER, true, gender);
		if(success == true) {
			indexDeceased = dict.indexMain; indexDeceased.createTransducer();
			indexMother = dict.indexMother; indexMother.createTransducer();
			indexFather = dict.indexFather;	indexFather.createTransducer();
			try {
				int cntAll =0 ;
				String familyCode;
				if(gender == "f") {
					familyCode = "21";
					processName = "Deceased women";
				} else {
					familyCode = "22";
					processName = "Deceased men";
				}
				// iterate through the birth certificates to link it to the death dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_NEWBORN, "");
				long estNumber = it.estimatedNumResults();
				LOG.outputConsole("Estimated number of certificates to be linked is: " + estNumber);	
				String taskName = "Linking Newborns to " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String birthEvent = ts.getSubject().toString();	
						String birthEventID = myHDT.getIDofEvent(birthEvent);
						int birthYear = myHDT.getEventDate(birthEvent);
						Person newborn = myHDT.getPersonInfo(birthEvent, ROLE_NEWBORN);
						if(newborn.isValidWithFullName() && birthYear != 0) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								Person mother, father;
								mother = myHDT.getPersonInfo(birthEvent, ROLE_MOTHER);
								father = myHDT.getPersonInfo(birthEvent, ROLE_FATHER);
								CandidateList candidatesDeceased=null, candidatesMother=null, candidatesFather=null;
								if(mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesDeceased = indexDeceased.searchForCandidate(newborn, birthEventID);
									if(candidatesDeceased.candidates.isEmpty() == false) {
										if(mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, birthEventID);
											if(candidatesMother.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMother = candidatesDeceased.findIntersectionCandidates(candidatesMother);
												for(String finalCandidate: finalCandidatesMother) {
													Boolean link = true;
													if(father.isValidWithFullName()){ 
														if(candidatesDeceased.candidates.get(finalCandidate).individualsInCertificate.contains("F")) {
															link = false; // if both have fathers, but their names did not match
														}
													}
													if(link == true) { // if one of them does not have a father, we match on two individuals
														String deathEventURI = myHDT.getEventURIfromID(finalCandidate);
														int yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
														if(yearDifference < 999) { // if it fits the time line
															Person deceased = myHDT.getPersonInfo(deathEventURI, ROLE_DECEASED);
															Person deceased_mother = myHDT.getPersonInfo(deathEventURI, ROLE_MOTHER);
															if(checkTimeConsistencyWithAge(yearDifference, deceased)) {
																if(checkTimeConsistencyWithAge(yearDifference, deceased_mother)) {
																	LINKS.saveLinks_Within_B_M_mother(candidatesDeceased, candidatesMother, finalCandidate, deceased, deceased_mother, familyCode, yearDifference);	
																}
															}
														}
													}
												}
											}
										}
										if(father.isValidWithFullName()){
											candidatesFather = indexFather.searchForCandidate(father, birthEventID);
											if(candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesFather = candidatesDeceased.findIntersectionCandidates(candidatesFather);
												for(String finalCandidate: finalCandidatesFather) {
													Boolean link = true;
													if(mother.isValidWithFullName()){
														if(candidatesDeceased.candidates.get(finalCandidate).individualsInCertificate.contains("M")) {
															link = false; // if both have mothers, but their names did not match
														}
													}
													if(link == true) { // if one of them does not have a mother, we match on two individuals
														String deathEventURI = myHDT.getEventURIfromID(finalCandidate);
														int yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
														if(yearDifference < 999) { // if it fits the time line
															Person deceased = myHDT.getPersonInfo(deathEventURI, ROLE_DECEASED);		
															Person deceased_father = myHDT.getPersonInfo(deathEventURI, ROLE_FATHER);
															if(checkTimeConsistencyWithAge(yearDifference, deceased)) {
																if(checkTimeConsistencyWithAge(yearDifference, deceased_father)) {
																	LINKS.saveLinks_Within_B_M_father(candidatesDeceased, candidatesFather, finalCandidate, deceased, deceased_father, familyCode, yearDifference);	
																}
															}
														}
													}
												}
											}
										}
										if(mother.isValidWithFullName() && father.isValidWithFullName()) {
											if(candidatesMother.candidates.isEmpty() == false && candidatesFather.candidates.isEmpty() == false) {
												Set<String> finalCandidatesMotherFather = candidatesDeceased.findIntersectionCandidates(candidatesMother, candidatesFather);
												for(String finalCandidate: finalCandidatesMotherFather) {
													String deathEventURI = myHDT.getEventURIfromID(finalCandidate);
													int yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
													if(yearDifference < 999) { // if it fits the time line
														Person deceased = myHDT.getPersonInfo(deathEventURI, ROLE_DECEASED);
														Person deceased_mother = myHDT.getPersonInfo(deathEventURI, ROLE_MOTHER);
														Person deceased_father = myHDT.getPersonInfo(deathEventURI, ROLE_FATHER);
														if(checkTimeConsistencyWithAge(yearDifference, deceased)) {
															if(checkTimeConsistencyWithAge(yearDifference, deceased_mother)) {
																if(checkTimeConsistencyWithAge(yearDifference, deceased_father)) {
																	LINKS.saveLinks_Within_B_M(candidatesDeceased, candidatesMother, candidatesFather, finalCandidate, deceased, deceased_mother, deceased_father, familyCode, yearDifference);																				
																}
															}
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
				LOG.logError("link_between_B_M", "Error in linking parents of newborns to partners in process " + processName);
				e.printStackTrace();
			} finally {
				if(closeStream == true) {
					LINKS.closeStream();
				}
			}
		}
	}


	//	public Boolean generateDeceasedIndex(String gender) {
	//		long startTime = System.currentTimeMillis();
	//		int cntInserts=0, cntAll =0 , cnt_P_M_F=0, cnt_P_M =0 , cnt_P_F=0, cnt_P=0, cnt_no_P=0  ;
	//		IteratorTripleString it;
	//		if(gender == "f") {
	//			processName = "Deceased women";
	//			indexDeceased = new Index("deceased-women", mainDirectoryPath, maxLev, fixedLev);
	//			indexMother = new Index("deceased-women-mother", mainDirectoryPath, maxLev, fixedLev);
	//			indexFather = new Index("deceased-women-father", mainDirectoryPath, maxLev, fixedLev);
	//		} else {
	//			processName = "Deceased men";
	//			indexDeceased = new Index("deceased-men", mainDirectoryPath, maxLev, fixedLev);
	//			indexMother = new Index("deceased-men-mother", mainDirectoryPath, maxLev, fixedLev);
	//			indexFather = new Index("deceased-men-father", mainDirectoryPath, maxLev, fixedLev);
	//		}
	//		LOG.outputConsole("START: Generating Dictionary for " + processName);
	//		try {
	//			indexDeceased.openIndex();
	//			indexMother.openIndex();
	//			indexFather.openIndex();
	//			// iterate over all brides or grooms of marriage events
	//			it = myHDT.dataset.search("", ROLE_DECEASED, "");
	//			long estNumber = it.estimatedNumResults();
	//			LOG.outputConsole("Estimated number of certificates to be indexed is: " + estNumber);	
	//			String taskName = "Indexing " + processName;
	//			ProgressBar pb = null;
	//			try {
	//				pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
	//				while(it.hasNext()) {
	//					TripleString ts = it.next();
	//					cntAll++;
	//					String deathEvent = ts.getSubject().toString();
	//					String eventID = myHDT.getIDofEvent(deathEvent);
	//					Person deceased = myHDT.getPersonInfo(deathEvent, ROLE_DECEASED);	
	//					if(deceased.getGender().equals(gender) || deceased.getGender().equals("u")) {
	//						if(deceased.isValidWithFullName()) {
	//							Person partnerMother = myHDT.getPersonInfo(deathEvent, ROLE_MOTHER);
	//							Person partnerFather = myHDT.getPersonInfo(deathEvent, ROLE_FATHER);
	//							if(partnerMother.isValidWithFullName()){
	//								if(partnerFather.isValidWithFullName()){
	//									indexDeceased.addPersonToIndex(deceased, eventID, "M-F");
	//									indexMother.addPersonToIndex(partnerMother, eventID, "M-F");
	//									indexFather.addPersonToIndex(partnerFather, eventID, "M-F");
	//									cnt_P_M_F++;
	//									cntInserts++;
	//								} else {
	//									indexDeceased.addPersonToIndex(deceased, eventID, "M");
	//									indexMother.addPersonToIndex(partnerMother, eventID, "M");
	//									cnt_P_M++;
	//									cntInserts++;
	//								}
	//							} else {
	//								if(partnerFather.isValidWithFullName()){
	//									indexDeceased.addPersonToIndex(deceased, eventID, "F");
	//									indexFather.addPersonToIndex(partnerFather, eventID, "F");
	//									cnt_P_F++;
	//									cntInserts++;			
	//								} else {
	//									cnt_P++;
	//								}
	//							}
	//						}  else {
	//							cnt_no_P++;
	//						}
	//					} else {
	//						cnt_no_P++;
	//					}
	//					if(cntAll % 10000 == 0) {
	//						pb.stepBy(10000);
	//					}						
	//				} pb.stepTo(estNumber);
	//			} finally {
	//				pb.close();
	//			}
	//		} catch (NotFoundException e) {
	//			LOG.logError("generatePartnerIndex", "Error in iterating over partners of marriage events");
	//			LOG.logError("", e.toString());
	//			return false;
	//		} finally {
	//			indexDeceased.closeStream();
	//			indexMother.closeStream();
	//			indexFather.closeStream();
	//			LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);
	//			LOG.outputConsole("--> Total Certificates: " +  cntAll);
	//			LOG.outputConsole("--> Total Indexed Certificates: " +  cntInserts); 
	//			LOG.outputConsole("--> P - M - F: " +  cnt_P_M_F); 
	//			LOG.outputConsole("--> P - M : " +  cnt_P_M); 
	//			LOG.outputConsole("--> P - F: " +  cnt_P_F); 
	//			LOG.outputConsole("--> P: " +  cnt_P); 
	//			LOG.outputConsole("--> NO P: " +  cnt_no_P); 
	//			String nonIndexed = Integer.toString(cntAll - cntInserts);
	//			LOG.outputConsole("--> Total Non-Indexed Certificates (missing first/last name): " + nonIndexed);
	//		}
	//		return true;
	//	}

	/**
	 * Given the year of a birth event, check whether this marriage event fits the timeline of a possible match
	 * 
	 * @param birthYear
	 *            year of birth 
	 * @param candidateMarriageEvent
	 *            marriage event URI            
	 */
	public int checkTimeConsistency_Within_B_D(int birthYear, String candidateDeathEvent) {
		int deathYear = myHDT.getEventDate(candidateDeathEvent);
		int diff = deathYear - birthYear;
		if(diff >= MIN_YEAR_DIFF && diff < MAX_YEAR_DIFF) {
			return diff;
		} else {
			return 999;
		}
	}


	public Boolean checkTimeConsistencyWithAge(int registrationDifference, Person personURI) {
		int ageDeceased = myHDT.getAgeFromHDT(personURI.getURI());
		if(ageDeceased != 999) {
			if(Math.abs(registrationDifference - ageDeceased) < 2) {
				return true;
			} else {
				return false;
			}
		} else {
			return true;
		}
	}

}

