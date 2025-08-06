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
import me.tongfei.progressbar.ProgressBarStyle;

public class Within_B_D {

	private String mainDirectoryPath, processName = "";;
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 0, MAX_YEAR_DIFF = 110,  linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, ignoreDate, ignoreBlock, singleInd;
	Index indexDeceased, indexMother, indexFather;

	public static final Logger lg = LogManager.getLogger(Within_B_D.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Within_B_D(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.singleInd = singleInd;
		this.myHDT = hdt;

		String options = LOG.getUserOptions(maxLevenshtein, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String resultsFileName = "within-B-D" + options;
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
		if(this.singleInd == false) {
			link_within_B_D("f", false); // false = do not close stream
			link_within_B_D("m", true); // true = close stream
		} else {
			link_within_B_D_single("f", false); 
			link_within_B_D_single("m", true); 
		}
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
				String taskName = "Linking Newborns to " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String birthEvent = ts.getSubject().toString();	
						String birthEventID = myHDT.getIDofEvent(birthEvent);
						Person newborn = myHDT.getPersonInfo(birthEvent, ROLE_NEWBORN);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								Person mother, father;
								mother = myHDT.getPersonInfo(birthEvent, ROLE_MOTHER);
								father = myHDT.getPersonInfo(birthEvent, ROLE_FATHER);
								CandidateList candidatesDeceased=null, candidatesMother=null, candidatesFather=null;
								if(mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesDeceased = indexDeceased.searchForCandidate(newborn, birthEventID, ignoreBlock);
									if(candidatesDeceased.candidates.isEmpty() == false) {
										if(mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, birthEventID, ignoreBlock);
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
														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
														}
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
											candidatesFather = indexFather.searchForCandidate(father, birthEventID, ignoreBlock);
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
														int yearDifference = 0;
														if(ignoreDate == false) {
															int birthYear = myHDT.getEventDate(birthEvent);
															yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
														}
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
													int yearDifference = 0;
													if(ignoreDate == false) {
														int birthYear = myHDT.getEventDate(birthEvent);
														yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
													}
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

	public void link_within_B_D_single(String gender, Boolean closeStream) {
		Dictionary dict = new Dictionary("within-B-D", mainDirectoryPath, maxLev, fixedLev);
		Boolean success = dict.generateDictionary(myHDT, ROLE_DECEASED, true, gender);
		if(success == true) {
			indexDeceased = dict.indexMain; indexDeceased.createTransducer();
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
				String taskName = "Linking Newborns to " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String birthEvent = ts.getSubject().toString();	
						String birthEventID = myHDT.getIDofEvent(birthEvent);
						Person newborn = myHDT.getPersonInfo(birthEvent, ROLE_NEWBORN);
						if(newborn.isValidWithFullName()) {
							if(newborn.getGender().equals(gender) || newborn.getGender().equals("u")) {
								CandidateList candidatesDeceased=null;
								candidatesDeceased = indexDeceased.searchForCandidate(newborn, birthEventID, ignoreBlock);
								if(candidatesDeceased.candidates.isEmpty() == false) {
									for(String finalCandidate: candidatesDeceased.candidates.keySet()) {
										String deathEventURI = myHDT.getEventURIfromID(finalCandidate);
										int yearDifference = 0;
										if(ignoreDate == false) {
											int birthYear = myHDT.getEventDate(birthEvent);
											yearDifference = checkTimeConsistency_Within_B_D(birthYear, deathEventURI);
										}
										if(yearDifference < 999) { // if it fits the time line
											Person deceased = myHDT.getPersonInfo(deathEventURI, ROLE_DECEASED);
											if(checkTimeConsistencyWithAge(yearDifference, deceased)) {
												LINKS.saveLinks_Within_B_M_single(candidatesDeceased, finalCandidate, deceased, familyCode, yearDifference);
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

