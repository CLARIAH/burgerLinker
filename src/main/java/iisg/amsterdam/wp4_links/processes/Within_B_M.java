package iisg.amsterdam.wp4_links.processes;

import static iisg.amsterdam.wp4_links.Properties.*;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import iisg.amsterdam.wp4_links.CandidateList;
import iisg.amsterdam.wp4_links.Index;
import iisg.amsterdam.wp4_links.LinksCSV;
import iisg.amsterdam.wp4_links.MyHDT;
import iisg.amsterdam.wp4_links.Person;
import iisg.amsterdam.wp4_links.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;

public class Within_B_M {

	private String mainDirectoryPath, processName = "";;
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 13, MAX_YEAR_DIFF = 80, indexingUpdateInterval = 2000, linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, bestLink;
	Index indexPartner, indexMother, indexFather;

	public static final Logger lg = LogManager.getLogger(Within_B_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Within_B_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean bestLink, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.bestLink = bestLink;
		this.myHDT = hdt;
		String fixed = "", best = "";
		if(fixedLev == true) {
			fixed = "-fixed";
		}
		if(bestLink == true) {
			best = "-best";
		}
		String resultsFileName = "within-B-M-maxLev-" + maxLevenshtein + fixed + best;
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
		link_within_B_M("f", false); // false = do not close stream
		link_within_B_M("m", true); // true = close stream
	}


	public void link_within_B_M(String gender, Boolean closeStream) {
		Boolean success = generatePartnerIndex(gender);
		if(success == true) {	
			indexPartner.createTransducer();
			indexMother.createTransducer();
			indexFather.createTransducer();
			try {
				int cntAll =0 ;
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
				// iterate through the marriage certificates to link it to the marriage dictionaries
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
								CandidateList candidatesPartner=null, candidatesMother=null, candidatesFather=null;
								if(mother.isValidWithFullName() || father.isValidWithFullName()) {
									candidatesPartner = indexPartner.searchForCandidate(newborn, birthEventID);
									if(candidatesPartner.candidates.isEmpty() == false) {
										if(mother.isValidWithFullName()){
											candidatesMother = indexMother.searchForCandidate(mother, birthEventID);
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
														String marriageEventURI = myHDT.getEventURIfromID(finalCandidate, "direct");
														int yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
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
											candidatesFather = indexFather.searchForCandidate(father, birthEventID);
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
														String marriageEventURI = myHDT.getEventURIfromID(finalCandidate, "direct");
														int yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
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
													String marriageEventURI = myHDT.getEventURIfromID(finalCandidate, "direct");
													int yearDifference = checkTimeConsistency_Within_B_M(birthYear, marriageEventURI);
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


	public Boolean generatePartnerIndex(String gender) {
		long startTime = System.currentTimeMillis();
		int cntInserts=0, cntAll =0 , cnt_P_M_F=0, cnt_P_M =0 , cnt_P_F=0, cnt_P=0, cnt_no_P=0  ;
		IteratorTripleString it;
		String rolePartner, rolePartnerMother, rolePartnerFather; 
		if(gender == "f") {
			rolePartner = ROLE_BRIDE;
			rolePartnerMother = ROLE_BRIDE_MOTHER;
			rolePartnerFather = ROLE_BRIDE_FATHER;
			processName = "Brides";
			indexPartner = new Index("bride", mainDirectoryPath, maxLev, fixedLev);
			indexMother = new Index("bride-mother", mainDirectoryPath, maxLev, fixedLev);
			indexFather = new Index("bride-father", mainDirectoryPath, maxLev, fixedLev);
		} else {
			rolePartner = ROLE_GROOM;
			rolePartnerMother = ROLE_GROOM_MOTHER;
			rolePartnerFather = ROLE_GROOM_FATHER;
			processName = "Grooms";
			indexPartner = new Index("groom", mainDirectoryPath, maxLev, fixedLev);
			indexMother = new Index("groom-mother", mainDirectoryPath, maxLev, fixedLev);
			indexFather = new Index("groom-father", mainDirectoryPath, maxLev, fixedLev);
		}
		LOG.outputConsole("START: Generating Dictionary for " + processName);
		try {
			indexPartner.openIndex();
			indexMother.openIndex();
			indexFather.openIndex();
			// iterate over all brides or grooms of marriage events
			it = myHDT.dataset.search("", rolePartner, "");
			long estNumber = it.estimatedNumResults();
			LOG.outputConsole("Estimated number of certificates to be indexed is: " + estNumber);	
			String taskName = "Indexing " + processName;
			ProgressBar pb = null;
			try {
				pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
				while(it.hasNext()) {
					TripleString ts = it.next();
					cntAll++;
					String marriageEvent = ts.getSubject().toString();
					String eventID = myHDT.getIDofEvent(marriageEvent);
					Person partner = myHDT.getPersonInfo(marriageEvent, rolePartner);
					if(partner.isValidWithFullName()) {
						Person partnerMother = myHDT.getPersonInfo(marriageEvent, rolePartnerMother);
						Person partnerFather = myHDT.getPersonInfo(marriageEvent, rolePartnerFather);
						if(partnerMother.isValidWithFullName()){
							if(partnerFather.isValidWithFullName()){
								indexPartner.addPersonToIndex(partner, eventID, "M-F");
								indexMother.addPersonToIndex(partnerMother, eventID, "M-F");
								indexFather.addPersonToIndex(partnerFather, eventID, "M-F");
								cnt_P_M_F++;
								cntInserts++;
							} else {
								indexPartner.addPersonToIndex(partner, eventID, "M");
								indexMother.addPersonToIndex(partnerMother, eventID, "M");
								cnt_P_M++;
								cntInserts++;
							}
						} else {
							if(partnerFather.isValidWithFullName()){
								indexPartner.addPersonToIndex(partner, eventID, "F");
								indexFather.addPersonToIndex(partnerFather, eventID, "F");
								cnt_P_F++;
								cntInserts++;			
							} else {
								cnt_P++;
							}
						}
					} else {
						cnt_no_P++;
					}
					if(cntAll % 10000 == 0) {
						pb.stepBy(10000);
					}						
				} pb.stepTo(estNumber);
			} finally {
				pb.close();
			}
		} catch (NotFoundException e) {
			LOG.logError("generatePartnerIndex", "Error in iterating over partners of marriage events");
			LOG.logError("", e.toString());
			return false;
		} finally {
			indexPartner.closeStream();
			indexMother.closeStream();
			indexFather.closeStream();
			LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);
			LOG.outputConsole("--> Total Certificates: " +  cntAll);
			LOG.outputConsole("--> Total Indexed Certificates: " +  cntInserts); 
			LOG.outputConsole("--> P - M - F: " +  cnt_P_M_F); 
			LOG.outputConsole("--> P - M : " +  cnt_P_M); 
			LOG.outputConsole("--> P - F: " +  cnt_P_F); 
			LOG.outputConsole("--> P: " +  cnt_P); 
			LOG.outputConsole("--> NO P: " +  cnt_no_P); 
			String nonIndexed = Integer.toString(cntAll - cntInserts);
			LOG.outputConsole("--> Total Non-Indexed Certificates (missing first/last name): " + nonIndexed);
		}
		return true;
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

