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

// Between_M_M: link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)

public class Between_M_M {

	// output directory specified by the user + name of the called function
	private String mainDirectoryPath, processName = "";
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 14, MAX_YEAR_DIFF = 100, linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Between_M_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Between_M_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean bestLink, Boolean formatCSV) {
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
		String resultsFileName = "between-M-M-maxLev-" + maxLevenshtein + fixed + best;
		if(formatCSV == true) {
			String header = "id_certificate_parents,"
					+ "id_certificate_partners,"
					+ "family_line,"
					+ "levenshtein_total_bride,"
					+ "levenshtein_max_bride,"
					+ "levenshtein_total_groom,"
					+ "levenshtein_max_groom,"
					+ "matched_names_bride,"
					+ "number_names_mother,"
					+ "number_names_bride,"
					+ "matched_names_groom,"
					+ "number_names_father,"
					+ "number_names_groom,"
					+ "year_diff";
			LINKS = new LinksCSV(resultsFileName, mainDirectoryPath, header);
		}
		link_between_M_M();
	}

	public void link_between_M_M() {
		Dictionary dict = new Dictionary("between-M-M", mainDirectoryPath, maxLev, fixedLev);
		Boolean success = dict.generateDictionary(myHDT, ROLE_BRIDE, ROLE_GROOM, true);
		if(success == true) {	
			indexBride = dict.indexFemalePartner;
			indexGroom = dict.indexMalePartner;
			indexBride.createTransducer();
			indexGroom.createTransducer();
			try {
				int cntAll =0 ;
				String familyCode = "";
				// iterate through the marriage certificates to link it to the marriage dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_BRIDE, "");
				long estNumber = it.estimatedNumResults();	
				String taskName = "Linking " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String marriageEvent = ts.getSubject().toString();	
						String marriageEventID = myHDT.getIDofEvent(marriageEvent);
						int marriageEventYear = myHDT.getEventDate(marriageEvent);
						Person mother, father;			
						for(int i=0; i<2; i++) {
							mother = null; father = null;   
							if(i==0) {
								// bride's parents
								mother = myHDT.getPersonInfo(marriageEvent, ROLE_BRIDE_MOTHER);
								father = myHDT.getPersonInfo(marriageEvent, ROLE_BRIDE_FATHER);
								familyCode = "21";
							} else {
								// groom's parents
								mother = myHDT.getPersonInfo(marriageEvent, ROLE_GROOM_MOTHER);
								father = myHDT.getPersonInfo(marriageEvent, ROLE_GROOM_FATHER);;
								familyCode = "22";
							}
							if(mother.isValidWithFullName() && father.isValidWithFullName()) {
								// start linking here
								CandidateList candidatesGroom = indexGroom.searchForCandidate(father, marriageEventID);
								if(candidatesGroom.candidates.isEmpty() == false) {
									CandidateList candidatesBride = indexBride.searchForCandidate(mother, marriageEventID);
									if(candidatesBride.candidates.isEmpty() == false) {
										Set<String> finalCandidatesList = candidatesBride.findIntersectionCandidates(candidatesGroom);
										for(String finalCandidate: finalCandidatesList) {
											String marriageEventAsCoupleURI = myHDT.getEventURIfromID(finalCandidate);
											int yearDifference = checkTimeConsistencyMarriageToMarriage(marriageEventYear, marriageEventAsCoupleURI);
											if(yearDifference < 999) { // if it fits the time line
												Person bride = myHDT.getPersonInfo(marriageEventAsCoupleURI, ROLE_BRIDE);
												Person groom = myHDT.getPersonInfo(marriageEventAsCoupleURI, ROLE_GROOM);	
												
												LINKS.saveLinks_Between_M_M(candidatesBride, candidatesGroom, finalCandidate, bride, groom, familyCode, yearDifference);																				
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
				LOG.logError("link_between_M_M", "Error in linking partners to partners in process " + processName);
				e.printStackTrace();
			} finally {
				LINKS.closeStream();
			}
		}
	}


//	public Boolean generateCouplesIndex() {
//		long startTime = System.currentTimeMillis();
//		int cntInserts=0, cntAll =0 ;
//		IteratorTripleString it;
//		processName = "Couples";
//		indexBride = new Index("bride", mainDirectoryPath, maxLev, fixedLev);
//		indexGroom = new Index("groom", mainDirectoryPath, maxLev, fixedLev);
//		LOG.outputConsole("START: Generating Dictionary for " + processName);
//		try {
//			indexBride.openIndex();
//			indexGroom.openIndex();
//			// iterate over all brides or grooms of marriage events
//			it = myHDT.dataset.search("", ROLE_BRIDE, "");
//			long estNumber = it.estimatedNumResults();
//			LOG.outputConsole("Estimated number of certificates to be indexed is: " + estNumber);	
//			String taskName = "Indexing " + processName;
//			ProgressBar pb = null;
//			try {
//				pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
//				while(it.hasNext()) {
//					TripleString ts = it.next();
//					cntAll++;
//					String marriageEvent = ts.getSubject().toString();
//					String eventID = myHDT.getIDofEvent(marriageEvent);
//					Person bride = myHDT.getPersonInfo(marriageEvent, ROLE_BRIDE);
//					Person groom = myHDT.getPersonInfo(marriageEvent, ROLE_GROOM);
//					if(bride.isValidWithFullName() && groom.isValidWithFullName()) {				
//						indexBride.addPersonToIndex(bride, eventID);
//						indexGroom.addPersonToIndex(groom, eventID);
//						cntInserts++;
//					}			
//					if(cntAll % 10000 == 0) {
//						pb.stepBy(10000);
//					}						
//				} pb.stepTo(estNumber);
//			} finally {
//				pb.close();
//			}
//		} catch (NotFoundException e) {
//			LOG.logError("generateCoupleIndex", "Error in iterating over partners of marriage events");
//			LOG.logError("", e.toString());
//			return false;
//		} finally {
//			indexBride.closeStream();
//			indexGroom.closeStream();
//			LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);
//			LOG.outputConsole("--> Total Certificates: " +  cntAll);
//			LOG.outputConsole("--> Total Indexed Certificates: " +  cntInserts); 
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
	public int checkTimeConsistencyMarriageToMarriage(int marriageAsParentsYear, String marriageAsCouple) {
		int marriageAsCoupleYear = myHDT.getEventDate(marriageAsCouple);
		int diff = marriageAsParentsYear - marriageAsCoupleYear;
		if(diff >= MIN_YEAR_DIFF && diff < MAX_YEAR_DIFF) {
			return diff;
		} else {
			return 999;
		}
	}


}
