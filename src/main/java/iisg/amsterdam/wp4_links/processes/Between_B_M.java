package iisg.amsterdam.wp4_links.processes;

import static iisg.amsterdam.wp4_links.Properties.ROLE_BRIDE;
import static iisg.amsterdam.wp4_links.Properties.ROLE_FATHER;
import static iisg.amsterdam.wp4_links.Properties.ROLE_GROOM;
import static iisg.amsterdam.wp4_links.Properties.ROLE_MOTHER;
import static iisg.amsterdam.wp4_links.Properties.ROLE_NEWBORN;

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

public class Between_B_M {

	private String mainDirectoryPath, processName = "";;
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = -10, MAX_YEAR_DIFF = 36, indexingUpdateInterval = 2000, linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, bestLink;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Between_B_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Between_B_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean bestLink, Boolean formatCSV) {
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
		String resultsFileName = "between-B-M-maxLev-" + maxLevenshtein + fixed + best;
		if(formatCSV == true) {
			String header = "id_certificate_newbornParents,"
					+ "id_certificate_partners,"
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
		link_between_B_M();
	}
	
	
	public void link_between_B_M() {
		Boolean success = generateCouplesIndex();
		if(success == true) {	
			indexBride.createTransducer();
			indexGroom.createTransducer();
			try {
				int cntAll =0 ;
				// iterate through the marriage certificates to link it to the marriage dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_NEWBORN, "");
				long estNumber = it.estimatedNumResults();
				LOG.outputConsole("Estimated number of certificates to be linked is: " + estNumber);	
				String taskName = "Linking " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String birthEvent = ts.getSubject().toString();	
						String birthEventID = myHDT.getIDofEvent(birthEvent);
						int birthYear = myHDT.getEventDate(birthEvent);
						Person mother = myHDT.getPersonInfo(birthEvent, ROLE_MOTHER);
						Person father = myHDT.getPersonInfo(birthEvent, ROLE_FATHER);				
						if(mother.isValidWithFullName() && father.isValidWithFullName()) {
							// start linking here
							CandidateList candidatesGroom = indexGroom.searchForCandidate(father, birthEventID);
							if(candidatesGroom.candidates.isEmpty() == false) {
								CandidateList candidatesBride = indexBride.searchForCandidate(mother, birthEventID);
								if(candidatesBride.candidates.isEmpty() == false) {
									Set<String> finalCandidatesList = candidatesBride.findIntersectionCandidates(candidatesGroom);
									for(String finalCandidate: finalCandidatesList) {
										String marriageEventAsCoupleURI = myHDT.getEventURIfromID(finalCandidate, "direct");
										int yearDifference = checkTimeConsistency_between_b_m(birthYear, marriageEventAsCoupleURI);
										if(yearDifference < 999) { // if it fits the time line
											Person bride = myHDT.getPersonInfo(marriageEventAsCoupleURI, ROLE_BRIDE);
											Person groom = myHDT.getPersonInfo(marriageEventAsCoupleURI, ROLE_GROOM);
											LINKS.saveLinks_Between_B_M(candidatesBride, candidatesGroom, finalCandidate, bride, groom, yearDifference);																				
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
				LINKS.closeStream();
			}
		}
	}


	public Boolean generateCouplesIndex() {
		long startTime = System.currentTimeMillis();
		int cntInserts=0, cntAll =0 ;
		IteratorTripleString it;
		processName = "Couples";
		indexBride = new Index("bride", mainDirectoryPath, maxLev, fixedLev);
		indexGroom = new Index("groom", mainDirectoryPath, maxLev, fixedLev);
		LOG.outputConsole("START: Generating Dictionary for " + processName);
		try {
			indexBride.openIndex();
			indexGroom.openIndex();
			// iterate over all brides or grooms of marriage events
			it = myHDT.dataset.search("", ROLE_BRIDE, "");
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
					Person bride = myHDT.getPersonInfo(marriageEvent, ROLE_BRIDE);
					Person groom = myHDT.getPersonInfo(marriageEvent, ROLE_GROOM);
					if(bride.isValidWithFullName() && groom.isValidWithFullName()) {				
						indexBride.addPersonToIndex(bride, eventID);
						indexGroom.addPersonToIndex(groom, eventID);
						cntInserts++;
					}			
					if(cntAll % 10000 == 0) {
						pb.stepBy(10000);
					}						
				} pb.stepTo(estNumber);
			} finally {
				pb.close();
			}
		} catch (NotFoundException e) {
			LOG.logError("generateCoupleIndex", "Error in iterating over partners of marriage events");
			LOG.logError("", e.toString());
			return false;
		} finally {
			indexBride.closeStream();
			indexGroom.closeStream();
			LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);
			LOG.outputConsole("--> Total Certificates: " +  cntAll);
			LOG.outputConsole("--> Total Indexed Certificates: " +  cntInserts); 
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
	public int checkTimeConsistency_between_b_m(int birthYearAsParents, String marriageAsCouple) {
		int marriageAsCoupleYear = myHDT.getEventDate(marriageAsCouple);
		int diff = birthYearAsParents - marriageAsCoupleYear;
		if(diff >= MIN_YEAR_DIFF && diff <= MAX_YEAR_DIFF) {
			return diff;
		} else {
			return 999;
		}
	}

	
	
}
