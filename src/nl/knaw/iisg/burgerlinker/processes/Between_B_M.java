package nl.knaw.iisg.burgerlinker.processes;

import static nl.knaw.iisg.burgerlinker.Properties.ROLE_BRIDE;
import static nl.knaw.iisg.burgerlinker.Properties.ROLE_FATHER;
import static nl.knaw.iisg.burgerlinker.Properties.ROLE_GROOM;
import static nl.knaw.iisg.burgerlinker.Properties.ROLE_MOTHER;
import static nl.knaw.iisg.burgerlinker.Properties.ROLE_NEWBORN;

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

public class Between_B_M {

	private String mainDirectoryPath, processName = "";
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = -5, MAX_YEAR_DIFF = 36, linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, ignoreDate, ignoreBlock;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Between_B_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Between_B_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.myHDT = hdt;
		
		String options = LOG.getUserOptions(maxLevenshtein, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String resultsFileName = "between-B-M" + options;
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
		Dictionary dict = new Dictionary("between-B-M", mainDirectoryPath, maxLev, fixedLev);
		Boolean success = dict.generateDictionary(myHDT, ROLE_BRIDE, ROLE_GROOM, true);
		if(success == true) {	
			indexBride = dict.indexFemalePartner;
			indexGroom = dict.indexMalePartner;
			indexBride.createTransducer();
			indexGroom.createTransducer();
			try {
				int cntAll =0 ;
				// iterate through the birth certificates to link it to the marriage dictionaries
				IteratorTripleString it = myHDT.dataset.search("", ROLE_NEWBORN, "");
				long estNumber = it.estimatedNumResults();
				String taskName = "Linking " + processName;
				ProgressBar pb = null;
				try {
					pb = new ProgressBar(taskName, estNumber, linkingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1); 
					while(it.hasNext()) {	
						TripleString ts = it.next();	
						cntAll++;
						String birthEvent = ts.getSubject().toString();	
						String birthEventID = myHDT.getIDofEvent(birthEvent);	
						Person mother = myHDT.getPersonInfo(birthEvent, ROLE_MOTHER);
						Person father = myHDT.getPersonInfo(birthEvent, ROLE_FATHER);				
						if(mother.isValidWithFullName() && father.isValidWithFullName()) {
							// start linking here
							CandidateList candidatesGroom = indexGroom.searchForCandidate(father, birthEventID, ignoreBlock);
							if(candidatesGroom.candidates.isEmpty() == false) {
								CandidateList candidatesBride = indexBride.searchForCandidate(mother, birthEventID, ignoreBlock);
								if(candidatesBride.candidates.isEmpty() == false) {
									Set<String> finalCandidatesList = candidatesBride.findIntersectionCandidates(candidatesGroom);
									for(String finalCandidate: finalCandidatesList) {
										String marriageEventAsCoupleURI = myHDT.getEventURIfromID(finalCandidate);
										int yearDifference = 0;
										if(ignoreDate == false) {
											int birthYear = myHDT.getEventDate(birthEvent);
											yearDifference = checkTimeConsistency_between_b_m(birthYear, marriageEventAsCoupleURI);
										}
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
