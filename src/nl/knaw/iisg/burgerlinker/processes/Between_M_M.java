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


// Between_M_M: link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates (reconstructs family ties)
public class Between_M_M {
	// output directory specified by the user + name of the called function
	private String mainDirectoryPath, processName = "";
	private MyHDT myHDT;
	private final int MIN_YEAR_DIFF = 14, MAX_YEAR_DIFF = 100, linkingUpdateInterval = 10000;
	private int maxLev;
	private Boolean fixedLev, ignoreDate, ignoreBlock;
	Index indexBride, indexGroom;

	public static final Logger lg = LogManager.getLogger(Between_M_M.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	LinksCSV LINKS;

	public Between_M_M(MyHDT hdt, String directoryPath, Integer maxLevenshtein, Boolean fixedLev, Boolean ignoreDate, Boolean ignoreBlock, Boolean singleInd, Boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.myHDT = hdt;

		String options = LOG.getUserOptions(maxLevenshtein, fixedLev, singleInd, ignoreDate, ignoreBlock);
		String resultsFileName = "between-M-M" + options;
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

						String marriageEvent = ts.getSubject().toString();
						String marriageEventID = myHDT.getIDofEvent(marriageEvent);

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
								CandidateList candidatesGroom = indexGroom.searchForCandidate(father, marriageEventID, ignoreBlock);

								if(candidatesGroom.candidates.isEmpty() == false) {
									CandidateList candidatesBride = indexBride.searchForCandidate(mother, marriageEventID, ignoreBlock);

									if(candidatesBride.candidates.isEmpty() == false) {
										Set<String> finalCandidatesList = candidatesBride.findIntersectionCandidates(candidatesGroom);

										for(String finalCandidate: finalCandidatesList) {
											String marriageEventAsCoupleURI = myHDT.getEventURIfromID(finalCandidate);

											int yearDifference = 0;
											if(ignoreDate == false) {
												int marriageEventYear = myHDT.getEventDate(marriageEvent);
												yearDifference = checkTimeConsistencyMarriageToMarriage(marriageEventYear, marriageEventAsCoupleURI);
											}
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
