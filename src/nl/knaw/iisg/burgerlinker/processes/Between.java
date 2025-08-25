package nl.knaw.iisg.burgerlinker.processes;


import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import nl.knaw.iisg.burgerlinker.CandidateList;
import nl.knaw.iisg.burgerlinker.Couple;
import nl.knaw.iisg.burgerlinker.Dictionary;
import nl.knaw.iisg.burgerlinker.Index;
import nl.knaw.iisg.burgerlinker.LinksCSV;
import nl.knaw.iisg.burgerlinker.MyHDT;
import nl.knaw.iisg.burgerlinker.Person;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


public class Between {
	private MyHDT myHDT;
    private Process process;
	private String mainDirectoryPath, processName;
	private final int linkingUpdateInterval = 10000;
	private int maxLev;
	private boolean fixedLev, ignoreDate, ignoreBlock;
	Index indexFemale, indexMale;

	public static final Logger lg = LogManager.getLogger(Between.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	LinksCSV LINKS;

	public Between(MyHDT hdt, Process process, String directoryPath, Integer maxLevenshtein,
                   boolean fixedLev, boolean ignoreDate, boolean ignoreBlock,
                   boolean singleInd, boolean formatCSV) {
		this.mainDirectoryPath = directoryPath;
		this.maxLev = maxLevenshtein;
		this.fixedLev = fixedLev;
		this.ignoreDate = ignoreDate;
		this.ignoreBlock = ignoreBlock;
		this.myHDT = hdt;
        this.process = process;
        this.processName = "between_" + this.process.toString();

        // setup output format
		String options = LOG.getUserOptions(this.maxLev, this.fixedLev, singleInd,
                                            this.ignoreDate, this.ignoreBlock);
		String resultsFileName = this.processName + options;
		if(formatCSV == true) {
			LINKS = new LinksCSV(resultsFileName, this.mainDirectoryPath, this.process.csvHeader);
		}

		link_between();
	}

	public void link_between() {
		Dictionary dict = new Dictionary(this.processName, this.mainDirectoryPath,
                                         this.maxLev, this.fixedLev);

        boolean genderUnknown = (process.type == Process.ProcessType.BIRTH_DECEASED);
        boolean success = dict.generateDictionary(this.myHDT, process.roleBSubject, process.roleBSubjectPartner,
                                                  genderUnknown);
		if(success == true) {
			indexMale = dict.indexMalePartner;
			indexFemale = dict.indexFemalePartner;
			indexMale.createTransducer();
			indexFemale.createTransducer();
			try {
				String taskName = "Linking " + processName;
				int cntAll = 0;

				// iterate through the birth certificates to link it to the marriage dictionaries
				IteratorTripleString it = this.myHDT.dataset.search("", process.roleASubject, "");
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

						String event = ts.getSubject().toString();
						String eventID = this.myHDT.getIDofEvent(event);

                        Set<Couple> couples = new HashSet<>();
                        if (process.type == Process.ProcessType.MARIAGE_MARIAGE) {
                            Couple otherCouple = new Couple(myHDT.getPersonInfo(event, process.roleBSubjectPartnerMother),
                                                            myHDT.getPersonInfo(event, process.roleBSubjectPartnerFather),
                                                            "22");
                            couples.put(otherCouple);
                        }

                        // default couple
                        Couple couple = new Couple(myHDT.getPersonInfo(event, process.roleASubjectMother),
                                                   myHDT.getPersonInfo(event, process.roleASubjectFather),
                                                   "21");
                        couples.put(couple);

						Person mother, father;
                        for (Couple c: couples) {
                            mother = c.husband;
                            father = c.wife;

                            if(mother.isValidWithFullName() && father.isValidWithFullName()) {
                                // start linking here
                                CandidateList candidatesMale = indexMale.searchForCandidate(father, eventID, this.ignoreBlock);
                                if(candidatesMale.candidates.isEmpty() == false) {
                                    CandidateList candidatesFemale = indexFemale.searchForCandidate(mother, eventID, this.ignoreBlock);

                                    if(candidatesFemale.candidates.isEmpty() == false) {
                                        Set<String> finalCandidatesList = candidatesFemale.findIntersectionCandidates(candidatesMale);

                                        for(String finalCandidate: finalCandidatesList) {
                                            String eventAsCoupleURI = this.myHDT.getEventURIfromID(finalCandidate);

                                            int yearDifference = 0;
                                            if(ignoreDate == false) {
                                                int eventYear = this.myHDT.getEventDate(event);
                                                yearDifference = checkTimeConsistency(eventYear, eventAsCoupleURI);
                                            }
                                            if(yearDifference < 999) { // if it fits the time line
                                                Person subjectB = this.myHDT.getPersonInfo(eventAsCoupleURI,
                                                                                           process.roleBSubject);
                                                Person subjectBPartner = this.myHDT.getPersonInfo(eventAsCoupleURI,
                                                                                                  process.roleBSubjectPartner);

                                                // determine order
                                                Person subjectBFemale, subjectBMale;
                                                if(subjectB.isFemale()) {
                                                    subjectBFemale = subjectB;
                                                    subjectBMale = subjectBPartner;
                                                } else {
                                                    subjectBFemale = subjectBPartner;
                                                    subjectBMale = subjectB;
                                                }

                                                if (process.type == Process.ProcessType.MARIAGE_MARIAGE) {
                                                    String familyCode = c.familyCode;
                                                    LINKS.saveLinks_Between_M_M(candidatesFemale, candidatesMale, finalCandidate,
                                                                                subjectBFemale, subjectBMale, familyCode,
                                                                                yearDifference);
                                                } else {
                                                    LINKS.saveLinks_Between_B_M(candidatesFemale, candidatesMale, finalCandidate,
                                                                                subjectBFemale, subjectBMale,
                                                                                yearDifference);
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
				LOG.logError("link_between", "Linking Error in Process " + processName);
				e.printStackTrace();
			} finally {
				LINKS.closeStream();
			}
		}
	}

	/**
	 * Check whether the time span between related events is plausable
	 */
	public int checkTimeConsistency(int eventYear, String referenceEventName) {
		int referenceYear = myHDT.getEventDate(referenceEventName);
		int diff = referenceYear - eventYear;
		if(diff >= this.process.minYearDiff && diff <= this.process.minYearDiff) {
			return diff;
		} else {
			return 999;
		}
	}
}
