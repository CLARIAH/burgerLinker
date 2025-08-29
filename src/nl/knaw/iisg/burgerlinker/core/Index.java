package nl.knaw.iisg.burgerlinker.core;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;

import com.github.liblevenshtein.transducer.Candidate;

import nl.knaw.iisg.burgerlinker.data.MyDB;
import nl.knaw.iisg.burgerlinker.core.MyTransducer;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.FileUtilities;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class Index {
	private String indexID, directoryDB, directoryDictionaryFirstNames, directoryDictionaryLastNames;
	private MyDB db;
	private BufferedOutputStream streamDictionaryFirstNames, streamDictionaryLastNames;
	private MyTransducer myTransducerFirstNames, myTransducerLastNames;
	private final String eventID_matchedNames_separator = ":";
	private final int flush_limit = 50;
	private int maxLev = 0, counterDictionaryFirstNames = 0, counterDictionaryLastNames = 0;
	public TreeMap<Integer, Integer> nameLengthLevenshtein;
	public HashSet<String> indexedFullNames;

	public static final Logger lg = LogManager.getLogger(Index.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	public Index(String ID, String directoryPath, int maxLev, Boolean fixedLev) {
		this.indexID = ID;
		this.maxLev = maxLev;
		this.directoryDB = directoryPath + "/" + DIRECTORY_NAME_DATABASE + "/" + indexID;
		this.directoryDictionaryFirstNames = directoryPath + "/" + DIRECTORY_NAME_DICTIONARY + "/" + indexID + "-FN.txt";
		this.directoryDictionaryLastNames = directoryPath + "/" + DIRECTORY_NAME_DICTIONARY + "/" + indexID + "-LN.txt";

		setNameLengthLevenshteinRules(fixedLev);

		indexedFullNames = new HashSet<String>();
	}

	public void setNameLengthLevenshteinRules(Boolean fixedLevenshtein) {
		// if fixedLev == true
		int[] maxLevenshtein = new int[]{maxLev, maxLev, maxLev, maxLev, maxLev};
		if(fixedLevenshtein == false)
			switch(maxLev) {
			case 0:
				maxLevenshtein = new int[]{0, 0, 0, 0, 0};
				break;
			case 1:
				maxLevenshtein = new int[]{0, 0, 1, 1, 1};
				break;
			case 2:
				maxLevenshtein = new int[]{0, 1, 1, 2, 2};
				break;
			case 3:
				maxLevenshtein = new int[]{0, 1, 2, 2, 3};
				break;
			case 4:
				maxLevenshtein = new int[]{0, 1, 2, 3, 4};
				break;
			}

		nameLengthLevenshtein = new TreeMap<Integer, Integer>();
		nameLengthLevenshtein.put(1, maxLevenshtein[0]);
		for(int i=2; i<=5; i++) {
			nameLengthLevenshtein.put(i, maxLevenshtein[1]);
		}
		for(int i=6; i<=8; i++) {
			nameLengthLevenshtein.put(i, maxLevenshtein[2]);
		}
		for(int i=9; i<=11; i++) {
			nameLengthLevenshtein.put(i, maxLevenshtein[3]);
		}

		nameLengthLevenshtein.put(12, maxLevenshtein[4]);
	}

	public int getAcceptedLevenshteinPerLength(String name, String sourceID) {
		int l = name.length();
		try {
			if (l < 13) {
				return nameLengthLevenshtein.get(l);
			} else {
				return nameLengthLevenshtein.get(12);
			}
		} catch (Exception e) {
			LOG.logError("getAcceptedLevenshteinPerLength", "Error when getting the max Lev distance for cert: " + sourceID + " of length " + l);
			e.printStackTrace();

			return 0;
		}
	}

	public void openIndex() {
		createDB();
		createDictionary();
	}

	public void createDB() {
		db = new MyDB(directoryDB);
		try {
			db.openMyDB(true);
		} catch (RocksDBException e) {
			LOG.logError("createDB", "Error when creating the following DB: " + indexID + " in the following directory " + directoryDB);
			e.printStackTrace();
		}
	}

	public void createDictionary() {
		try {
			streamDictionaryFirstNames = FILE_UTILS.createFileStream(directoryDictionaryFirstNames);
			streamDictionaryLastNames = FILE_UTILS.createFileStream(directoryDictionaryLastNames);
		} catch (IOException e) {
			LOG.logError("createDictionary", "Error when creating the following dictionary text file: " + directoryDictionaryFirstNames);
			e.printStackTrace();
		}
	}

	public void createTransducer() {
		myTransducerFirstNames = new MyTransducer(directoryDictionaryFirstNames, maxLev);
		myTransducerLastNames = new MyTransducer(directoryDictionaryLastNames, maxLev);
	}

	public void addSingleValueToMyDB(String key, String value) {
		db.addSingleValueToDB(key, value);
	}

	public void addListValueToMyDB(String key, String value) {
		db.addListValueToDB(key, value);
	}

	public String getSingleValueFromDB(String key) {
		return db.getSingleValueFromDB(key);
	}

	public ArrayList<String> getListFromDB(String key) {
		return db.getListFromDB(key);
	}

	public void addToMyDictionaryFirstNames(String message) {
		FILE_UTILS.writeToOutputStream(streamDictionaryFirstNames, message);
		counterDictionaryFirstNames++;

		if(counterDictionaryFirstNames == flush_limit) {
			flushDictionary(streamDictionaryFirstNames);
			counterDictionaryFirstNames = 0;
		}
	}

	public void addToMyDictionaryLastNames(String message) {
		FILE_UTILS.writeToOutputStream(streamDictionaryLastNames, message);
		counterDictionaryLastNames++;

		if(counterDictionaryLastNames == flush_limit) {
			flushDictionary(streamDictionaryLastNames);
			counterDictionaryLastNames = 0;
		}
	}

	public Boolean flushDictionary(BufferedOutputStream dict) {
		try {
			dict.flush();

			return true;
		} catch (IOException e) {
			LOG.logError("flushDictionary", "Error when flushing dictionary of index: " + indexID);
			e.printStackTrace();

			return false;
		}
	}

	public Boolean closeStream() {
		try {
			streamDictionaryFirstNames.close();
			streamDictionaryLastNames.close();

			return true;
		} catch (IOException e) {
			LOG.logError("closeDictionary", "Error when closing dictionary of index: " + indexID);
			e.printStackTrace();

			return false;
		}
	}

	public Boolean addPersonToIndex(Person person, String eventID) {
		try {
			String[] firstNames = person.decomposeFirstname();
			String lastName = person.getLastName();
			int numberOfFirstNames = firstNames.length;

			for(String firstName: firstNames) {
				addToMyDictionaryFirstNames(firstName);
				String value = eventID + eventID_matchedNames_separator + numberOfFirstNames;
				String fullName = firstName + person.names_separator + lastName;
				addListValueToMyDB(fullName, value);
				indexedFullNames.add(fullName);
			}
			addToMyDictionaryLastNames(lastName);

			return true;
		} catch (Exception e) {
			LOG.logError("addPersonToIndex", "Error adding person: " + person + " of eventID: " + eventID + " to index: " + indexID);
		}

		return false;
	}

	public Boolean addPersonToIndex(Person person, String eventID, String numberOfIndividuals) {
		try {
			String[] firstNames = person.decomposeFirstname();
			String lastName = person.getLastName();
			int numberOfFirstNames = firstNames.length;

			for(String firstName: firstNames) {
				addToMyDictionaryFirstNames(firstName);
				String value = eventID + eventID_matchedNames_separator + numberOfFirstNames + eventID_matchedNames_separator + numberOfIndividuals;
				String fullName = firstName + person.names_separator + lastName;
				addListValueToMyDB(fullName, value);
				indexedFullNames.add(fullName);
			}
			addToMyDictionaryLastNames(lastName);

			return true;
		} catch (Exception e) {
			LOG.logError("addPersonToIndex", "Error adding person: " + person + " of eventID: " + eventID + " to index: " + indexID);
		}

		return false;
	}

	public ArrayList<Candidate> searchFirstNameInTransducer(String firstName, int maxLev) {
		ArrayList<Candidate> result = new ArrayList<Candidate>();
		try {
			Iterable<Candidate> candidates = myTransducerFirstNames.transducer.transduce(firstName, maxLev);
			for(Candidate cand: candidates) {
				result.add(cand);
			}

			return result;
		} catch (Exception e) {
			LOG.logError("searchFirstNameInTransducer", "Error searching for first name of person: " + firstName + " in index: " + indexID + "-FN");
		}

		return result;
	}

	public ArrayList<Candidate> searchLastNameInTransducer(String lastName, int maxLev) {
		ArrayList<Candidate> result = new ArrayList<Candidate>();
		try {
			Iterable<Candidate> candidates = myTransducerLastNames.transducer.transduce(lastName, maxLev);
			for(Candidate cand: candidates) {
				result.add(cand);
			}

			return result;
		} catch (Exception e) {
			LOG.logError("searchLastNameInTransducer", "Error searching for last name of person: " + lastName + " in index: " + indexID + "-LN");
		}

		return result;
	}

	public CandidateList searchForCandidate(Person person, String sourceCertificateID, Boolean ignoreBlock) {
		CandidateList candidateList = new CandidateList(person, sourceCertificateID);
		String[] firstNames = person.decomposeFirstname();
		String lastName = person.getLastName();
		ArrayList<Candidate> initialCandidates_LastNames = searchLastNameInTransducer(lastName, getAcceptedLevenshteinPerLength(lastName, "LN-"+sourceCertificateID));
		ArrayList<Candidate> candidates_LastNames;

		if(ignoreBlock) {
			candidates_LastNames = initialCandidates_LastNames;
		} else {
			candidates_LastNames = blockFirstLetterLastName(initialCandidates_LastNames, lastName);
		}

		if (! candidates_LastNames.isEmpty()) {
			for(String firstName: firstNames) {
				if(! firstName.equals("")) {
					ArrayList<Candidate> initialCandidates_FirstNames = searchFirstNameInTransducer(firstName, getAcceptedLevenshteinPerLength(firstName, "FN-"+sourceCertificateID));
					if (! initialCandidates_FirstNames.isEmpty()) {
						for (Candidate firstNameCandidate: initialCandidates_FirstNames) {
							for (Candidate lastNameCandidate: candidates_LastNames) {
								String fullNameCandidate = firstNameCandidate.term() + person.names_separator + lastNameCandidate.term();
								if(indexedFullNames.contains(fullNameCandidate)) {
									ArrayList<String> candidateCertificatesIDList = getListFromDB(fullNameCandidate);
									if(candidateCertificatesIDList!= null) {
										for(String candCertificateID: candidateCertificatesIDList) {
											String[] certificate = candCertificateID.split(":");
											if(certificate.length == 3) {
												candidateList.addCandidate(certificate[0], certificate[1], certificate[2], firstName, firstNameCandidate, lastNameCandidate);
											} else {
												candidateList.addCandidate(certificate[0], certificate[1], "", firstName, firstNameCandidate, lastNameCandidate);
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
		return candidateList;
	}

	public ArrayList<Candidate> blockFirstLetterLastName(ArrayList<Candidate> initialCandidates_LastNames, String lastName) {
		ArrayList<Candidate> results = new ArrayList<>();

		char firstLetter = lastName.charAt(0);
		for(Candidate cand : initialCandidates_LastNames) {
			if(cand.term().charAt(0) == firstLetter) {
				results.add(cand);
			}
		}

		return results;
	}

	public HashMap<String, String> getIntersectedCandidateEvents(HashMap<String, Candidate> candidatesRole1, HashMap<String, Candidate> candidatesRole2) {
		HashMap<String,String> candidatesRole1Events = separateEventFromMeta(candidatesRole1.keySet());
		HashMap<String,String> candidatesRole2Events = separateEventFromMeta(candidatesRole2.keySet());

		Set<String> candidates1 = candidatesRole1Events.keySet();
		Set<String> candidates2 = candidatesRole2Events.keySet();
		candidates1.retainAll(candidates2);

		HashMap<String, String> result = new HashMap<String, String>();
		for (String cand : candidates1) {
			String matchedNamesRole1 = candidatesRole1Events.get(cand);
			String matchedNamesRole2 = candidatesRole2Events.get(cand);

			int distanceRole1 = candidatesRole1.get(cand + eventID_matchedNames_separator + matchedNamesRole1).distance();
			int distanceRole2 = candidatesRole2.get(cand + eventID_matchedNames_separator + matchedNamesRole2).distance();

            String metaNames = matchedNamesRole1 + "-" + matchedNamesRole2;
			String metaDistances = distanceRole1 + "-" + distanceRole2;

			result.put(cand, metaDistances + "," + metaNames);
		}
		return result;
	}

	public HashMap<String,String> separateEventFromMeta(Set<String> events){
		HashMap<String, String> result = new HashMap<String, String>();
		for(String eventWithMeta: events) {
			String[] event = eventWithMeta.split(eventID_matchedNames_separator);
			result.put(event[0], event[1]);
		}

		return result;
	}
}
