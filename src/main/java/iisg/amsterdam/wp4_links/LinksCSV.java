package iisg.amsterdam.wp4_links;

import static iisg.amsterdam.wp4_links.Properties.DIRECTORY_NAME_RESULTS;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.liblevenshtein.transducer.Candidate;

import iisg.amsterdam.wp4_links.utilities.FileUtilities;
import iisg.amsterdam.wp4_links.utilities.LoggingUtilities;

public class LinksCSV {


	private String linksID;
	private String fileLinks;
	private BufferedOutputStream streamLinks;
	private int counterFlush = 0;

	public static final Logger lg = LogManager.getLogger(LinksCSV.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();


	public LinksCSV(String ID, String directoryPath, String header) {
		this.linksID = ID;
		this.fileLinks = directoryPath + "/" + DIRECTORY_NAME_RESULTS + "/" + linksID + ".csv";
		openLinks(header);
	}

	public LinksCSV(String ID, String directoryPath) {
		this.linksID = ID;
		this.fileLinks = directoryPath + "/" + DIRECTORY_NAME_RESULTS + "/" + linksID + ".csv";
		openLinks();
	}

	public LinksCSV(String ID, String directoryPath, Boolean triples) {
		this.linksID = ID;
		this.fileLinks = directoryPath + "/" + DIRECTORY_NAME_RESULTS + "/" + linksID + ".nt";
		openLinks();
	}

	public void openLinks(String header) {
		try {
			streamLinks = FILE_UTILS.createFileStream(fileLinks);	
			addToStream(header);
		} catch (IOException e) {
			LOG.logError("openLinks", "Error when creating the following links file: " + fileLinks);
			e.printStackTrace();
		}
	}

	public void openLinks() {
		try {
			streamLinks = FILE_UTILS.createFileStream(fileLinks);	
		} catch (IOException e) {
			LOG.logError("openLinks", "Error when creating the following links file: " + fileLinks);
			e.printStackTrace();
		}
	}

	public Boolean flushLinks() {
		try {
			streamLinks.flush();
			return true;
		} catch (IOException e) {
			LOG.logError("flushLinks", "Error when flushing links file: " + linksID);
			e.printStackTrace();
			return false;
		}
	}

	public Boolean closeStream() {
		try {
			streamLinks.close();
			return true;
		} catch (IOException e) {
			LOG.logError("closeStream", "Error when closing links file: " + linksID);
			e.printStackTrace();
			return false;
		}
	}


	public void addToStream(String message) {
		FILE_UTILS.writeToOutputStream(streamLinks, message);
		counterFlush++;
		if(counterFlush >= 20) {
			counterFlush = 0;
			flushLinks();
		}
	}


	public Boolean checkMinimumMatchedNames(HashMap<String, Candidate> personPairedNames, CandidateList personList, String targetCertificateID) {
		int matchedNames = personPairedNames.size();
		int numberNamesPerson1 = personList.sourcePerson.getNumberOfFirstNames();
		int numberNamesPerson2 = personList.candidates.get(targetCertificateID).numberNames;
		if(numberNamesPerson1 > 1 && numberNamesPerson2 > 1 && matchedNames < 2) {
			return false;
		}
		return true;

	}



	public void saveLinks_Between_B_M(CandidateList motherList, CandidateList fatherList, String targetCertificateID, Person bride, Person groom, int yearDifference) {		

		HashMap<String, Candidate> motherPairedNames = motherList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> fatherPairedNames = fatherList.candidates.get(targetCertificateID).organiseMetadata();

		if(checkMinimumMatchedNames(motherPairedNames, motherList, targetCertificateID)) {
			if(checkMinimumMatchedNames(fatherPairedNames, fatherList, targetCertificateID)) {

				String link =  motherList.sourceCertificateID + "," + // source certificateID
						targetCertificateID + "," + // target CertificateID

				motherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total bride
				motherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein bride

				fatherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total groom
				fatherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein groom

				motherPairedNames.size() + "," + // number of matched names for the bride
				motherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the mother
				motherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the bride

				fatherPairedNames.size() + "," + // number of matched names for the groom
				fatherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the father
				fatherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the groom

				yearDifference ;
				addToStream(link);
			}
		}
	}



	public void saveLinks_Between_M_M(CandidateList motherList, CandidateList fatherList, String targetCertificateID, Person bride, Person groom, String familyLine, int yearDifference) {		

		HashMap<String, Candidate> motherPairedNames = motherList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> fatherPairedNames = fatherList.candidates.get(targetCertificateID).organiseMetadata();

		if(checkMinimumMatchedNames(motherPairedNames, motherList, targetCertificateID)) {
			if(checkMinimumMatchedNames(fatherPairedNames, fatherList, targetCertificateID)) {

				String link =  motherList.sourceCertificateID + "," + // source certificateID
						targetCertificateID + "," + // target CertificateID
						familyLine + "," + // family line (21:bride, 22:groom)

				motherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total bride
				motherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein bride

				fatherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total groom
				fatherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein groom

				motherPairedNames.size() + "," + // number of matched names for the bride
				motherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the mother
				motherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the bride

				fatherPairedNames.size() + "," + // number of matched names for the groom
				fatherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the father
				fatherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the groom

				yearDifference ;

				addToStream(link);
			}
		}
	}


	public void saveLinks_Within_B_M(CandidateList partnerList, CandidateList motherList, CandidateList fatherList, String targetCertificateID, Person partner, Person mother, Person father, String familyLine, int yearDifference) {		
		HashMap<String, Candidate> partnerPairedNames = partnerList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> motherPairedNames = motherList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> fatherPairedNames = fatherList.candidates.get(targetCertificateID).organiseMetadata();


		if(checkMinimumMatchedNames(partnerPairedNames, partnerList, targetCertificateID)) {
			if(checkMinimumMatchedNames(motherPairedNames, motherList, targetCertificateID)) {
				if(checkMinimumMatchedNames(fatherPairedNames, fatherList, targetCertificateID)) {

					String link =  partnerList.sourceCertificateID + "," + // source certificateID
							targetCertificateID + "," + // target CertificateID
							familyLine + "," + // family line (21:bride, 22:groom)

				partnerList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total newborn
				partnerList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein newborn

				motherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total mother
				motherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein mother

				fatherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total father
				fatherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein father

				partnerPairedNames.size() + "," + // number of matched names for the newborn
				partnerList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn
				partnerList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner

				motherPairedNames.size() + "," + // number of matched names for the newborn_mother
				motherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn_mother 
				motherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner_mother

				fatherPairedNames.size() + "," + // number of matched names for the newborn_father
				fatherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn_father 
				fatherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner_father

				yearDifference ;
					addToStream(link);
				}
			}
		}
	}

	public void saveLinks_Within_B_M_mother(CandidateList partnerList, CandidateList motherList, String targetCertificateID, Person partner, Person mother, String familyLine, int yearDifference) {		
		HashMap<String, Candidate> partnerPairedNames = partnerList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> motherPairedNames = motherList.candidates.get(targetCertificateID).organiseMetadata();


		if(checkMinimumMatchedNames(partnerPairedNames, partnerList, targetCertificateID)) {
			if(checkMinimumMatchedNames(motherPairedNames, motherList, targetCertificateID)) {

				String link =  partnerList.sourceCertificateID + "," + // source certificateID
						targetCertificateID + "," + // target CertificateID
						familyLine + "," + // family line (21:bride, 22:groom)

				partnerList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total newborn
				partnerList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein newborn

				motherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total mother
				motherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein mother

				"N.A,N.A," + 

				partnerPairedNames.size() + "," + // number of matched names for the newborn
				partnerList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn
				partnerList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner

				motherPairedNames.size() + "," + // number of matched names for the newborn_mother
				motherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn_mother 
				motherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner_mother

				"N.A,N.A,N.A," + // number of matched names for the newborn_father

				yearDifference ;
				addToStream(link);
			}
		}
	}

	public void saveLinks_Within_B_M_father(CandidateList partnerList, CandidateList fatherList, String targetCertificateID, Person partner, Person father, String familyLine, int yearDifference) {		
		HashMap<String, Candidate> partnerPairedNames = partnerList.candidates.get(targetCertificateID).organiseMetadata();
		HashMap<String, Candidate> fatherPairedNames = fatherList.candidates.get(targetCertificateID).organiseMetadata();

		if(checkMinimumMatchedNames(partnerPairedNames, partnerList, targetCertificateID)) {
			if(checkMinimumMatchedNames(fatherPairedNames, fatherList, targetCertificateID)) {

				String link =  partnerList.sourceCertificateID + "," + // source certificateID
						targetCertificateID + "," + // target CertificateID
						familyLine + "," + // family line (21:bride, 22:groom)

				partnerList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total newborn
				partnerList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein newborn


				"N.A,N.A," + 

				fatherList.candidates.get(targetCertificateID).levenshteinTotal + "," + // levenshtein total mother
				fatherList.candidates.get(targetCertificateID).maximumMatchedLevenshtein + "," + // max levenshtein mother


				partnerPairedNames.size() + "," + // number of matched names for the newborn
				partnerList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn
				partnerList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner


				"N.A,N.A,N.A," + // number of matched names for the newborn_father

				fatherPairedNames.size() + "," + // number of matched names for the newborn_mother
				fatherList.sourcePerson.getNumberOfFirstNames() + "," + // number of total names for the newborn_mother 
				fatherList.candidates.get(targetCertificateID).numberNames + "," + // number of total names for the partner_mother

				yearDifference ;
				addToStream(link);
			}
		}
	}


	public void saveIndividualLink(String idPerson1, String idPerson2, String linksMeta) {
		if(Integer.parseInt(idPerson1) < Integer.parseInt(idPerson2)) {
			addToStream(idPerson1 + "," + idPerson2 + "," + linksMeta);
		} else {
			addToStream(idPerson2 + "," + idPerson1 + "," + linksMeta);
		}
	}

	public String getLinksFilePath() {
		return this.fileLinks;
	}


}
