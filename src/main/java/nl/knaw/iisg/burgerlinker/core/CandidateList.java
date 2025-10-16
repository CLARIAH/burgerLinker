package nl.knaw.iisg.burgerlinker.core;


import java.util.HashMap;
import java.util.Set;

import com.github.liblevenshtein.transducer.Candidate;

import nl.knaw.iisg.burgerlinker.structs.Certificate;
import nl.knaw.iisg.burgerlinker.structs.Person;


public class CandidateList {
	public Person sourcePerson;
	public String sourceCertificateID;
	public HashMap<String, Certificate> candidates;

	public CandidateList(Person p, String sourceCertificateID) {
		this.sourcePerson = p;
		this.sourceCertificateID = sourceCertificateID;

		candidates = new HashMap<String, Certificate>();
	}

	public Boolean addCandidate(String candidateCertificateID, String candidateNumberNames, String candidateNumberIndividuals, String sourceFirstName, Candidate candidateFirstName,  Candidate candidateLastName) {
		if(candidates.containsKey(candidateCertificateID)) {
			Certificate cand = candidates.get(candidateCertificateID);
			cand.addMatchedFirstName(sourceFirstName, candidateFirstName);

			return true;
		} else {
			candidates.put(candidateCertificateID, new Certificate(candidateCertificateID, candidateNumberNames, candidateNumberIndividuals,
                                                                   sourceFirstName, candidateFirstName, candidateLastName,
                                                                   sourcePerson.names_separator));

            return false;
		}
	}

	public Set<String> findIntersectionCandidates(CandidateList otherList) {
		Set<String> thisCandidateCertificatesIDs = this.candidates.keySet();
		Set<String> otherCandidateCertificatesIDs = otherList.candidates.keySet();

		thisCandidateCertificatesIDs.retainAll(otherCandidateCertificatesIDs);

		return thisCandidateCertificatesIDs;
	}

	public Set<String> findIntersectionCandidates(CandidateList list1, CandidateList list2) {
		Set<String> thisCandidateCertificatesIDs = this.candidates.keySet();
		Set<String> list1CandidateCertificatesIDs = list1.candidates.keySet();
		Set<String> list2CandidateCertificatesIDs = list2.candidates.keySet();

		thisCandidateCertificatesIDs.retainAll(list1CandidateCertificatesIDs);
		thisCandidateCertificatesIDs.retainAll(list2CandidateCertificatesIDs);

		return thisCandidateCertificatesIDs;
	}
}
