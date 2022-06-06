package iisg.amsterdam.burgerlinker;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import iisg.amsterdam.burgerlinker.utilities.LoggingUtilities;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;

public class Dictionary {
	public String processName;
	public String mainDirectoryPath;
	public int maxLev;
	public Boolean fixedLev;
	public Index indexFemalePartner, indexMalePartner, indexMain, indexMother, indexFather;
	private final int indexingUpdateInterval = 2000;



	public static final Logger lg = LogManager.getLogger(Dictionary.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);



	public Dictionary(String processName, String mainDirectoryPath, int maxLev, Boolean fixedLev) {
		this.processName = processName;
		this.mainDirectoryPath = mainDirectoryPath;
		this.maxLev = maxLev;
		this.fixedLev = fixedLev;

	}


	public Boolean generateDictionary(MyHDT myHDT, String roleMain, Boolean genderFilter, String gender) { 
		long startTime = System.currentTimeMillis();
		int countInserts=0, countAll =0, count_No_Main=0;
		IteratorTripleString it;
		indexMain = new Index(getRoleFragment(roleMain)+gender, mainDirectoryPath, maxLev, fixedLev);
		LOG.outputConsole("START: Generating Dictionary for process: " + processName);
		try {
			indexMain.openIndex();
			it = myHDT.dataset.search("", roleMain, "");
			long estNumber = it.estimatedNumResults();	
			String taskName = "Indexing " + processName;
			ProgressBar pb = null;
			try {
				pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
				while(it.hasNext()) {
					TripleString ts = it.next();
					countAll++;
					String event = ts.getSubject().toString();
					Person personMain = myHDT.getPersonInfo(event, roleMain);
					if( (genderFilter == false) || (genderFilter == true && personMain.hasGender(gender)) ) {
						if(personMain.isValidWithFullName()){
							String eventID = myHDT.getIDofEvent(event);
							indexMain.addPersonToIndex(personMain, eventID, "");
							countInserts++;
						}
					} else {
						count_No_Main++;
					}
				if(countAll % 10000 == 0) {
					pb.stepBy(10000);
				}						
			} pb.stepTo(estNumber);
		} finally {
			pb.close();
		}
	} catch (NotFoundException e) {
		LOG.logError("generateDictionary", "Error in iterating over HDT file in process "+ processName);
		LOG.logError("", e.toString());
		return false;
	} finally {
		indexMain.closeStream();
		LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);		
		int countNonIndexed = countAll - countInserts;	
		LOG.outputConsole("");
		LOG.outputConsole("--------");
		LOG.outputConsole("- Number of Certificates: " +  countAll);
		LOG.outputConsole("- Number of Indexed Certificates: " +  countInserts); 
		LOG.outputConsole("- Number of Non-Indexed Certificates: " +  countNonIndexed);
		if(count_No_Main > 0) {
			LOG.outputConsole("-> Includes no Main Individual: " +  count_No_Main);  
		}	
		LOG.outputConsole("--------");
		LOG.outputConsole("");						
	}
	return true;
}





public Boolean generateDictionary(MyHDT myHDT, String roleFemalePartner, String roleMalePartner, Boolean knownGender) { 
	long startTime = System.currentTimeMillis();
	int countInserts=0, countAll =0 ;
	IteratorTripleString it;
	indexFemalePartner = new Index(getRoleFragment(roleFemalePartner), mainDirectoryPath, maxLev, fixedLev);
	indexMalePartner = new Index(getRoleFragment(roleMalePartner), mainDirectoryPath, maxLev, fixedLev);
	LOG.outputConsole("START: Generating Dictionary for process: " + processName);
	try {
		indexFemalePartner.openIndex();
		indexMalePartner.openIndex();
		it = myHDT.dataset.search("", roleFemalePartner, "");
		long estNumber = it.estimatedNumResults();
		String taskName = "Indexing " + processName;
		ProgressBar pb = null;
		try {
			pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
			while(it.hasNext()) {
				TripleString ts = it.next();
				countAll++;
				String event = ts.getSubject().toString();	
				Person femalePartner = myHDT.getPersonInfo(event, roleFemalePartner);
				if(femalePartner.isValidWithFullName()){
					String eventID = myHDT.getIDofEvent(event);
					Person malePartner = myHDT.getPersonInfo(event, roleMalePartner);
					if(malePartner.isValidWithFullName()) {	
						Boolean insert = addToIndex(femalePartner, malePartner, eventID, knownGender);
						if(insert) {
							countInserts++;
						}
					}	
				}
				if(countAll % 10000 == 0) {
					pb.stepBy(10000);
				}						
			} pb.stepTo(estNumber);
		} finally {
			pb.close();
		}
	} catch (NotFoundException e) {
		LOG.logError("generateDictionary", "Error in iterating over HDT file in process "+ processName);
		LOG.logError("", e.toString());
		return false;
	} finally {
		indexFemalePartner.closeStream();
		indexMalePartner.closeStream();
		LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);
		LOG.outputConsole("- Total Certificates: " +  countAll);
		LOG.outputConsole("- Total Indexed Certificates: " +  countInserts); 
		String nonIndexed = Integer.toString(countAll - countInserts);
		LOG.outputConsole("- Total Non-Indexed Certificates (missing first/last name): " + nonIndexed);
	}
	return true;
}



public Boolean generateDictionary(MyHDT myHDT, String roleMain, String roleMother, String roleFather, Boolean genderFilter, String gender) { 
	long startTime = System.currentTimeMillis();
	int countInserts=0, countAll =0, count_Main_Mother_Father=0, count_Main_Mother=0, count_Main_Father=0, count_Main=0, count_No_Main=0;
	IteratorTripleString it;
	indexMain = new Index(getRoleFragment(roleMain)+gender, mainDirectoryPath, maxLev, fixedLev);
	indexMother = new Index(getRoleFragment(roleMother)+gender, mainDirectoryPath, maxLev, fixedLev);
	indexFather = new Index(getRoleFragment(roleFather)+gender, mainDirectoryPath, maxLev, fixedLev);
	LOG.outputConsole("START: Generating Dictionary for process: " + processName);
	try {
		indexMain.openIndex();
		indexMother.openIndex();
		indexFather.openIndex();
		it = myHDT.dataset.search("", roleMain, "");
		long estNumber = it.estimatedNumResults();	
		String taskName = "Indexing " + processName;
		ProgressBar pb = null;
		try {
			pb = new ProgressBar(taskName, estNumber, indexingUpdateInterval, System.err, ProgressBarStyle.UNICODE_BLOCK, " cert.", 1);
			while(it.hasNext()) {
				TripleString ts = it.next();
				countAll++;
				String event = ts.getSubject().toString();
				Person personMain = myHDT.getPersonInfo(event, roleMain);
				if( (genderFilter == false) || (genderFilter == true && personMain.hasGender(gender)) ) {
					if(personMain.isValidWithFullName()){
						String eventID = myHDT.getIDofEvent(event);
						Person mother = myHDT.getPersonInfo(event, roleMother);
						Person father = myHDT.getPersonInfo(event, roleFather);
						Boolean motherValid = mother.isValidWithFullName();
						Boolean fatherValid = father.isValidWithFullName();
						if(motherValid && fatherValid) {	
							indexMain.addPersonToIndex(personMain, eventID, "M-F");
							indexMother.addPersonToIndex(mother, eventID, "M-F");
							indexFather.addPersonToIndex(father, eventID, "M-F");
							count_Main_Mother_Father++;
						} else {
							if(motherValid || fatherValid != false) {
								if(motherValid) {
									indexMain.addPersonToIndex(personMain, eventID, "M");
									indexMother.addPersonToIndex(mother, eventID, "M");
									count_Main_Mother++;
								} else {
									indexMain.addPersonToIndex(personMain, eventID, "F");
									indexFather.addPersonToIndex(father, eventID, "F");
									count_Main_Father++;
								}
							} else {
								count_Main++;
							}
						}
					} else {
						count_No_Main++;
					}
				} 
				if(countAll % 10000 == 0) {
					pb.stepBy(10000);
				}						
			} pb.stepTo(estNumber);
		} finally {
			pb.close();
		}
	} catch (NotFoundException e) {
		LOG.logError("generateDictionary", "Error in iterating over HDT file in process "+ processName);
		LOG.logError("", e.toString());
		return false;
	} finally {
		indexMain.closeStream();
		indexMother.closeStream();
		indexFather.closeStream();
		LOG.outputTotalRuntime("Generating Dictionary for " + processName, startTime, true);	
		countInserts = count_Main_Mother_Father + count_Main_Mother + count_Main_Father;	
		int countNonIndexed = countAll - countInserts;	
		LOG.outputConsole("");
		LOG.outputConsole("--------");
		LOG.outputConsole("- Number of Certificates: " +  countAll);
		LOG.outputConsole("- Number of Indexed Certificates: " +  countInserts); 
		if(count_Main_Mother_Father > 0) {
			LOG.outputConsole("-> Includes 3 Individuals (Main + Mother + Father): " +  count_Main_Mother_Father); 
		}
		if(count_Main_Mother > 0) {
			LOG.outputConsole("-> Includes 2 Individuals (Main + Mother ): " +  count_Main_Mother);  
		}
		if(count_Main_Father > 0) {
			LOG.outputConsole("-> Includes 2 Individuals (Main + Father): " +  count_Main_Father);  
		}
		LOG.outputConsole("- Number of Non-Indexed Certificates: " +  countNonIndexed);
		if(count_Main > 0) {
			LOG.outputConsole("-> Includes only Main Individual: " +  count_Main); 
		}
		if(count_No_Main > 0) {
			LOG.outputConsole("-> Includes no Main Individual: " +  count_No_Main);  
		}	
		LOG.outputConsole("--------");
		LOG.outputConsole("");						
	}
	return true;
}




// if known gender, person1 is the female partner and person2 is the male partner
public Boolean addToIndex(Person person1, Person person2, String eventID, Boolean knownGender) {
	if(knownGender == true) {
		indexFemalePartner.addPersonToIndex(person1, eventID);
		indexMalePartner.addPersonToIndex(person2, eventID);
		return true;
	} else {
		if(person1.isFemale() && person2.isMale()) {
			indexFemalePartner.addPersonToIndex(person1, eventID);
			indexMalePartner.addPersonToIndex(person2, eventID);
			return true;
		} else {
			if(person2.isFemale() && person1.isMale()) {
				indexFemalePartner.addPersonToIndex(person2, eventID);
				indexMalePartner.addPersonToIndex(person1, eventID);
				return true;
			}
		}
	}
	return false;
}


public String getRoleFragment(String role) {
	String[] bits = role.split("/");
	return bits[bits.length-1];
}



}
