package nl.knaw.iisg.burgerlinker.core;


import java.io.File;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;

import nl.knaw.iisg.burgerlinker.data.MyRDF;
import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class Dictionary {
	public String processName;
	public File mainDirectoryPath;
	public int maxLev;
	public Boolean fixedLev;
	public Index indexFemalePartner, indexMalePartner, indexMain, indexMother, indexFather;
	private final int indexingUpdateInterval = 2000;

	public static final Logger lg = LogManager.getLogger(Dictionary.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	public Dictionary(String processName, File mainDirectoryPath, int maxLev, Boolean fixedLev) {
		this.processName = processName;
		this.mainDirectoryPath = mainDirectoryPath;
		this.maxLev = maxLev;
		this.fixedLev = fixedLev;
	}

	public boolean generateDictionaryOneWay(MyRDF myRDF, String query, boolean genderFilter, String gender) {
		indexMain = new Index("subject-"+gender, mainDirectoryPath, maxLev, fixedLev);

		long startTime = System.currentTimeMillis();

		int countInserts = 0;
        int countAll = 0;
        int count_No_Main = 0;

		LOG.outputConsole("START: Generating Dictionary for process: " + processName);
		try {
			String taskName = "Indexing " + processName;

			indexMain.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();

                    Person personMain = new Person(event,
                                               bindingSet.getValue("givenNameSubject").stringValue(),
                                               bindingSet.getValue("familyNameSubject").stringValue(),
                                               bindingSet.getValue("genderSubject").stringValue());

					countAll++;
					if( (genderFilter == false) || (genderFilter == true && personMain.hasGender(gender)) ) {
						if(personMain.isValidWithFullName()){
							String eventID = bindingSet.getValue("eventID").stringValue();
							indexMain.addPersonToIndex(personMain, eventID, "");

							countInserts++;
						}
					} else {
						count_No_Main++;
					}

                    if(countAll % 10000 == 0) {
                        spinner.update(countAll);
                    }
			    }
            } finally {
                qResult.close();
                spinner.terminate();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
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

    public boolean generateDictionaryTwoWay(MyRDF myRDF, String query, boolean knownGender) {
        Index indexSubject = new Index("subject", mainDirectoryPath, maxLev, fixedLev);
        Index indexPartner = new Index("partner", mainDirectoryPath, maxLev, fixedLev);

        long startTime = System.currentTimeMillis();

        int countInserts = 0;
        int countAll = 0;

        LOG.outputConsole("START: Generating Dictionary for process: " + processName);
        try {
            String taskName = "Indexing " + processName;

            indexSubject.openIndex();
            indexPartner.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();
                    Person subject = new Person(event,
                                            bindingSet.getValue("givenNameSubject").stringValue(),
                                            bindingSet.getValue("familyNameSubject").stringValue(),
                                            bindingSet.getValue("genderSubject").stringValue());

                    countAll++;
                    if (subject.isValidWithFullName()){
                        String eventID = bindingSet.getValue("eventID").stringValue();

                        Person partner = new Person(event,
                                                bindingSet.getValue("givenNamePartner").stringValue(),
                                                bindingSet.getValue("familyNamePartner").stringValue(),
                                                bindingSet.getValue("genderPartner").stringValue());
                        if(partner.isValidWithFullName()) {
                            boolean insert = addToIndex(subject, partner, eventID, knownGender);

                            if(insert) {
                                countInserts++;
                            }
                        }

                    }

                    if(countAll % 10000 == 0) {
                        spinner.update(countAll);
                    }
                }
            } finally {
                qResult.close();
                spinner.terminate();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
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

    public boolean generateDictionaryThreeWay(MyRDF myRDF, String query, boolean genderFilter, String gender) {
        indexMain = new Index("subject-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexMother = new Index("subjectMother-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexFather = new Index("subjectFather-"+gender, mainDirectoryPath, maxLev, fixedLev);

        long startTime = System.currentTimeMillis();

        int countInserts = 0;
        int countAll = 0;
        int count_Main_Mother_Father = 0;
        int count_Main_Mother = 0;
        int count_Main_Father = 0;
        int count_Main = 0;
        int count_No_Main = 0;

        LOG.outputConsole("START: Generating Dictionary for process: " + processName);
        try {
            String taskName = "Indexing " + processName;

            indexMain.openIndex();
            indexMother.openIndex();
            indexFather.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();

                    Person personMain = new Person(event,
                                               bindingSet.getValue("givenNameSubject").stringValue(),
                                               bindingSet.getValue("familyNameSubject").stringValue(),
                                               bindingSet.getValue("genderSubject").stringValue());

                    countAll++;
                    if( (genderFilter == false) || (genderFilter == true && personMain.hasGender(gender)) ) {
                        if(personMain.isValidWithFullName()){
							String eventID = bindingSet.getValue("eventID").stringValue();

                            Person mother = new Person(event,
                                                   bindingSet.getValue("givenNameSubjectMother").stringValue(),
                                                   bindingSet.getValue("familyNameSubjectMother").stringValue(),
                                                   bindingSet.getValue("genderSubjectMother").stringValue());

                            Person father = new Person(event,
                                                   bindingSet.getValue("givenNameSubjectFather").stringValue(),
                                                   bindingSet.getValue("familyNameSubjectFather").stringValue(),
                                                   bindingSet.getValue("genderSubjectFather").stringValue());

                            boolean motherValid = mother.isValidWithFullName();
                            boolean fatherValid = father.isValidWithFullName();
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
                        spinner.update(countAll);
                    }
                }
            } finally {
                qResult.close();
                spinner.terminate();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
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
}
