package nl.knaw.iisg.burgerlinker.core;


import java.io.File;
import java.text.DecimalFormat;
import java.util.List;
import java.util.LinkedHashMap;

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
	public Index indexMain, indexMale, indexFemale, indexMother, indexFather;

	public static final Logger lg = LogManager.getLogger(Dictionary.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);

	public Dictionary(String processName, File mainDirectoryPath, int maxLev, Boolean fixedLev) {
		this.processName = processName;
		this.mainDirectoryPath = mainDirectoryPath;
		this.maxLev = maxLev;
		this.fixedLev = fixedLev;
	}

	public boolean generateDictionaryOneWay(MyRDF myRDF, String query, boolean genderFilter, Person.Gender gender) {
		indexMain = new Index("subject-"+gender, mainDirectoryPath, maxLev, fixedLev);

		long startTime = System.currentTimeMillis();

		int countInserts = 0;
        int countAll = 0;
        int count_No_Main = 0;

		LOG.outputConsole(".: Generating Dictionary for process: " + processName + " (" + gender + ")");
		try {
			String taskName = ".: Indexing " + processName + " (" + gender + ")";

			indexMain.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();

                    Person personMain = new Person(event,
                                               bindingSet.getValue("givenNameSubject"),
                                               bindingSet.getValue("familyNameSubject"),
                                               bindingSet.getValue("genderSubject"));

					countAll++;
					if(!genderFilter || (genderFilter && personMain.hasGender(gender)) ) {
						if(personMain.isValidWithFullName()){
							String eventID = bindingSet.getValue("eventID").stringValue();
							indexMain.addPersonToIndex(personMain, eventID, "");

							countInserts++;
						}
					} else {
						count_No_Main++;
					}

                    if(countAll % 5000 == 0) {
                        spinner.update(countAll);
                    }
			    }
            } finally {
                qResult.close();
                spinner.terminate();
                spinner.join();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
            LOG.logError("", e.toString());

            return false;
        } finally {
            indexMain.closeStream();

            int countNonIndexed = countAll - countInserts;

            DecimalFormat formatter = new DecimalFormat("#,###");
            LinkedHashMap<String, String> summary = new LinkedHashMap<>();
            summary.put("Certificates Total", formatter.format(countAll));
            summary.put("Certificates Indexed", formatter.format(countInserts));

            int keyLenMax = 0, valLenMax = 0;
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                if (val.length() > valLenMax) {
                    valLenMax -= val.length();
                }
                if (key.length() > keyLenMax) {
                    keyLenMax = key.length();
                }
            }

            LOG.outputConsole(".: Index Summary");
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                LOG.outputConsole("   - " + String.format("%-" + keyLenMax + "s", key)
                                  + "   " + String.format("%" + valLenMax + "s", val));
            }
        }

        return true;
    }

    public boolean generateDictionaryTwoWay(MyRDF myRDF, String query) {
        indexMale = new Index("males", mainDirectoryPath, maxLev, fixedLev);
        indexFemale = new Index("females", mainDirectoryPath, maxLev, fixedLev);

        long startTime = System.currentTimeMillis();

        int countInserts = 0;
        int countAll = 0;

        LOG.outputConsole(".: Generating Dictionary for process: " + processName);
        try {
            String taskName = ".: Indexing " + processName;

            indexMale.openIndex();
            indexFemale.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();
                    Person subject = new Person(event,
                                            bindingSet.getValue("givenNameSubject"),
                                            bindingSet.getValue("familyNameSubject"),
                                            bindingSet.getValue("genderSubject"));

                    countAll++;
                    if (subject.isValidWithFullName()){
                        String eventID = bindingSet.getValue("eventID").stringValue();

                        Person partner = new Person(event,
                                                bindingSet.getValue("givenNamePartner"),
                                                bindingSet.getValue("familyNamePartner"),
                                                bindingSet.getValue("genderPartner"));
                        if (partner.isValidWithFullName()) {
                            boolean insert = addToIndex(subject, partner, eventID);

                            if(insert) {
                                countInserts++;
                            }
                        }

                    }

                    if(countAll % 5000 == 0) {
                        spinner.update(countAll);
                    }
                }
            } finally {
                qResult.close();
                spinner.terminate();
                spinner.join();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
            LOG.logError("", e.toString());

            return false;
        } finally {
            indexMale.closeStream();
            indexFemale.closeStream();

            int countNonIndexed = countAll - countInserts;

            DecimalFormat formatter = new DecimalFormat("#,###");
            LinkedHashMap<String, String> summary = new LinkedHashMap<>();
            summary.put("Certificates Total", formatter.format(countAll));
            summary.put("Certificates Indexed", formatter.format(countInserts));

            int keyLenMax = 0, valLenMax = 0;
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                if (val.length() > valLenMax) {
                    valLenMax -= val.length();
                }
                if (key.length() > keyLenMax) {
                    keyLenMax = key.length();
                }
            }

            LOG.outputConsole(".: Index Summary");
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                LOG.outputConsole("   - " + String.format("%-" + keyLenMax + "s", key)
                                  + "   " + String.format("%" + valLenMax + "s", val));
            }
        }

        return true;
    }

    public boolean generateDictionaryThreeWay(MyRDF myRDF, String query, boolean genderFilter, Person.Gender gender) {
        indexMain = new Index("subject-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexMother = new Index("subjectMother-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexFather = new Index("subjectFather-"+gender, mainDirectoryPath, maxLev, fixedLev);

        long startTime = System.currentTimeMillis();

        int countInserts = 0;
        int countAll = 0;
        int count_Main_Mother_Father = 0;
        int count_Main_Mother = 0;
        int count_Main_Father = 0;

        LOG.outputConsole(".: Generating Dictionary for process: " + processName + " (" + gender + ")");
        try {
            String taskName = ".: Indexing " + processName + " (" + gender + ")";

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
                                               bindingSet.getValue("givenNameSubject"),
                                               bindingSet.getValue("familyNameSubject"),
                                               bindingSet.getValue("genderSubject"));

                    countAll++;
                    if (!genderFilter || (genderFilter && personMain.hasGender(gender))) {
                        if (personMain.isValidWithFullName()){
							String eventID = bindingSet.getValue("eventID").stringValue();

                            Person mother = new Person(event,
                                                   bindingSet.getValue("givenNameSubjectMother"),
                                                   bindingSet.getValue("familyNameSubjectMother"),
                                                   bindingSet.getValue("genderSubjectMother"));

                            Person father = new Person(event,
                                                   bindingSet.getValue("givenNameSubjectFather"),
                                                   bindingSet.getValue("familyNameSubjectFather"),
                                                   bindingSet.getValue("genderSubjectFather"));

                            boolean motherValid = mother.isValidWithFullName();
                            boolean fatherValid = father.isValidWithFullName();
                            if (motherValid && fatherValid) {
                                indexMain.addPersonToIndex(personMain, eventID, "M-F");
                                indexMother.addPersonToIndex(mother, eventID, "M-F");
                                indexFather.addPersonToIndex(father, eventID, "M-F");

                                count_Main_Mother_Father++;
                                countInserts++;
                            } else if (motherValid) {
                                indexMain.addPersonToIndex(personMain, eventID, "M");
                                indexMother.addPersonToIndex(mother, eventID, "M");

                                count_Main_Mother++;
                                countInserts++;
                            } else if (fatherValid) {
                                indexMain.addPersonToIndex(personMain, eventID, "F");
                                indexFather.addPersonToIndex(father, eventID, "F");

                                count_Main_Father++;
                                countInserts++;
                            }
                        }
                    }
                    if(countAll % 5000 == 0) {
                        spinner.update(countAll);
                    }
                }
            } finally {
                qResult.close();
                spinner.terminate();
                spinner.join();
            }
        } catch (Exception e) {
            LOG.logError("generateDictionary", "Error in iterating over RDF file in process "+ processName);
            LOG.logError("", e.toString());

            return false;
        } finally {
            indexMain.closeStream();
            indexMother.closeStream();
            indexFather.closeStream();

            int countNonIndexed = countAll - countInserts;

            DecimalFormat formatter = new DecimalFormat("#,###");
            LinkedHashMap<String, String> summary = new LinkedHashMap<>();
            summary.put("Certificates Total", formatter.format(countAll));
            summary.put("Certificates Indexed", formatter.format(countInserts));
            summary.put(" with both parents", formatter.format(count_Main_Mother_Father));
            summary.put(" with mother only", formatter.format(count_Main_Mother));
            summary.put(" with father only", formatter.format(count_Main_Father));

            int keyLenMax = 0, valLenMax = 0;
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                if (val.length() > valLenMax) {
                    valLenMax -= val.length();
                }
                if (key.length() > keyLenMax) {
                    keyLenMax = key.length();
                }
            }

            LOG.outputConsole(".: Index Summary");
            for (String key: summary.keySet()) {
                String val = summary.get(key);
                LOG.outputConsole("   - " + String.format("%-" + keyLenMax + "s", key)
                                  + "   " + String.format("%" + valLenMax + "s", val));
            }
        }

        return true;
    }

    // if known gender, person1 is the female partner and person2 is the male partner
    public boolean addToIndex(Person person1, Person person2, String eventID) {
        if (person1.isFemale()) {  // && person2.isMale()) {
            indexFemale.addPersonToIndex(person1, eventID);
            indexMale.addPersonToIndex(person2, eventID);

            return true;
        } else if (person2.isFemale()) {  // && person1.isMale()) {
            indexFemale.addPersonToIndex(person2, eventID);
            indexMale.addPersonToIndex(person1, eventID);

            return true;
        }

        return false;
    }
}
