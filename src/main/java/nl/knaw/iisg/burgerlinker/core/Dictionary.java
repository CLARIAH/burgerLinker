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
	public Index indexMain, indexMale, indexFemale, indexMother, indexFather,
                 indexPartner, indexPartnerMother, indexPartnerFather;

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
                                               bindingSet.getValue("subjectGivenName"),
                                               bindingSet.getValue("subjectFamilyName"),
                                               bindingSet.getValue("subjectGender"));

					countAll++;
					if(!genderFilter || (genderFilter && personMain.hasGender(gender)) ) {
						if(personMain.isValidWithFullName()){
							indexMain.addPersonToIndex(personMain, event, "");

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
                                            bindingSet.getValue("subjectGivenName"),
                                            bindingSet.getValue("subjectFamilyName"),
                                            bindingSet.getValue("subjectGender"));

                    countAll++;
                    if (subject.isValidWithFullName()){
                        Person partner = new Person(event,
                                                bindingSet.getValue("partnerGivenName"),
                                                bindingSet.getValue("partnerFamilyName"),
                                                bindingSet.getValue("partnerGender"));
                        if (partner.isValidWithFullName()) {
                            boolean insert = addToIndex(subject, partner, event);

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
                                               bindingSet.getValue("subjectGivenName"),
                                               bindingSet.getValue("subjectFamilyName"),
                                               bindingSet.getValue("subjectGender"));

                    countAll++;
                    if (!genderFilter || (genderFilter && personMain.hasGender(gender))) {
                        if (personMain.isValidWithFullName()){
                            Person mother = new Person(event,
                                                   bindingSet.getValue("subjectMotherGivenName"),
                                                   bindingSet.getValue("subjectMotherFamilyName"),
                                                   bindingSet.getValue("subjectMotherGender"));

                            Person father = new Person(event,
                                                   bindingSet.getValue("subjectFatherGivenName"),
                                                   bindingSet.getValue("subjectFatherFamilyName"),
                                                   bindingSet.getValue("subjectFatherGender"));

                            boolean motherValid = mother.isValidWithFullName();
                            boolean fatherValid = father.isValidWithFullName();
                            if (motherValid && fatherValid) {
                                indexMain.addPersonToIndex(personMain, event, "M-F");
                                indexMother.addPersonToIndex(mother, event, "M-F");
                                indexFather.addPersonToIndex(father, event, "M-F");

                                count_Main_Mother_Father++;
                                countInserts++;
                            } else if (motherValid) {
                                indexMain.addPersonToIndex(personMain, event, "M");
                                indexMother.addPersonToIndex(mother, event, "M");

                                count_Main_Mother++;
                                countInserts++;
                            } else if (fatherValid) {
                                indexMain.addPersonToIndex(personMain, event, "F");
                                indexFather.addPersonToIndex(father, event, "F");

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

    public boolean generateDictionarySixWay(MyRDF myRDF, String query, Person.Gender gender) {
        indexMain = new Index("subject-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexMother = new Index("subjectMother-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexFather = new Index("subjectFather-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexPartner = new Index("partner-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexPartnerMother = new Index("partnerMother-"+gender, mainDirectoryPath, maxLev, fixedLev);
        indexPartnerFather = new Index("partnerFather-"+gender, mainDirectoryPath, maxLev, fixedLev);

        long startTime = System.currentTimeMillis();

        int countInserts = 0;
        int countAll = 0;
        int count_with_father_only = 0;
        int count_with_mother_only = 0;
        int count_with_both_parents = 0;
        int count_with_partner = 0;
        int count_with_partner_mother_only = 0;
        int count_with_partner_father_only = 0;
        int count_with_partner_both_parents = 0;

        LOG.outputConsole(".: Generating Dictionary for process: " + processName + " (" + gender + ")");
        try {
            String taskName = ".: Indexing " + processName + " (" + gender + ")";

            // Keep separate indexes for subject and partner relatives since
            // they will always be on the same side of the certificate (due to
            // their gender), eg brides won't suddenly become grooms
            indexMain.openIndex();
            indexMother.openIndex();
            indexFather.openIndex();
            indexPartner.openIndex();
            indexPartnerMother.openIndex();
            indexPartnerFather.openIndex();

            TupleQueryResult qResult = null;
            ActivityIndicator spinner = new ActivityIndicator(taskName);
			try {
                spinner.start();

                qResult = myRDF.getQueryResults(query);
                for (BindingSet bindingSet: qResult) {
					String event = bindingSet.getValue("event").stringValue();

                    Person personMain = new Person(event,
                                               bindingSet.getValue("subjectGivenName"),
                                               bindingSet.getValue("subjectFamilyName"),
                                               bindingSet.getValue("subjectGender"));

                    countAll++;
                    if (personMain.isValidWithFullName()){
                        Person mainMother = new Person(event,
                                               bindingSet.getValue("subjectMotherGivenName"),
                                               bindingSet.getValue("subjectMotherFamilyName"),
                                               bindingSet.getValue("subjectMotherGender"));

                        Person mainFather = new Person(event,
                                               bindingSet.getValue("subjectFatherGivenName"),
                                               bindingSet.getValue("subjectFatherFamilyName"),
                                               bindingSet.getValue("subjectFatherGender"));

                        Person partner = new Person(event,
                                               bindingSet.getValue("partnerGivenName"),
                                               bindingSet.getValue("partnerFamilyName"),
                                               bindingSet.getValue("partnerGender"));

                        Person partnerMother = new Person(event,
                                               bindingSet.getValue("partnerMotherGivenName"),
                                               bindingSet.getValue("partnerMotherFamilyName"),
                                               bindingSet.getValue("partnerMotherGender"));

                        Person partnerFather = new Person(event,
                                               bindingSet.getValue("partnerFatherGivenName"),
                                               bindingSet.getValue("partnerFatherFamilyName"),
                                               bindingSet.getValue("partnerFatherGender"));

                        boolean mainMotherValid = mainMother.isValidWithFullName();
                        boolean mainFatherValid = mainFather.isValidWithFullName();
                        boolean partnerValid = partner.isValidWithFullName();
                        boolean partnerMotherValid = partnerMother.isValidWithFullName();
                        boolean partnerFatherValid = partnerFather.isValidWithFullName();

                        String tag = "";
                        if (partnerValid) {
                            tag += "[PARTNER]";
                        }
                        if (mainMotherValid) {
                            tag += "[SUBJECTMOTHER]";
                        }
                        if (mainFatherValid) {
                            tag += "[SUBJECTFATHER]";
                        }
                        if (partnerMotherValid) {
                            tag += "[PARTNERMOTHER]";
                        }
                        if (partnerFatherValid) {
                            tag += "[PARTNERFATHER]";
                        }
                        if (tag.length() > 0) {
                            indexMain.addPersonToIndex(personMain, event, tag);

                            if (partnerValid) {
                                indexPartner.addPersonToIndex(partner, event, tag);
                                count_with_partner += 1;

                                if (partnerMotherValid) {
                                    indexPartnerMother.addPersonToIndex(partnerMother, event, tag);
                                }
                                if (partnerFatherValid) {
                                    indexPartnerFather.addPersonToIndex(partnerFather, event, tag);
                                }
                            }
                            if (mainMotherValid) {
                                indexMother.addPersonToIndex(mainMother, event, tag);
                            }
                            if (mainFatherValid) {
                                indexFather.addPersonToIndex(mainFather, event, tag);
                            }

                            if (mainMotherValid && !mainFatherValid) {
                                count_with_mother_only += 1;
                            } else if (!mainMotherValid && mainFatherValid) {
                                count_with_father_only += 1;
                            } else if (mainMotherValid && mainFatherValid) {
                                count_with_both_parents += 1;
                            }

                            if (partnerMotherValid && !partnerFatherValid) {
                                count_with_partner_mother_only += 1;
                            } else if (!partnerMotherValid && partnerFatherValid) {
                                count_with_partner_father_only += 1;
                            } else if (partnerMotherValid && partnerFatherValid) {
                                count_with_partner_both_parents += 1;
                            }

                            countInserts++;
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
            indexPartner.closeStream();
            indexPartnerMother.closeStream();
            indexPartnerFather.closeStream();

            int countNonIndexed = countAll - countInserts;

            DecimalFormat formatter = new DecimalFormat("#,###");
            LinkedHashMap<String, String> summary = new LinkedHashMap<>();
            summary.put("Certificates Total", formatter.format(countAll));
            summary.put("Certificates Indexed", formatter.format(countInserts));
            summary.put(" with mother only", formatter.format(count_with_mother_only));
            summary.put(" with father only", formatter.format(count_with_father_only));
            summary.put(" with both parents", formatter.format(count_with_both_parents));
            summary.put(" with partner", formatter.format(count_with_partner));
            summary.put("  with mother only", formatter.format(count_with_partner_mother_only));
            summary.put("  with father only", formatter.format(count_with_partner_father_only));
            summary.put("  with both parents", formatter.format(count_with_partner_both_parents));

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
    public boolean addToIndex(Person person1, Person person2, String event) {
        if (person1.isFemale()) {  // && person2.isMale()) {
            indexFemale.addPersonToIndex(person1, event);
            indexMale.addPersonToIndex(person2, event);

            return true;
        } else if (person2.isFemale()) {  // && person1.isMale()) {
            indexFemale.addPersonToIndex(person2, event);
            indexMale.addPersonToIndex(person1, event);

            return true;
        }

        return false;
    }
}
