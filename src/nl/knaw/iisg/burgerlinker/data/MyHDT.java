package nl.knaw.iisg.burgerlinker.data;


import static nl.knaw.iisg.burgerlinker.Properties.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.FileUtilities;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;

import org.apache.jena.query.ARQ;


public class MyHDT implements ProgressListener {
	public HDT dataset;
	public HDT targetDataset;
	public boolean doubleInputs = false;
    private Map<String, String> dataModel;
    private String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";


	public static final Logger lg = LogManager.getLogger(MyHDT.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

	/**
	 * Constructor
	 *
	 * @param hdt_file_path
	 *            Path of the HDT file
	 */
	public MyHDT(String hdtPath, Map<String, String> dataModel) {  // One HDT file with its index are given as input
		try {
			ARQ.init();
			LOG.outputConsole("START: Loading HDT dataset...");

			long startTime = System.currentTimeMillis();
            this.dataModel = dataModel;
			dataset = HDTManager.loadIndexedHDT(hdtPath, null);

			LOG.outputTotalRuntime("Loading HDT dataset", startTime, true);
			LOG.outputConsole("");
			LOG.outputConsole("--------");
			LOG.outputConsole("- Number of statements in this dataset: "+ dataset.getTriples().getNumberOfElements());
			LOG.outputConsole("- Number of distinct subjects: "+ dataset.getDictionary().getNsubjects());
			LOG.outputConsole("- Number of distinct predicates: "+ dataset.getDictionary().getNpredicates());
			LOG.outputConsole("- Number of distinct objects: "+ dataset.getDictionary().getNobjects());
			LOG.outputConsole("--------");
			LOG.outputConsole("");
			System.out.println();
		} catch (IOException e) {
			LOG.logError("MyHDT_Constructor", "Error loading HDT dataset");
			e.printStackTrace();
		}
	}

	public MyHDT(String hdtPath1, String hdtPath2, Boolean doubleInputs,
                 Map<String, String> dataModel) {  // Two HDT files with their index are given as inputs
		try {
			doubleInputs = true;

			ARQ.init();
			LOG.outputConsole("START: Loading HDT datasets...");

			long startTime = System.currentTimeMillis();
            this.dataModel = dataModel;
			dataset = HDTManager.loadIndexedHDT(hdtPath1, null);
			targetDataset = HDTManager.loadIndexedHDT(hdtPath2, null);

			LOG.outputTotalRuntime("Loading HDT dataset", startTime, true);
			LOG.outputConsole("");
			LOG.outputConsole("Dataset 1: " + hdtPath1);
			LOG.outputConsole("--------");
			LOG.outputConsole("- Number of statements in this dataset: "+ dataset.getTriples().getNumberOfElements());
			LOG.outputConsole("- Number of distinct subjects: "+ dataset.getDictionary().getNsubjects());
			LOG.outputConsole("- Number of distinct predicates: "+ dataset.getDictionary().getNpredicates());
			LOG.outputConsole("- Number of distinct objects: "+ dataset.getDictionary().getNobjects());
			LOG.outputConsole("--------");
			LOG.outputConsole("");
			LOG.outputConsole("");
			LOG.outputConsole("Dataset 2: " + hdtPath2);
			LOG.outputConsole("--------");
			LOG.outputConsole("- Number of statements in this dataset: "+ targetDataset.getTriples().getNumberOfElements());
			LOG.outputConsole("- Number of distinct subjects: "+ targetDataset.getDictionary().getNsubjects());
			LOG.outputConsole("- Number of distinct predicates: "+ targetDataset.getDictionary().getNpredicates());
			LOG.outputConsole("- Number of distinct objects: "+ targetDataset.getDictionary().getNobjects());
			LOG.outputConsole("--------");
			LOG.outputConsole("");
			System.out.println();
		} catch (IOException e) {
			LOG.logError("MyHDT_Constructor", "Error loading HDT datasets");
			e.printStackTrace();
		}
	}

	public MyHDT(String inputRDF, String outputDir) { // One RDF file is given as input to be converted to HDT
		ARQ.init();
		long startTime = System.currentTimeMillis();

		String baseURI = "file://" + inputRDF;
		String fileName = FILE_UTILS.getFileName(inputRDF);
		String outputHDT = outputDir + "/" + fileName + ".hdt";

		RDFNotation notation = null;
		try {
			notation =  RDFNotation.guess(inputRDF);
		} catch (IllegalArgumentException e) {
			LOG.logError("MyHDT Constructor", "Could not guess notation for "+inputRDF+" - Trying NTriples...");
			notation = RDFNotation.NTRIPLES;
		}

		HDT hdt;
		try {
			LOG.outputConsole("START: Generating HDT dataset...");

			hdt = HDTManager.generateHDT(inputRDF, baseURI, notation, new HDTSpecification(), null);
			hdt.saveToHDT(outputHDT, null);

			LOG.outputTotalRuntime("HDT generated and saved at "+ outputHDT, startTime, true);

			startTime = System.currentTimeMillis();
			LOG.outputConsole("START: Generating HDT index...");

			hdt = HDTManager.indexedHDT(hdt, null);
			LOG.outputTotalRuntime("Index generated and saved at: "+ outputHDT+".index", startTime, true);

			hdt.close();
		} catch (IOException | ParserException e) {
			LOG.logError("MyHDT Constructor", "Problem generating HDT file");
			LOG.logError("MyHDT Constructor", e.getLocalizedMessage());
		}
	}

	public MyHDT(String inputHDT1, String inputHDT2, String outputDir) { // Two HDT files given as input to be merged
		ARQ.init();

		long startTime = System.currentTimeMillis();

        String outputHDT = outputDir + "/merged-dataset.hdt";
		File file = new File(outputHDT);
		File theDir = new File(file.getAbsolutePath()+"_tmp");

        theDir.mkdirs();
		String location = theDir.getAbsolutePath()+"/";

        HDT hdt;
		try {
			LOG.outputConsole("START: Merging HDT datasets...");

			hdt = HDTManager.catHDT(location, inputHDT1, inputHDT2, new HDTSpecification(), null);
			hdt.saveToHDT(outputHDT, null);

			LOG.outputTotalRuntime("HDT merged and saved at "+ outputHDT, startTime, true);

			Files.delete(Paths.get(location+"dictionary"));
			Files.delete(Paths.get(location+"triples"));

			theDir.delete();

			startTime = System.currentTimeMillis();
			LOG.outputConsole("START: Generating HDT index...");

			hdt = HDTManager.indexedHDT(hdt, null);
			LOG.outputTotalRuntime("Index generated and saved at: "+ outputHDT+".index", startTime, true);

			hdt.close();
		} catch (IOException e) {
			LOG.logError("MyHDT Constructor", "Problem generating HDT file");
			LOG.logError("MyHDT Constructor", e.getLocalizedMessage());
		}
	}

	// ===== Statistics =====

	/**
	 * Returns the exact number of triples in this HDT file
	 * @param output
	 *            true for displaying the number of triples in the console
	 */
	public int getExactNumberOfTriples(){
		int counterTriples = 0;
		try {
			IteratorTripleString it = dataset.search("", "", "");
			while(it.hasNext()) {
				it.next();

				counterTriples++;
			}
		} catch (NotFoundException e) {
			LOG.logError("getExactNumberOfTriples", "Error counting triples in HDT dataset");
			e.printStackTrace();
		}

		return counterTriples;
	}

	/**
	 * Returns the number of registrations
	 *
	 * @param certificate_type
	 *            "Birth_Certificate",
	 *            "Marriage_Certificate" or
	 *            "Death_Certificate"
	 */
	public int getNumberOfSubjects(String object){
		int counterRegistrations = 0;

		try {
			IteratorTripleString it = dataset.search("", RDF_TYPE, object);
			while(it.hasNext()) {
				it.next();

				counterRegistrations++;
			}
		} catch (NotFoundException e) {
			LOG.logError("getNumberOfSubjects", "Error counting subjects of object '" + object + "' in HDT dataset");
			e.printStackTrace();
		}

		return counterRegistrations;
	}

	// ===== RDF Utility Functions =====

	/**
	 * Returns the actual value of a typed literal
	 * (e.g. returns the String "John" from the input "John"^^xsd:string)
	 *
	 * @param typed_literal
	 */
	public String getStringValueFromLiteral(String typed_literal) {
		try {
			if (typed_literal != null) {
				typed_literal = typed_literal.substring(typed_literal.indexOf('"')+ 1);
				typed_literal = typed_literal.substring(0, typed_literal.indexOf('"'));
			}
		} catch (Exception e) {
			LOG.logError("getStringValueFromLiteral", "Error in converting Literal to String for input string: " + typed_literal);
			LOG.logError("getStringValueFromLiteral", e.getLocalizedMessage());
		}

		return typed_literal;
	}

	/**
	 * Converts a Java String to xsd:String
	 * (e.g. returns the String "John"^^xsd:string from the input Java String "John")
	 *
	 * @param typed_literal
	 */
	public String convertStringToTypedLiteral(String literal) {
		// "johannes franciescus"^^<http://www.w3.org/2001/XMLSchema#string>
		try {
			if (literal != null) {
				literal = '"' + literal + '"' + "^^<http://www.w3.org/2001/XMLSchema#string>";
			}
		} catch (Exception e) {
			LOG.logError("convertStringToTypedLiteral", "Error in converting String to Literal for input string: " + literal);
			LOG.logError("convertStringToTypedLiteral", e.getLocalizedMessage());
		}
		return literal;
	}

	/**
	 * Converts a Java String to xsd:String
	 * (e.g. returns the Integer "123"^^xsd:int from the input Java String "123")
	 *
	 * @param typed_literal
	 */
	public String convertStringToTypedInteger(String literal, String type) {
		try {
			if (literal != null) {
				literal = '"' + literal + '"' + "^^<http://www.w3.org/2001/XMLSchema#" + type +">";
			}
		} catch (Exception e) {
			LOG.logError("convertStringToTypedInteger", "Error in converting String to Integer for input string: " + literal);
			LOG.logError("convertStringToTypedInteger", e.getLocalizedMessage());
		}

		return literal;
	}

	public String convertStringToTypedInt(String literal) {
		try {
			if (literal != null) {
				literal = '"' + literal + '"' + "^^<http://www.w3.org/2001/XMLSchema#int>";
			}
		} catch (Exception e) {
			LOG.logError("convertStringToTypedInteger", "Error in converting String to Integer for input string: " + literal);
			LOG.logError("convertStringToTypedInteger", e.getLocalizedMessage());
		}

		return literal;
	}

	public void closeDataset() {
		try {
			dataset.close();
		} catch (Exception e) {
			LOG.logError("closeDataset", "Error while closing HDT file");
			LOG.logError("closeDataset", e.getLocalizedMessage());
		}
	}

	// ===== Dataset Specific Functions =====

	/**
	 * Returns the ID provided in the original CSV file of a certain life event
	 *
	 * @param eventURI
	 * 		the URI of a life event
	 */
	//	public String getIDofEvent(String eventURI) {
	//		// Of course the more correct way would be to query the HDT file and get the registration ID from the event URI
	//		String[] bits = eventURI.split("/");
	//		return bits[bits.length-1];
	//	}

	public String getIDofEvent(String eventURI) {
		//		try {
		IteratorTripleString it;
		try {
			it = dataset.search(eventURI, this.dataModel.get("event_registration_identifier"), "");
			if(it.hasNext()) {
				TripleString ts = it.next();

				String eventID = ts.getObject().toString();
				if (eventID != null) {

                    return getStringValueFromLiteral(eventID);
				} else {
					LOG.logError("getIDofEvent", "The ID for the following event URI is not found in the dataset: " + eventURI);
				}
			} else {
				if(eventURI.contains("e-")) {
					String test = eventURI.substring(eventURI.lastIndexOf("-") + 1);

                    return test;
				}
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}
		LOG.logError("getIDofEvent", "The ID for the following event URI is not found in the dataset: " + eventURI);

		return null;
	}

	//	public String getIDofPerson(String personURI) {
	//		// Of course the more correct way would be to query the HDT file and get the registration ID from the event URI
	//		String[] bits = personURI.split("/");
	//		return bits[bits.length-1];
	//	}

	public String getIDofPerson(String personURI) {
		try {
			IteratorTripleString it = dataset.search(personURI, this.dataModel.get("person_identifier"), "");
			if(it.hasNext()) {
				TripleString ts = it.next();

				String personID = ts.getObject().toString();
				if (personID != null) {
					return getStringValueFromLiteral(personID);
				} else {
					LOG.logError("getIDofPerson", "The ID for the following person URI is not found in the dataset: " + personURI);
				}
			}
		}
		catch (NotFoundException e) {
			LOG.logError("getIDofPerson", "The ID for the following person URI is not found in the dataset: " + personURI);
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Returns an Object of the Java Class Person with their personal details (first name, last name, and gender) extracted from the HDT
	 *
	 * @param event_uri
	 * 		URI referring to the event in which this individual is part of
	 * @param role
	 * 		role of this person in this event with its acronym
	 */
	public Person getPersonInfo(String eventURI, String role) {
		try {
			IteratorTripleString it = dataset.search(eventURI, role, "");
			while(it.hasNext()) {
				TripleString ts = it.next();

				Person p = new Person(ts.getObject(), role);

				p.setFirstName(getFirstNameFromHDT(ts.getObject()));
				p.setLastName(getLastNameFromHDT(ts.getObject()));
				p.setGender(getGenderFromHDT(ts.getObject()));

				return p;
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return new Person();
	}

	public int getAgeFromHDT(String personURI) {
		int age = 999;
		try {
			IteratorTripleString it = dataset.search(personURI, this.dataModel.get("person_age"), "");
			if(it.hasNext()){
				TripleString ts = it.next();

				age = Integer.parseInt(getStringValueFromLiteral(ts.getObject().toString()));
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return age;
	}

	/**
	 * Returns the object (?o) of the statement (URI, foaf:firstName, ?o)
	 *
	 * @param URI
	 * 		URI referring to a certain person
	 */
	public String getFirstNameFromHDT(CharSequence URI) {
		String first_name = null;
		try {
			IteratorTripleString it = dataset.search(URI, this.dataModel.get("person_given_name"), "");
			if(it.hasNext()){
				TripleString ts = it.next();

				first_name = ts.getObject().toString();
				first_name = getStringValueFromLiteral(first_name);
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return first_name;
	}

	/**
	 * Returns the object (?o) of the statement (URI, foaf:lastName, ?o)
	 *
	 * @param URI
	 * 		URI referring to a certain person
	 */
	public String getLastNameFromHDT(CharSequence URI) {
		String last_name = null;
		try {
			IteratorTripleString it = dataset.search(URI, this.dataModel.get("person_family_name"), "");
			if(it.hasNext()){
				TripleString ts = it.next();

				last_name = ts.getObject().toString();
				last_name = getStringValueFromLiteral(last_name);
			}
        } catch (NotFoundException e) {
			e.printStackTrace();
		}

		return last_name;
	}

	/**
	 * Returns the object (?o) of the statement (URI, foaf:gender, ?o)
	 *
	 * @param URI
	 * 		URI referring to a certain person
	 */
	public String getGenderFromHDT(CharSequence URI) {
		String gender = "u";
		try {
			IteratorTripleString it = dataset.search(URI, this.dataModel.get("person_gender"), "");
			if(it.hasNext()){
				TripleString ts = it.next();

				gender = ts.getObject().toString();
				if(gender.equals("f")
                   || gender.equals(this.dataModel.get("person_gender_female"))
                   || gender.equals("\"f\"")) {
					return "f";
				} else {
					if(gender.equals("m")
                       || gender.equals(this.dataModel.get("person_gender_male"))
                       || gender.equals("\"m\"")) {
						return "m";
					}
				}
				//gender = getStringValueFromLiteral(gender);
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return gender;
	}

	public int countNewbornsByGender(String gender) {
		int result = 0;
		try {
			IteratorTripleString it = dataset.search("", this.dataModel.get("role_newborn"), "");
			while(it.hasNext()){
				TripleString ts = it.next();

				String newborn = ts.getObject().toString();
				if(getGenderFromHDT(newborn).equals(gender)) {
					result++;
				}
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return result;
	}

	/**
	 * Returns the object (?o) of the statement (URI, iisg-vocab:event_date, ?o)
	 *
	 * @param URI
	 * 		URI referring to a certain event
	 */
	public int getEventDate(String eventURI) {
		try {
			IteratorTripleString it = dataset.search(eventURI, this.dataModel.get("event_date"), "");
			while(it.hasNext()) {
				TripleString ts = it.next();

				String[] bits = ts.getObject().toString().split("-");
				try {
					int returnedDate = Integer.parseInt(bits[0].replace("\"", ""));

					return returnedDate;
				} catch (Exception e) {
					return 0;
				}
			}
		} catch (NotFoundException e) {
			//e.printStackTrace();
		}

		return 0;
	}

    /**
	 * Returns the event URI from its ID provided in the original CSV file
	 * @param eventID
	 * 		the ID of this event
	 */
	public String getEventURIfromID(String eventID, String type) {
		try {
			String typedEventID = convertStringToTypedInteger(eventID, type);

			IteratorTripleString it = dataset.search("", this.dataModel.get("event_registration_identifier"), typedEventID);
			if(it.hasNext()) {
				TripleString ts = it.next();

				String eventURI = ts.getSubject().toString();
				if (eventURI != null) {
					return eventURI;
				} else {
					LOG.logError("getEventURIfromID", "The following eventID is not found in the dataset: " + eventID);
				}
			}
		}
		catch (NotFoundException e) {
			LOG.logError("getEventURIfromID", "The following eventID is not found in the dataset: " + eventID);
			e.printStackTrace();
		}

		return null;
	}

	public String getEventURIfromID(String eventID) {
		String result = getEventURIfromID(eventID, "integer");
		if(result == null) {
			result = getEventURIfromID(eventID, "int");
			if (result == null) {
				LOG.logError("getEventURIfromID", "The following eventID is not found in the dataset: " + eventID);
			}
		}

		return result;
	}

	public String getPersonID(String eventID, String role) {
		String eventURI = getEventURIfromID(eventID);
		try {
			IteratorTripleString it = dataset.search(eventURI, role, "");
			while(it.hasNext()) {
				TripleString ts = it.next();

				String personURI = ts.getObject().toString();

				return getIDofPerson(personURI);
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	public String getPersonID(String eventID, String role1, String role2, String familyLine) {
		String role = getPropertyFromFamilyLine(role1, role2, familyLine);
		String eventURI = getEventURIfromID(eventID);
		try {
			IteratorTripleString it = dataset.search(eventURI, role, "");
			while(it.hasNext()) {
				TripleString ts = it.next();

				String personURI = ts.getObject().toString();

				return getIDofPerson(personURI);
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	public String getPropertyFromFamilyLine(String role1, String role2, String familyLine) {
		String role;
		if (familyLine.equals("21")) { // bride line
			if(role1.contains("bride") || role1.contains("Bride")) {
				role = role1 ;
			} else {
				role = role2 ;
			}
		} else { // groom line
			if(role1.contains("groom") || role1.contains("Groom")) {
				role = role1 ;
			} else {
				role = role2 ;
			}
		}


		return role;
	}

	public String removeBrackets(String someURI) {
		if(someURI.startsWith("<")) {
			return someURI.substring(1, someURI.length()-1);
		} else {
			return someURI;
		}
	}

	public String convertToYearType(int age) {
		return "\"" + Integer.toString(age) + "\"^^<http://www.w3.org/2001/XMLSchema#gYear>";
	}

	public String getBirthYearFromAge(String personURI, int age) {
		String sbj = null;
		String person = removeBrackets(personURI);
		try {
			IteratorTripleString it = dataset.search("", "", person);
			if(it.hasNext()){
				TripleString ts = it.next();

				sbj = ts.getSubject().toString();
				int eventYear = getEventDate(sbj) ;
				if(eventYear != 0) {
					int birthYear = eventYear - age ;
					return convertToYearType(birthYear);
				}
			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void notifyProgress(float level, String message) {
		// TODO Auto-generated method stub

	}
}
