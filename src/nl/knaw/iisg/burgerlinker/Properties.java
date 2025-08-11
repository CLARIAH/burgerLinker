package nl.knaw.iisg.burgerlinker;

public final class Properties {

	// Prefixes
	public final static String PREFIX_CIV = "https://iisg.amsterdam/id/civ/";
	public final static String PREFIX_SCHEMA = "http://schema.org/";
	public final static String PREFIX_SCHEMA_S = "https://schema.org/";
	public final static String PREFIX_BIO = "http://purl.org/vocab/bio/0.1/";
	public final static String PREFIX_DC = "http://purl.org/dc/terms/";
	public final static String PREFIX_DBO = "http://dbpedia.org/ontology/";

	// Types
	public final static String TYPE_BIRTH_EVENT = PREFIX_CIV + "Birth";
	public final static String TYPE_MARRIAGE_EVENT = PREFIX_CIV + "Marriage";
	public final static String TYPE_DEATH_EVENT = PREFIX_CIV + "Death";
	public final static String TYPE_DIVORCE_EVENT = PREFIX_CIV + "Divorce";
	public final static String TYPE_PERSON = PREFIX_SCHEMA + "Person";
	public final static String TYPE_PERSON_S = PREFIX_SCHEMA_S + "Person";
	public final static String TYPE_PLACE = PREFIX_SCHEMA + "Place";
	public final static String TYPE_PLACE_S = PREFIX_SCHEMA_S + "Place";
	public final static String TYPE_COUNTRY = PREFIX_CIV + "Country";
	public final static String TYPE_REGION = PREFIX_CIV + "Region";
	public final static String TYPE_PROVINCE = PREFIX_CIV + "Province";
	public final static String TYPE_MUNICIPALITY = PREFIX_CIV + "Municipality";

	// General Object Properties
	public final static String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public final static String OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";
	public final static String EVENT_LOCATION = PREFIX_CIV + "eventLocation";
	public final static String REGISTRATION_LOCATION = PREFIX_CIV + "registrationLocation";

	// Data properties Registrations & Events
	public final static String EVENT_DATE = PREFIX_CIV + "eventDate";
	public final static String REGISTRATION_DATE = PREFIX_CIV + "registrationDate";
	public final static String REGISTRATION_ID = PREFIX_CIV + "registrationID";
	public final static String REGISTRATION_SEQUENCE = PREFIX_CIV + "registrationSeqID";
	public final static String BIRTH_DATE_FLAG = PREFIX_CIV + "birthDateFlag";

	// Data properties Persons correct
	public final static String PERSON_ID = PREFIX_CIV + "personID";
	public final static String GIVEN_NAME = PREFIX_SCHEMA + "givenName";
	public final static String GIVEN_NAME_S = PREFIX_SCHEMA_S + "givenName";
	public final static String FAMILY_NAME = PREFIX_SCHEMA + "familyName";
	public final static String FAMILY_NAME_S = PREFIX_SCHEMA_S + "familyName";
	public final static String BIRTH_YEAR = PREFIX_DBO + "birthYear";
	public final static String AGE = PREFIX_CIV + "age";
	// public final static String AGE = PREFIX_IISG_VOCAB + "ageYears";

	// Gender
	public final static String GENDER = PREFIX_SCHEMA + "gender";
	public final static String GENDER_S = PREFIX_SCHEMA_S + "gender";
	public final static String GENDER_FEMALE_URI = PREFIX_SCHEMA + "Female";
	public final static String GENDER_FEMALE_URI_S = PREFIX_SCHEMA_S + "Female";
	public final static String GENDER_FEMALE_LITERAL = "\"f\"^^<http://www.w3.org/2001/XMLSchema#string>";
	public final static String GENDER_MALE_URI = PREFIX_SCHEMA + "Male";
	public final static String GENDER_MALE_URI_S = PREFIX_SCHEMA_S + "Male";
	public final static String GENDER_MALE_LITERAL = "\"m\"^^<http://www.w3.org/2001/XMLSchema#string>";


	// Roles correct
	public final static String ROLE_NEWBORN = PREFIX_CIV + "newborn";
	public final static String ROLE_MOTHER = PREFIX_CIV + "mother";
	public final static String ROLE_FATHER = PREFIX_CIV + "father";
	public final static String ROLE_DECEASED = PREFIX_CIV + "deceased";
	public final static String ROLE_PARTNER = PREFIX_CIV + "partner";
	public final static String ROLE_BRIDE = PREFIX_CIV + "bride";
	public final static String ROLE_BRIDE_MOTHER = PREFIX_CIV + "motherBride";
	public final static String ROLE_BRIDE_FATHER = PREFIX_CIV + "fatherBride";
	public final static String ROLE_GROOM = PREFIX_CIV + "groom";
	public final static String ROLE_GROOM_MOTHER = PREFIX_CIV + "motherGroom";
	public final static String ROLE_GROOM_FATHER = PREFIX_CIV + "fatherGroom";

	// Created Links
	public final static String LINK_IDENTICAL = OWL_SAMEAS;
	public final static String LINK_SIBLINGS = PREFIX_SCHEMA + "sibling";
	public final static String LINK_SPOUSE =  PREFIX_SCHEMA + "spouse";
	public final static String LINK_CHILDOF = PREFIX_SCHEMA + "children";
	public final static String LINK_PARENTOF = PREFIX_SCHEMA + "parent";


	// Named Graphs Metadata
	public final static String META_MATCHED_INVIDIUALS = PREFIX_CIV + "matchedIndividuals";
	public final static String META_LEVENSHTEIN = PREFIX_CIV + "levenshtein";
	public final static String META_GRAPH_TITLE = PREFIX_DC + "title";
	public final static String META_GRAPH_DESC = PREFIX_DC + "description";

	// SUB DIRECTORIES
	public final static String DIRECTORY_NAME_DICTIONARY = "dictionaries";
	public final static String DIRECTORY_NAME_DATABASE = "databases";
	public final static String DIRECTORY_NAME_RESULTS = "results";
}
