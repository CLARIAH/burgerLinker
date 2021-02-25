package iisg.amsterdam.wp4_links;

public final class Properties {

	// Default Values
	public static String NAMESPACE = "https://iisg.amsterdam/";
	public static String DATASET_NAME = "links/";


	public static String getNamespace() {
		return NAMESPACE;
	}
	public static void setNamespace(String namespace) {
		NAMESPACE = namespace;
		if(!NAMESPACE.endsWith("/")) {
			NAMESPACE = NAMESPACE + "/";
		}
	}

	public static String getDatasetName() {
		return DATASET_NAME;
	}
	public static void setDatasetName(String datasetName) {
		DATASET_NAME = datasetName;
		if(!DATASET_NAME.endsWith("/")) {
			DATASET_NAME = DATASET_NAME + "/";
		}
	}


	// Prefixes
	public final static String PREFIX_IISG = NAMESPACE + DATASET_NAME;
	public final static String PREFIX_IISG_VOCAB = PREFIX_IISG + "vocab/";
	public final static String PREFIX_SCHEMA = "http://schema.org/";
	public final static String PREFIX_BIO = "http://purl.org/vocab/bio/0.1/";
	public final static String PREFIX_DC = "http://purl.org/dc/terms/";

	// Types
	public final static String TYPE_BIRTH_REGISTRATION = PREFIX_IISG_VOCAB + "BirthRegistration";
	public final static String TYPE_BIRTH_EVENT = PREFIX_BIO + "Birth";
	public final static String TYPE_MARRIAGE_REGISTRATION = PREFIX_IISG_VOCAB + "MarriageRegistration";
	public final static String TYPE_MARRIAGE_EVENT = PREFIX_BIO + "Marriage";
	public final static String TYPE_DEATH_REGISTRATION = PREFIX_IISG_VOCAB + "DeathRegistration";
	public final static String TYPE_DEATH_EVENT = PREFIX_BIO + "Death";
	public final static String TYPE_DIVORCE_REGISTRATION = PREFIX_IISG_VOCAB + "DivorceRegistration";
	public final static String TYPE_PERSON = PREFIX_SCHEMA + "Person";
	public final static String TYPE_PLACE = PREFIX_SCHEMA + "Place";
	public final static String TYPE_COUNTRY = PREFIX_IISG + "Country";
	public final static String TYPE_REGION = PREFIX_IISG + "Region";
	public final static String TYPE_PROVINCE = PREFIX_IISG + "Province";
	public final static String TYPE_MUNICIPALITY = PREFIX_IISG + "Municipality";

	// General Object Properties
	public final static String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public final static String OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";
	public final static String REGISTER_EVENT = PREFIX_IISG_VOCAB + "registerEvent";
	public final static String LOCATION = PREFIX_SCHEMA + "location";

	// Data properties Registrations & Events
	public final static String DATE = PREFIX_BIO + "date";
	public final static String REGISTRATION_ID = PREFIX_IISG_VOCAB + "registrationID";
	public final static String REGISTRATION_SEQUENCE = PREFIX_IISG_VOCAB + "registrationSeqID";
	public final static String BIRTH_DATE_FLAG = PREFIX_IISG_VOCAB + "birthDateFlag";

	// Data properties Persons correct
	public final static String PERSON_ID = PREFIX_IISG_VOCAB + "personID";
	public final static String GIVEN_NAME = PREFIX_SCHEMA + "givenName";
	public final static String FAMILY_NAME = PREFIX_SCHEMA + "familyName";
	
	// Gender
	public final static String GENDER = PREFIX_SCHEMA + "gender";
	public final static String GENDER_FEMALE_URI = PREFIX_SCHEMA + "Female";
	public final static String GENDER_FEMALE_LITERAL = "\"f\"^^<http://www.w3.org/2001/XMLSchema#string>";
	public final static String GENDER_MALE_URI = PREFIX_SCHEMA + "Male";
	public final static String GENDER_MALE_LITERAL = "\"m\"^^<http://www.w3.org/2001/XMLSchema#string>";
	

	// Roles correct 
	public final static String ROLE_NEWBORN = PREFIX_IISG_VOCAB + "newborn";
	public final static String ROLE_MOTHER = PREFIX_IISG_VOCAB + "mother";
	public final static String ROLE_FATHER = PREFIX_IISG_VOCAB + "father";
	public final static String ROLE_DECEASED = PREFIX_IISG_VOCAB + "deceased";
	public final static String ROLE_DECEASED_PARTNER = PREFIX_IISG_VOCAB + "deceasedPartner";
	public final static String ROLE_BRIDE = PREFIX_IISG_VOCAB + "bride";
	public final static String ROLE_BRIDE_MOTHER = PREFIX_IISG_VOCAB + "motherBride";
	public final static String ROLE_BRIDE_FATHER = PREFIX_IISG_VOCAB + "fatherBride";
	public final static String ROLE_GROOM = PREFIX_IISG_VOCAB + "groom";
	public final static String ROLE_GROOM_MOTHER = PREFIX_IISG_VOCAB + "motherGroom";
	public final static String ROLE_GROOM_FATHER = PREFIX_IISG_VOCAB + "fatherGroom";

	//	// Roles old
	//	public final static String ROLE_NEWBORN = PREFIX_IISG_VOCAB + "newborn";
	//	public final static String ROLE_MOTHER = PREFIX_IISG_VOCAB + "mother";
	//	public final static String ROLE_FATHER = PREFIX_IISG_VOCAB + "father";
	//	public final static String ROLE_DECEASED = PREFIX_IISG_VOCAB + "deceased";
	//	public final static String ROLE_DECEASED_PARTNER = PREFIX_IISG_VOCAB + "deceasedPartner";
	//	public final static String ROLE_BRIDE = "https://iisg.amsterdam/vocab/bride";
	//	public final static String ROLE_BRIDE_MOTHER = "https://iisg.amsterdam/vocab/bride_mother";
	//	public final static String ROLE_BRIDE_FATHER = "https://iisg.amsterdam/vocab/bride_father";
	//	public final static String ROLE_GROOM = "https://iisg.amsterdam/vocab/groom";
	//	public final static String ROLE_GROOM_MOTHER = "https://iisg.amsterdam/vocab/groom_mother";
	//	public final static String ROLE_GROOM_FATHER = "https://iisg.amsterdam/vocab/groom_father";

	// Created Links
	public final static String LINK_IDENTICAL = OWL_SAMEAS;
	public final static String LINK_SIBLINGS = PREFIX_SCHEMA + "sibling";
	public final static String LINK_SPOUSE =  PREFIX_SCHEMA + "spouse";
	public final static String LINK_CHILDOF = PREFIX_SCHEMA + "children";
	public final static String LINK_PARENTOF = PREFIX_SCHEMA + "parent";


	// Named Graphs Metadata
	public final static String META_MATCHED_INVIDIUALS = PREFIX_IISG_VOCAB + "matchedIndividuals";
	public final static String META_LEVENSHTEIN = PREFIX_IISG_VOCAB + "levenshtein";
	public final static String META_GRAPH_TITLE = PREFIX_DC + "title";
	public final static String META_GRAPH_DESC = PREFIX_DC + "description";

	// SUB DIRECTORIES
	public final static String DIRECTORY_NAME_DICTIONARY = "dictionaries";
	public final static String DIRECTORY_NAME_DATABASE = "databases";
	public final static String DIRECTORY_NAME_RESULTS = "results";


}
