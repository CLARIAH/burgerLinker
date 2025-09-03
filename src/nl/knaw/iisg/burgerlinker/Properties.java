package nl.knaw.iisg.burgerlinker;

public final class Properties {
	// Output directories
	public final static String DIRECTORY_NAME_DICTIONARY = "dictionaries";
	public final static String DIRECTORY_NAME_DATABASE = "databases";
	public final static String DIRECTORY_NAME_RESULTS = "results";

	// Output Prefixes
	public final static String PREFIX_CIV = "https://iisg.amsterdam/id/civ/";
	public final static String PREFIX_SCHEMA = "http://schema.org/";
	public final static String PREFIX_DC = "http://purl.org/dc/terms/";
	public final static String PREFIX_DBO = "http://dbpedia.org/ontology/";

	// Output properties
	public final static String BIRTH_YEAR = PREFIX_DBO + "birthYear";

	public final static String OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";

	public final static String LINK_IDENTICAL = OWL_SAMEAS;
	public final static String LINK_SIBLINGS = PREFIX_SCHEMA + "sibling";
	public final static String LINK_SPOUSE =  PREFIX_SCHEMA + "spouse";
	public final static String LINK_CHILDOF = PREFIX_SCHEMA + "children";
	public final static String LINK_PARENTOF = PREFIX_SCHEMA + "parent";

	// Output Graph Metadata
	public final static String META_MATCHED_INVIDIUALS = PREFIX_CIV + "matchedIndividuals";
	public final static String META_LEVENSHTEIN = PREFIX_CIV + "levenshtein";
	public final static String META_GRAPH_TITLE = PREFIX_DC + "title";
	public final static String META_GRAPH_DESC = PREFIX_DC + "description";
}
