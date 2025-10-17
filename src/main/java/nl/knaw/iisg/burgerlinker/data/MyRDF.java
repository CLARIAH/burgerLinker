package nl.knaw.iisg.burgerlinker.data;


import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.QueryResults;

import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;
import nl.knaw.iisg.burgerlinker.utilities.FileUtilities;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class MyRDF {
    // TODO: make dynamic
    static String qPrefix = String.join("\n",
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
            "PREFIX civ: <https://iisg.amsterdam/id/civ/>",
            "PREFIX sdo: <https://schema.org/>"
            );
    public static String qInstanceCount = String.join("\n",
            qPrefix,
            "SELECT ?type (COUNT (?s) AS ?instanceCount)",
            "WHERE {",
            "?s rdf:type ?type . ",
            "} GROUP BY ?type");
    public static String qStatements = String.join("\n",
            qPrefix,
            "SELECT ?s ?p ?o ?sPersonID ?oPersonID ?eventDate",
            "WHERE {",
            "?s ?p ?o . ",
            "OPTIONAL { ?s civ:personID ?sPersonID } .",
            "OPTIONAL { ?o civ:personID ?oPersonID } .",
            "OPTIONAL { ?event ?q ?s; ",
            "                  civ:eventDate ?eventDate . }",
            "}");
     public static String qNewbornInfoFromEventURI = String.join("\n",
            qPrefix,
            "SELECT *",
            "WHERE {",
            "?event civ:newborn ?newbornID ;",
            "       civ:father ?fatherNewbornID ;",
            "       civ:mother ?motherNewbornID ;",
            "       civ:eventDate ?eventDate ;",
            "       civ:registrationID ?eventID .",
            "?newbornID sdo:givenName ?givenNameNewborn ;",
            "          sdo:familyName ?familyNameNewborn ;",
            "          sdo:gender ?genderNewborn .",
            "OPTIONAL { ?newbornID civ:age ?ageNewborn }",
            "OPTIONAL { ?newbornID civ:personID ?idNewborn }",
            "?fatherNewbornID sdo:givenName ?givenNameNewbornFather ;",
            "                 sdo:familyName ?familyNameNewbornFather ;",
            "                 sdo:gender ?genderNewbornFather .",
            "OPTIONAL { ?fatherNewbornID civ:age ?ageNewbornFather }",
            "OPTIONAL { ?fatherNewbornID civ:personID ?idNewbornFather }",
            "?motherNewbornID sdo:givenName ?givenNameNewbornMother ;",
            "                 sdo:familyName ?familyNameNewbornMother ;",
            "                 sdo:gender ?genderNewbornMother .",
            "OPTIONAL { ?motherNewbornID civ:age ?ageNewbornMother }",
            "OPTIONAL { ?motherNewbornID civ:personID ?idNewbornMother }",
            "}");
     public static String qDeceasedInfoFromEventURI = String.join("\n",
            qPrefix,
            "SELECT *",
            "WHERE {",
            "?event civ:deceased ?deceasedID ;",
            "       civ:partner ?partnerID ;",
            "       civ:father ?fatherDeceasedID ;",
            "       civ:mother ?motherDeceasedID ;",
            "       civ:eventDate ?eventDate ;",
            "       civ:registrationID ?eventID .",
            "?deceasedID sdo:givenName ?givenNameDeceased ;",
            "            sdo:familyName ?familyNameDeceased ;",
            "            sdo:gender ?genderDeceased .",
            "OPTIONAL { ?deceasedID civ:age ?ageDeceased }",
            "OPTIONAL { ?deceasedID civ:personID ?idDeceased }",
            "?partnerID sdo:givenName ?givenNamePartner ;",
            "           sdo:familyName ?familyNamePartner ;",
            "           sdo:gender ?genderPartner .",
            "OPTIONAL { ?partnerID civ:age ?agePartner }",
            "OPTIONAL { ?partnerID civ:personID ?idPartner }",
            "?fatherDeceasedID sdo:givenName ?givenNameDeceasedFather ;",
            "                  sdo:familyName ?familyNameDeceasedFather ;",
            "                  sdo:gender ?genderDeceasedFather .",
            "OPTIONAL { ?fatherDeceasedID civ:age ?ageDeceasedFather }",
            "OPTIONAL { ?fatherDeceasedID civ:personID ?idDeceasedFather }",
            "?motherDeceasedID sdo:givenName ?givenNameDeceasedMother ;",
            "                  sdo:familyName ?familyNameDeceasedMother ;",
            "                  sdo:gender ?genderDeceasedMother .",
            "OPTIONAL { ?motherDeceasedID civ:age ?ageDeceasedMother }",
            "OPTIONAL { ?motherDeceasedID civ:personID ?idDeceasedMother }",
            "}");
     public static String qMarriageInfoFromEventURI = String.join("\n",
            qPrefix,
            "SELECT *",
            "WHERE {",
            "?event civ:bride ?brideID ;",
            "       civ:groom ?groomID ;",
            "       civ:fatherBride ?fatherBrideID ;",
            "       civ:motherBride ?motherBrideID ;",
            "       civ:fatherGroom ?fatherGroomID ;",
            "       civ:motherGroom ?motherGroomID ;",
            "       civ:eventDate ?eventDate ;",
            "       civ:registrationID ?eventID .",
            "?brideID sdo:givenName ?givenNameBride ;",
            "         sdo:familyName ?familyNameBride ;",
            "         sdo:gender ?genderBride .",
            "OPTIONAL { ?brideID civ:age ?ageBride }",
            "OPTIONAL { ?brideID civ:personID ?idBride }",
            "?groomID sdo:givenName ?givenNameGroom ;",
            "         sdo:familyName ?familyNameGroom ;",
            "         sdo:gender ?genderGroom .",
            "OPTIONAL { ?groomID civ:age ?ageGroom }",
            "OPTIONAL { ?groomID civ:personID ?idGroom }",
            "?fatherBrideID sdo:givenName ?givenNameBrideFather ;",
            "               sdo:familyName ?familyNameBrideFather ;",
            "               sdo:gender ?genderBrideFather .",
            "OPTIONAL { ?fatherBrideID civ:age ?ageBrideFather }",
            "OPTIONAL { ?fatherBrideID civ:personID ?idBrideFather }",
            "?motherBrideID sdo:givenName ?givenNameBrideMother ;",
            "               sdo:familyName ?familyNameBrideMother ;",
            "               sdo:gender ?genderBrideMother .",
            "OPTIONAL { ?motherBrideID civ:age ?ageBrideMother }",
            "OPTIONAL { ?motherBrideID civ:personID ?idBrideMother }",
            "?fatherGroomID sdo:givenName ?givenNameGroomFather ;",
            "               sdo:familyName ?familyNameGroomFather ;",
            "               sdo:gender ?genderGroomFather .",
            "OPTIONAL { ?fatherGroomID civ:age ?ageGroomFather }",
            "OPTIONAL { ?fatherGroomID civ:personID ?idGroomFather }",
            "?motherGroomID sdo:givenName ?givenNameGroomMother ;",
            "               sdo:familyName ?familyNameGroomMother ;",
            "               sdo:gender ?genderGroomMother .",
            "OPTIONAL { ?motherGroomID civ:age ?ageGroomMother }",
            "OPTIONAL { ?motherGroomID civ:personID ?idGroomMother }",
            "}");

    // temporary on-disk triple store
    private Repository store;
    private RepositoryConnection conn;

    public static final Logger lg = LogManager.getLogger(MyRDF.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);
	FileUtilities FILE_UTILS = new FileUtilities();

    public MyRDF(File dataDir) {
        try{
            store = new SailRepository(new NativeStore(dataDir));
            conn = store.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void parse(String[] paths) {
        if (conn.isOpen()) {
            for (String path: paths) {
                ActivityIndicator spinner = new ActivityIndicator("Parsing Graph '" + path + "'");
                if (!FILE_UTILS.checkIfFileExists(path)) {
                    LOG.logError("parse", "Unable to find file '" + path + "'");
                    return;
                }

                // guess format from file name
                RDFFormat format = Rio.getParserFormatForFileName(path).orElse(null);
                if (format == null) {
                    LOG.logError("parse", "Unable to determine format of file '" + path +"'");
                    return;
                }

                spinner.start();
                try {
                    conn.add(new File(path), format);
                } catch (Exception e) {
                    LOG.logError("parse", "Error adding statements: " + e);
                }

                spinner.terminate();
                try {
                    spinner.join();
                } catch (Exception e) {
                    LOG.logError("parse", "Error waiting for ActivityIndicator to stop: " + e);
                }
            }
        } else {
            LOG.logError("parse", "Error connecting to RDF store");
        }
    }

    public List<BindingSet> getQueryResultsAsList(String query) {
        List<BindingSet> out = null;
        TupleQuery tupleQuery = conn.prepareTupleQuery(query);
        try (TupleQueryResult result = tupleQuery.evaluate()) {
            out = QueryResults.asList(result);
        }

        return out;
    }

    public List<BindingSet> getQueryResultsAsList(String query, Map<String, Value> bindings) {
        List<BindingSet> out = null;
        TupleQuery tupleQuery = conn.prepareTupleQuery(query);
        for (String key: bindings.keySet()) {
            Value value = bindings.get(key);
            tupleQuery.setBinding(key, value);
        }

        try (TupleQueryResult result = tupleQuery.evaluate()) {
            out = QueryResults.asList(result);
        }

        return out;
    }

    public TupleQueryResult getQueryResults(String query) {
        if (!conn.isOpen()) {
            LOG.logError("parse", "Error connecting to RDF store");

            return null;
        }
        TupleQuery tupleQuery = conn.prepareTupleQuery(query);

        return tupleQuery.evaluate();
    }

    public TupleQueryResult getQueryResults(String query, Map<String, Value> bindings) {
        if (!conn.isOpen()) {
            LOG.logError("parse", "Error connecting to RDF store");

            return null;
        }
        TupleQuery tupleQuery = conn.prepareTupleQuery(query);
        for (String key: bindings.keySet()) {
            Value value = bindings.get(key);
            tupleQuery.setBinding(key, value);
        }

        return tupleQuery.evaluate();
    }

    public RepositoryResult<Statement> getStatements() {
        return getStatements(null, null, null);
    }

    public RepositoryResult<Statement> getStatements(Resource s, IRI p, Value o) {
        // requires manual close
        if (!conn.isOpen()) {
            LOG.logError("parse", "Error connecting to RDF store");

            return null;
        }

        return conn.getStatements(s, p, o);
    }

    public long size() {
        long size = 0;
        if (!conn.isOpen()) {
            LOG.logError("parse", "Error connecting to RDF store");

            return size;
        }

        return conn.size();
    }

    public boolean close() {
        try {
            if (conn.isOpen()) {
                conn.close();
            }

            return true;
        } catch (Exception e) {
            LOG.logError("close", "Error when trying to close connection to RDF store");
        }

        return false;
    }

    public boolean shutdown() {
        try {
            if (close()) {
                store.shutDown();

                return true;
            }
        } catch (Exception e) {
            LOG.logError("close", "Error when trying to shutdown RDF store");
        }

        return false;
    }

    // ============ Utilities =============
    public int valueToInt(Value v) {
        int out = -1;
        if (v == null) {
            return out;
        }

        if (!v.isLiteral()) {
            LOG.logError("valueToInt", "Error - value is not a literal: '" + v.stringValue() + "'");
        }

        try {
            out = Integer.parseInt(v.stringValue());
        } catch (NumberFormatException e) {
            LOG.logError("valueToInt", "Error casting string to int: '" + v.stringValue() + "'");
        }

        return out;
    }

    public int yearFromDate(Value date) {
        // ISO 8601
        int out = -1;
        if (date != null && date.isLiteral()) {
            String[] dateArray = date.stringValue().split("-");  // YYYY-MM-DD
            if (dateArray.length == 3) {
                String yearStr = dateArray[0];
                try {
                    out = Integer.parseInt(yearStr);
                } catch (NumberFormatException e) {
                    LOG.logError("yearFromDate", "Error extracting year from literal: '" + yearStr + "'");
                }
            }
        }

        return out;
    }

    public static Literal mkLiteral(String value, String datatype) {
        return literal(value, iri(XSD.NAMESPACE, datatype));
    }

    public static String generalizeQuery(String q) {
        return generalizeQuery(q, "");
    }

    public static String generalizeQuery(String q, String gender) {
        if (q.contains("Newborn")) {
            q = q.replaceAll("(\\?[A-Za-z0-9]*)newborn", "$1subject");
            q = q.replaceAll("(\\?[A-Za-z0-9]*)Newborn", "$1Subject");
        } else if (q.contains("Deceased")) {
            q = q.replaceAll("(\\?[A-Za-z0-9]*)deceased", "$1subject");
            q = q.replaceAll("(\\?[A-Za-z0-9]*)Deceased", "$1Subject");
        } else if (q.contains("Bride")) {
            String subject, partner;
            if (gender == "f") {
                subject = "bride";
                partner = "groom";
            } else {
                subject = "groom";
                partner = "bride";
            }

            q = q.replaceAll("(\\?[A-Za-z0-9]*)" + subject, "$1subject");
            q = q.replaceAll("(\\?[A-Za-z0-9]*)" + subject.substring(0, 1).toUpperCase() + subject.substring(1), "$1Subject");
            q = q.replaceAll("(\\?[A-Za-z0-9]*)" + partner, "$1partner");
            q = q.replaceAll("(\\?[A-Za-z0-9]*)" + partner.substring(0, 1).toUpperCase() + partner.substring(1), "$1Partner");
        }

        return q;
    }
}
