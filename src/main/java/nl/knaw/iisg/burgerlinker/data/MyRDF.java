package nl.knaw.iisg.burgerlinker.data;


import java.io.File;
import java.net.URL;
import java.time.LocalDate;
import java.util.Arrays;
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
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.explanation.Explanation;

import nl.knaw.iisg.burgerlinker.structs.Person;
import nl.knaw.iisg.burgerlinker.utilities.ActivityIndicator;
import nl.knaw.iisg.burgerlinker.utilities.FileUtilities;
import nl.knaw.iisg.burgerlinker.utilities.LoggingUtilities;


public class MyRDF {
    public static String QUERY_SUMMARY = String.join("\n",
        "SELECT ?type (COUNT (?s) AS ?instanceCount)",
        "WHERE {",
        "    ?s a ?type .",
        "} GROUP BY ?type");
    public static List<String> QUERY_VARS_EVENT = Arrays.asList(
            "event", "eventDate");
    public static List<String> QUERY_VARS_BIRTHS = Arrays.asList(
            "newborn", "newbornGivenName", "newbornFamilyName", "newbornGender", "newbornAge",
            "newbornFather", "newbornFatherGivenName", "newbornFatherFamilyName", "newbornFatherGender", "newbornFatherAge",
            "newbornMother", "newbornMotherGivenName", "newbornMotherFamilyName", "newbornMotherGender", "newbornMotherAge"
            );
    public static List<String> QUERY_VARS_DEATHS = Arrays.asList(
            "deceased", "deceasedGivenName", "deceasedFamilyName", "deceasedGender", "deceasedAge",
            "deceasedFather", "deceasedFatherGivenName", "deceasedFatherFamilyName", "deceasedFatherGender", "deceasedFatherAge",
            "deceasedMother", "deceasedMotherGivenName", "deceasedMotherFamilyName", "deceasedMotherGender", "deceasedMotherAge",
            "partner", "partnerGivenName", "partnerFamilyName", "partnerGender"
            );
    public static List<String> QUERY_VARS_MARRIAGES = Arrays.asList(
            "bride", "brideGivenName", "brideFamilyName", "brideGender", "brideAge",
            "brideFather", "brideFatherGivenName", "brideFatherFamilyName", "brideFatherGender", "brideFatherAge",
            "brideMother", "brideMotherGivenName", "brideMotherFamilyName", "brideMotherGender", "brideMotherAge",
            "groom", "groomGivenName", "groomFamilyName", "groomGender", "groomAge",
            "groomFather", "groomFatherGivenName", "groomFatherFamilyName", "groomFatherGender", "groomFatherAge",
            "groomMother", "groomMotherGivenName", "groomMotherFamilyName", "groomMotherGender", "groomMotherAge"
            );

    // temporary on-disk triple store
    private Repository store;
    private RepositoryConnection conn;
    private boolean debug = false;

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

    public MyRDF(String endpoint) {
        try{
            store = new SPARQLRepository(endpoint);
            conn = store.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean parse(List<String> paths) {
        boolean success = false;
        if (store != null && conn.isOpen()) {
            for (String path: paths) {
                ActivityIndicator spinner = new ActivityIndicator(".: Parsing Graph '" + path + "'");
                if (!FILE_UTILS.checkIfFileExists(path)) {
                    LOG.logError("parse", "Unable to find file '" + path + "'");
                    return success;
                }

                // guess format from file name
                RDFFormat format = Rio.getParserFormatForFileName(path).orElse(null);
                if (format == null) {
                    format = getParserFormatForFileName(path);
                    if (format == null) {
                        LOG.logError("parse", "Unable to determine format of file '" + path +"'");
                        return success;
                    }
                }

                spinner.start();
                try {
                    conn.add(new File(path), format);
                } catch (Exception e) {
                    LOG.logError("parse", "Error adding statements: " + e);
                    return success;
                } finally {
                    spinner.terminate();
                    try {
                        spinner.join();
                    } catch (Exception e) {
                        LOG.logError("parse", "Error waiting for ActivityIndicator to stop: " + e);
                        return success;
                    }
                }

            }
        } else {
            LOG.logError("parse", "Error connecting to RDF store");
            return success;
        }

        success = true;
        return success;
    }

    public RDFFormat getParserFormatForFileName(String filename) {
        RDFFormat format = null;
        if (filename != null) {
            int dotIndex = filename.lastIndexOf(".");
            if (dotIndex >= 0) {
                String suffix = filename.substring(dotIndex + 1).toLowerCase();
                if (suffix.length() > 0) {
                    switch (suffix) {
                        case "nt":
                            format = RDFFormat.NTRIPLES;

                            break;
                        case "ttl":
                            format = RDFFormat.TURTLE;

                            break;
                        case "nq":
                            format = RDFFormat.NQUADS;

                            break;
                        case "n3":
                            format = RDFFormat.N3;

                            break;
                        case "jsonld":
                            format = RDFFormat.JSONLD;

                            break;
                        case "hdt":
                            format = RDFFormat.HDT;

                            break;
                        case "trig":
                            format = RDFFormat.TRIG;

                            break;
                        case "trigs":
                            format = RDFFormat.TRIGSTAR;

                            break;
                        case "ttls":
                            format = RDFFormat.TURTLESTAR;

                            break;
                        case "rdf":
                        case "rdfs":
                        case "owl":
                        case "xml":
                            format = RDFFormat.RDFXML;

                            break;
                    }
                }
            }
        }

        return format;
    }

    public List<BindingSet> getQueryResultsAsList(String query) {
        List<BindingSet> out = null;
        TupleQuery tupleQuery = conn.prepareTupleQuery(query);

        if (debug) {
            String explanation = tupleQuery.explain(Explanation.Level.Timed).toString();
            LOG.outputConsole(explanation);
        }

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

        if (debug) {
            String explanation = tupleQuery.explain(Explanation.Level.Timed).toString();
            LOG.outputConsole(explanation);
        }

        try (TupleQueryResult result = tupleQuery.evaluate()) {
            out = QueryResults.asList(result);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.outputConsole("Query:\n" + query);
        }

        return out;
    }

    public TupleQueryResult getQueryResults(String query) {
        if (!conn.isOpen()) {
            LOG.logError("getQueryResults", "Error connecting to RDF store");

            return null;
        }

        TupleQuery tupleQuery = null;
        try {
            tupleQuery = conn.prepareTupleQuery(query);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.outputConsole("Query:\n" + query);
        }

        if (tupleQuery == null) {
            LOG.logError("getQueryResults", "Error executing query");

            return null;
        }

        if (debug) {
            String explanation = tupleQuery.explain(Explanation.Level.Timed).toString();
            LOG.outputConsole(explanation);
        }

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

        if (debug) {
            String explanation = tupleQuery.explain(Explanation.Level.Timed).toString();
            LOG.outputConsole(explanation);
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

//    public int yearFromDate(Value date) {
//        // ISO 8601
//        int out = -1;
//        if (date != null && date.isLiteral()) {
//            String[] dateArray = date.stringValue().split("-");  // YYYY-MM-DD
//            if (dateArray.length == 3) {
//                String yearStr = dateArray[0];
//                try {
//                    out = Integer.parseInt(yearStr);
//                } catch (NumberFormatException e) {
//                    LOG.logError("yearFromDate", "Error extracting year from literal: '" + yearStr + "'");
//                }
//            }
//        }
//
//        return out;
//    }

    public LocalDate valueToDate(Value v) {
        // ISO 8601
        LocalDate date = null;
        if (v != null && v.isLiteral()) {
            try {
                date = LocalDate.parse(v.stringValue());
            } catch (Exception e) {
                LOG.logError("valueToDate", "Error extracting year from literal: '" + v.stringValue() + "'");
            }
        }

        return date;
    }

    public static Literal mkLiteral(String value) {
        return mkLiteral(value, "string");
    }

    public static Literal mkLiteral(String value, String datatype) {
        return literal(value, iri(XSD.NAMESPACE, datatype));
    }

    public static IRI mkIRI(String value) {
        return iri(value);
    }

    public static String generalizeQuery(String q) {
        return generalizeQuery(q, null);
    }

    public static String generalizeQuery(String q, Person.Gender gender) {
        if (q.contains("?newborn")) {
            q = q.replaceAll("(\\?)newborn", "$1subject");
        } else if (q.contains("?deceased")) {
            q = q.replaceAll("(\\?)deceased", "$1subject");
        } else if (q.contains("?bride")) {
            String subject, partner;
            if (gender == Person.Gender.MALE) {
                subject = "groom";
                partner = "bride";
            } else {
                subject = "bride";
                partner = "groom";
            }

            q = q.replaceAll("(\\?)" + subject, "$1subject");
            q = q.replaceAll("(\\?)" + partner, "$1partner");
        }

        return q;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    // public void validate() {
    //     if (store == null) {
    //         return;
    //     }

    //     ShaclSail shaclSail = new ShaclSail();
    //     shaclSail.setBaseSail(((SailRepository) store).getSail());

    //     SailRepository sailRepository = new SailRepository(shaclSail);
    //     sailRepository.init();

    //     try (SailRepositoryConnection conn = sailRepository.getConnection()) {
    //         conn.begin();
    //         conn.clear(RDF4J.SHACL_SHAPE_GRAPH);
    //         conn.add(shaclRules, "", RDFFormat.TURTLE, RDF4J.SHACL_SHAPE_GRAPH);
    //         try {
    //             conn.commit();
    //         } catch (RepositoryException e) {
    //             Throwable cause = e.getCause();
    //             if (cause instanceof ValidationException) {
    //                 // use the validationReportModel to understand validation violations
    //                 Model validationReportModel = ((ValidationException) cause).validationReportAsModel();

    //                 // Pretty print the validation report
    //                 WriterConfig writerConfig = new WriterConfig()
    //                         .set(BasicWriterSettings.PRETTY_PRINT, true)
    //                         .set(BasicWriterSettings.INLINE_BLANK_NODES, true);

    //                 Rio.write(validationReportModel, System.out, RDFFormat.TURTLE, writerConfig);
    //                 System.out.println();
    //             }
    //         }
    //     } catch (RDFParseException | IOException e) {
    //         e.printStackTrace(psOut);
    //   } finally {
    //     conn.close();
    //     sailRepository.shutDown();
    //     shaclSail.shutDown();
    //   }
    // }
}
