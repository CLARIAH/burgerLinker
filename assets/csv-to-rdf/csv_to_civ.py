#!/usr/bin/env python

import argparse
from pathlib import Path

from rdf.formats import NTriples
from rdf.graph import Statement
from rdf.namespaces import RDF, XSD
from rdf.terms import Literal, IRIRef


CIV_NS = IRIRef("https://iisg.amsterdam/id/civ/")
SDO_NS = IRIRef("https://schema.org/")

ROLES = ["newborn", "mother", "father",
         "bride", "motherBride", "fatherBride",
         "groom", "motherGroom", "fatherGroom",
         "deceased", "partner"]


def getDate(line_array: list[str], header: list[str], role: str):
    pre = ""
    match role:
        case "newborn":
            pre = "birth_"
        case "bride":
            pre = "mar_"
        case "deceased":
            pre = "death_"

    year, month, day = None, None, None
    for i, v in enumerate(line_array):
        if v.startswith('"') and v.endswith('"'):
            v = v[1: -1]
        if v in ["NULL", "0", ""]:
            continue

        k = header[i]
        if k == pre + "year":
            year = v.zfill(4)

            continue
        if k == pre + "month":
            month = v.zfill(2)

            continue
        if k == pre + "day":
            day = v.zfill(2)

            continue

    if year is not None\
       and month is not None\
       and day is not None:
        return Literal("%s-%s-%s" % (year, month, day),
                       datatype=XSD+"date")

    return None


def convert(path_in: Path, base_ns: str, delim: str):
    events = set()
    header = None
    with open(path_in, "r") as f_in:
        header = [e.strip()[1: -1] for e in f_in.readline().split(delim)]
        for line in f_in:
            line_array = [e.strip() for e in line.split(delim)]

            person = None
            event = None
            for i, v in enumerate(line_array):
                if v.startswith('"') and v.endswith('"'):
                    v = v[1: -1]
                if v == "NULL" or len(v) <= 0:
                    continue

                k = header[i]
                match k:
                    case "id_registration":
                        event = base_ns + "event/e-" + v
                        if v in events:
                            continue

                        yield Statement(event, CIV_NS + "registrationID",
                                        Literal(v))

                        events.add(v)
                    case "id_person":
                        person = base_ns + "person/p-" + v

                        yield Statement(person, RDF + "type", SDO_NS + "Person")
                        yield Statement(person, CIV_NS + "personID", Literal(v))
                    case "firstname" if person is not None:
                        yield Statement(person, SDO_NS + "givenName",
                                        Literal(v, datatype=XSD+"string"))
                    case "familyname" if person is not None:
                        yield Statement(person, SDO_NS + "familyName",
                                        Literal(v, datatype=XSD+"string"))
                    case "prefix" if person is not None:
                        yield Statement(person, CIV_NS + "prefixFamilyName",
                                        Literal(v, datatype=XSD+"string"))
                    case "sex" if person is not None:
                        gender = None
                        if v.lower() in ["f", "female"]:
                            gender = SDO_NS + "Female"
                        elif v.lower() in ["m", "male"]:
                            gender = SDO_NS + "Male"

                        if gender is not None:
                            yield Statement(person, SDO_NS + "gender", gender)

                    case "age_year" if person is not None:
                        dtype = XSD + "nonNegativeInteger"
                        yield Statement(person, CIV_NS + "age",
                                        Literal(v, datatype=dtype))
                    case "role" if person is not None and event is not None:
                        try:
                            role = ROLES[int(v) - 1]
                        except Exception:
                            continue

                        yield Statement(event, CIV_NS + role, person)
                        if role == "newborn":
                            yield Statement(event, RDF + "type", CIV_NS + "Birth")
                        elif role == "deceased":
                            yield Statement(event, RDF + "type", CIV_NS + "Death")
                        elif role == "bride":
                            yield Statement(event, RDF + "type", CIV_NS + "Marriage")

                        date = getDate(line_array, header, role)
                        if date is not None:
                            yield Statement(event, CIV_NS + "eventDate", date)


def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, default=None,
                        help="CSV file with data about individuals")
    parser.add_argument("-d", "--delimiter", type=str, default=";",
                        help="Delimiter to use when processing lines")
    parser.add_argument("--namespace", type=str, default="http://example.org/",
                        help="Base namespace to define entities in")

    flags = parser.parse_args()
    if flags.input is not None:
        path_in = Path(flags.input)
        if path_in.exists():
            with NTriples(mode='w') as g:
                for triple in convert(path_in, IRIRef(flags.namespace),
                                      flags.delimiter):
                    g.write(triple)


if __name__ == "__main__":
    __main__()
