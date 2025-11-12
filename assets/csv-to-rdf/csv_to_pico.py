#!/usr/bin/env python

import argparse
from pathlib import Path
import uuid

from rdf.formats import NTriples
from rdf.graph import Statement
from rdf.namespaces import RDF, XSD
from rdf.terms import BNode, Literal, IRIRef


PICOM_NS = IRIRef("https://personsincontext.org/model#")
PICOT_NS = IRIRef("https://terms.personsincontext.org/")
PNV_NS = IRIRef("https://w3id.org/pnv#")
PROV_NS = IRIRef("http://www.w3.org/ns/prov#")
SDO_NS = IRIRef("https://schema.org/")

ROLES = ["newborn", "mother", "father",
         "bride", "motherBride", "fatherBride",
         "groom", "motherGroom", "fatherGroom",
         "deceased", "partner"]


def getDate(line_array: list[str], header: list[str], role: str):
    pre = ""
    match role:
        case "newborn":  # newborn
            pre = "birth_"
        case "bride":  # bride/groom
            pre = "mar_"
        case "deceased":  # deceased
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


def getCert(role: str):
    cert = None
    match role:
        case "newborn":
            cert = PICOT_NS + "sourcetypes/551"
        case "bride":
            cert = PICOT_NS + "sourcetypes/552"
        case "deceased":
            cert = PICOT_NS + "sourcetypes/553"

    return cert


def getRole(role: str):
    roleIRI = None
    match role:
        case "newborn":
            roleIRI = PICOT_NS + "roles/575"
        case "partner" | "bride" | "groom":
            roleIRI = PICOT_NS + "roles/574"
        case "deceased":
            roleIRI = PICOT_NS + "roles/479"

    return roleIRI


def convert(path_in: Path, base_ns: str, delim: str):
    events = dict()
    header = None
    with open(path_in, "r") as f_in:
        header = [e.strip()[1: -1] for e in f_in.readline().split(delim)]
        for line in f_in:
            line_array = [e.strip() for e in line.split(delim)]

            person = None
            event = None
            name = None
            surname_prefix = None
            for i, v in enumerate(line_array):
                if v.startswith('"') and v.endswith('"'):
                    v = v[1: -1]
                if v == "NULL" or len(v) <= 0:
                    continue

                k = header[i]
                match k:
                    case "id_person":
                        person = base_ns + "person/p-" + v
                        yield Statement(person, RDF + "type", PICOM_NS + "PersonObservation")
                        yield Statement(person, SDO_NS + "identifier", Literal(v))

                        # PNV
                        name = BNode(uuid.uuid4().hex)
                        yield Statement(person, SDO_NS + "additionalName", name)
                        yield Statement(name, RDF + "type", PNV_NS + "PersonName")
                    case "firstname" if name is not None:
                        yield Statement(person, SDO_NS + "givenName", Literal(v, datatype=XSD+"string"))
                        yield Statement(name, PNV_NS + "givenName", Literal(v, datatype=XSD+"string"))
                    case "prefix" if name is not None:
                        surname_prefix = v
                        yield Statement(name, PNV_NS + "surnamePrefix", Literal(v, datatype=XSD+"string"))
                    case "familyname" if name is not None:
                        fullname = v if surname_prefix is None\
                                else surname_prefix + " " + v
                        yield Statement(person, SDO_NS + "familyName", Literal(fullname, datatype=XSD+"string"))

                        yield Statement(name, PNV_NS + "baseSurname", Literal(v, datatype=XSD+"string"))
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
                        yield Statement(person, PICOM_NS + "hasAge", Literal(v, datatype=dtype))
                    case "id_registration" if person is not None:
                        event = base_ns + "event/e-" + v
                        if event in events.keys():
                            yield Statement(person, PROV_NS + "hadPrimarySource", event)

                            continue

                        yield Statement(event, RDF + "type", SDO_NS + "ArchiveComponent")
                        yield Statement(event, SDO_NS + "identifier", Literal(v))
                        yield Statement(person, PROV_NS + "hadPrimarySource", event)

                        events[event] = dict()
                    case "role" if person is not None and event is not None:
                        try:
                            role = ROLES[int(v)-1]
                            events[event][role] = person
                        except Exception:
                            continue

                        roleIRI = getRole(role)
                        if roleIRI is not None:
                            yield Statement(person, PICOM_NS + "hasRole", roleIRI)

                            if role in ["newborn", "deceased", "bride"]:
                                cert = getCert(role)
                                if cert is not None:
                                    yield Statement(event, SDO_NS + "additionalType", cert)

                                date = getDate(line_array, header, role)
                                if date is not None:
                                    yield Statement(event, SDO_NS + "dateCreated", date)

    for triple in link_events(events):
        yield triple


def link_events(events):
    for event in events.values():
        if "newborn" in event.keys() or "deceased" in event.keys():
            subject = event["newborn"] if "newborn" in event.keys()\
                    else event["deceased"]

            mother, father = None, None
            if "mother" in event.keys():
                mother = event["mother"]

                yield Statement(subject, SDO_NS + "parent", mother)
                yield Statement(mother, SDO_NS + "children", subject)
            if "father" in event.keys():
                father = event["father"]

                yield Statement(subject, SDO_NS + "parent", father)
                yield Statement(father, SDO_NS + "children", subject)

            if mother is not None and father is not None:
                yield Statement(mother, SDO_NS + "spouse", father)
                yield Statement(father, SDO_NS + "spouse", mother)

            if "partner" in event.keys():
                partner = event["partner"]

                yield Statement(subject, SDO_NS + "spouse", partner)
                yield Statement(partner, SDO_NS + "spouse", subject)

            continue

        bride = None if "bride" not in event.keys() else event["bride"]
        groom = None if "groom" not in event.keys() else event["groom"]
        if bride is not None:
            motherBride, fatherBride = None, None
            if "motherBride" in event.keys():
                motherBride = event["motherBride"]

                yield Statement(bride, SDO_NS + "parent", motherBride)
            if "fatherBride" in event.keys():
                fatherBride = event["fatherBride"]

                yield Statement(bride, SDO_NS + "parent", fatherBride)

            if motherBride is not None and fatherBride is not None:
                yield Statement(motherBride, SDO_NS + "spouse", fatherBride)
                yield Statement(fatherBride, SDO_NS + "spouse", motherBride)

        if groom is not None:
            motherGroom, fatherGroom = None, None
            if "motherGroom" in event.keys():
                motherGroom = event["motherGroom"]

                yield Statement(groom, SDO_NS + "parent", motherGroom)
            if "fatherGroom" in event.keys():
                fatherGroom = event["fatherGroom"]

                yield Statement(groom, SDO_NS + "parent", fatherGroom)

            if motherGroom is not None and fatherGroom is not None:
                yield Statement(motherGroom, SDO_NS + "spouse", fatherGroom)
                yield Statement(fatherGroom, SDO_NS + "spouse", motherGroom)

        if bride is not None and groom is not None:
            yield Statement(bride, SDO_NS + "spouse", groom)
            yield Statement(groom, SDO_NS + "spouse", bride)


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
