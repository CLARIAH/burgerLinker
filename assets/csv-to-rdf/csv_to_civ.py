#!/usr/bin/env python

import argparse
import json
from pathlib import Path
import re

from rdf.formats import NTriples
from rdf.graph import Statement
from rdf.namespaces import RDF, XSD
from rdf.terms import Literal, IRIRef


CIV_NS = IRIRef("https://iisg.amsterdam/id/civ/")
SDO_NS = IRIRef("https://schema.org/")


def getElemOfLine(base_value: str, prop_name: str, mapping: dict):
    for prop, value in mapping.items():
        if prop == prop_name:
            return value

        if prop == "notes" and isinstance(value, list) and len(value) > 0:
            for note in value:
                if "type" not in note.keys() or note["type"] != "Annotation":
                    continue

                body = note.get("body")
                if body is None:
                    continue
                if not isinstance(body, dict) or "type" not in body.keys() or body["type"] != "Choice":
                    continue

                items = body.get("items")
                if items is None or not isinstance(items, list) or len(items) <= 0:
                    continue
                for item in items:
                    if "rdfs:label" in item.keys() and item["rdfs:label"] == base_value:
                        return getElemOfLine("", prop_name, item)

    return None


def translateValue(value: str, mapping: dict):
    notes = mapping.get("notes")
    if notes is not None and isinstance(notes, list) and len(notes) > 0:
        for note in notes:
            if "type" not in note.keys() or note["type"] != "Annotation":
                continue

            body = note.get("body")
            if body is None:
                continue
            if not isinstance(body, dict) or "type" not in body.keys() or body["type"] != "Choice":
                continue

            items = body.get("items")
            if items is None or not isinstance(items, list) or len(items) <= 0:
                continue
            for item in items:
                if "rdf:value" in item.keys() and item["rdf:value"] == value:
                    label = item.get("rdfs:label")
                    if label is not None:
                        return label

    return value


def modifyValue(value: str, name: str):
    if name == "eventYear":
        return value.zfill(4)
    if name in ["eventDay", "eventMonth"]:
        return value.zfill(2)

    return value


def expandVar(line: list[str], name: str, mapping: dict, index: dict[str, int]):
    for column_name_var in re.findall("{[0-9A-Za-z:._]+}", name):
        column_name = column_name_var[1:-1]

        if '.' in column_name:
            column_name_parts = column_name.split('.')
            assert len(column_name_parts) > 1

            base_name = column_name_parts[0]
            base_value = getValue(line, base_name, mapping, index)
            if base_value is None:
                continue

            replacement = "NULL"
            submap = mapping[base_name]
            for column_name_part in column_name_parts[1:]:
                replacement = getElemOfLine(base_value, column_name_part, submap)
                if isinstance(replacement, dict):
                    submap = replacement

                    continue

            name = name.replace(column_name_var, replacement)

            continue

        value_part = getValue(line, column_name, mapping, index)
        if value_part is not None:
            name = name.replace(column_name_var, value_part)

    return name


def getValueFromLine(line: list[str], column_name: str, index: dict[str, int]):
    if column_name is not None and column_name in index.keys():
        column_idx = index[column_name]
        if len(line) > column_idx:
            return line[column_idx]

    return None


def getValue(line: list[str], name: str, mapping: dict,
             index: dict[str, int]):
    if name in mapping.keys():
        nmap = mapping[name]
        if "virtual" in nmap.keys() and nmap["virtual"]:
            # virtual column
            value = nmap.get("value")
            if value is not None:
                if '{' in value and '}' in value:
                    value = expandVar(line, value, mapping, index)

                value = translateValue(value, nmap)
                if value == "NULL":
                    return None

                return modifyValue(value, name)

        column_name = nmap.get("titles")
        if column_name is not None:
            if '{' in column_name and '}' in column_name:
                column_name = expandVar(line, column_name, mapping, index)

            value = getValueFromLine(line, column_name, index)
            if value == "NULL":
                return None
            if value is not None:
                value = translateValue(value, nmap)

            return modifyValue(value, name)

    return None


def prepLine(line: str, delim: str):
    line_array = list()
    for e in line.split(delim):
        e = e.strip()
        if e.startswith('"') and e.endswith('"'):
            e = e[1: -1]

        if len(e) <= 0:
            e = "NULL"

        line_array.append(e)

    return line_array


def convert(path_in: Path, mapping: dict, base_ns: str, delim: str):
    header = None
    with open(path_in, "r") as f_in:
        header = prepLine(f_in.readline(), delim)
        header_to_idx = {v: i for i, v in enumerate(header)}
        for line in f_in:
            line_array = prepLine(line, delim)

            person = None
            person_value = getValue(line_array, "subjectID", mapping, header_to_idx)
            if person_value is not None:
                person = base_ns + "person/p-" + person_value
                yield Statement(person, RDF + "type", SDO_NS + "Person")

            if person is None:
                continue

            # person name
            first_name = getValue(line_array, "subjectFirstName", mapping, header_to_idx)
            if first_name is not None:
                yield Statement(person, SDO_NS + "givenName", Literal(first_name, datatype=XSD+"string"))

            surname_prefix = getValue(line_array, "subjectFamilyNamePrefix", mapping, header_to_idx)
            if surname_prefix is not None:
                yield Statement(person, CIV_NS + "prefixFamilyName", Literal(surname_prefix, datatype=XSD+"string"))

            family_name = getValue(line_array, "subjectFamilyName", mapping, header_to_idx)
            if family_name is not None:
                fullname = family_name if surname_prefix is None else surname_prefix + " " + family_name
                yield Statement(person, SDO_NS + "familyName", Literal(fullname, datatype=XSD+"string"))

            if first_name is not None and family_name is not None:
                if surname_prefix is not None:
                    name = " ".join([first_name, surname_prefix, family_name])
                else:
                    name = first_name + " " + family_name
                yield Statement(person, SDO_NS + "name", Literal(name, datatype=XSD+"string"))

            # person gender
            gender_value = getValue(line_array, "subjectGender", mapping, header_to_idx)
            if gender_value is not None:
                gender = None
                if gender_value.lower() == "female":
                    gender = SDO_NS + "Female"
                elif gender_value.lower() == "male":
                    gender = SDO_NS + "Male"

                if gender is not None:
                    yield Statement(person, SDO_NS + "gender", gender)

            # person age
            age = getValue(line_array, "subjectAge", mapping, header_to_idx)
            if age is not None:
                dtype = XSD + "nonNegativeInteger"
                yield Statement(person, CIV_NS + "age", Literal(age, datatype=dtype))

            # event
            event = None
            event_value = getValue(line_array, "eventID", mapping, header_to_idx)
            if event_value is not None:
                event = base_ns + "event/e-" + event_value

            if event is None:
                continue

            role = getValue(line_array, "subjectRole", mapping, header_to_idx)
            if role is not None:
                yield Statement(event, CIV_NS + role, person)

                if role in ["newborn", "deceased", "bride"]:
                    if role == "newborn":
                        yield Statement(event, RDF + "type", CIV_NS + "Birth")
                    elif role == "deceased":
                        yield Statement(event, RDF + "type", CIV_NS + "Death")
                    elif role == "bride":
                        yield Statement(event, RDF + "type", CIV_NS + "Marriage")

                    date = getValue(line_array, "eventDate", mapping, header_to_idx)
                    if date is not None:
                        yield Statement(event, CIV_NS + "eventDate", Literal(date, datatype=XSD+"date"))


def mkMapping(config: dict):
    mapping = dict()
    if "tableSchema" in config.keys()\
            and "columns" in config["tableSchema"].keys():
        for column in config["tableSchema"]["columns"]:
            if "name" not in column.keys():
                continue

            name = column["name"]
            if name not in mapping.keys():
                mapping[name] = column

    return mapping


def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, default=None,
                        help="CSV file with data about individuals")
    parser.add_argument("-c", "--config", type=str, default=None, required=True,
                        help="A CSV to RDF mapping in CSVW metadata format")
    parser.add_argument("-d", "--delimiter", type=str, default=";",
                        help="Delimiter to use when processing lines")
    parser.add_argument("--namespace", type=str, default="http://example.org/",
                        help="Base namespace to define entities in")

    flags = parser.parse_args()
    if flags.input is not None:
        path_in = Path(flags.input)
        if path_in.exists():
            config = json.load(open(flags.config, 'r'))
            if "url" not in config.keys() or config["url"] != path_in.name:
                return

            tmap = mkMapping(config)
            with NTriples(mode='w') as g:
                for triple in convert(path_in, tmap, IRIRef(flags.namespace),
                                      flags.delimiter):
                    g.write(triple)


if __name__ == "__main__":
    __main__()
