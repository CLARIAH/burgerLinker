#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import math
import numpy as np
import glob
import collections
import re
from datetime import datetime


# change the value of the variable below to create IRIs with different dataset name
# Example:  https://iisg.amsterdam/id/<DATASET_NAME>/event/1

DATASET_NAME = "zeeland"

PREFIX_CIV_VOCAB = "https://iisg.amsterdam/id/civ/"
PREFIX_DATASET = "https://iisg.amsterdam/id/" + DATASET_NAME + "/"
PREFIX_SCHEMA = "http://schema.org/"
PREFIX_BIO = "http://purl.org/vocab/bio/0.1/"
PREFIX_DCT = "http://purl.org/dc/terms/"

TYPE_BIRTH = PREFIX_CIV_VOCAB + "Birth"
TYPE_MARRIAGE = PREFIX_CIV_VOCAB + "Marriage"
TYPE_DEATH = PREFIX_CIV_VOCAB + "Death"
TYPE_DIVORCE = PREFIX_CIV_VOCAB + "Divorce"
TYPE_UKNOWN = PREFIX_CIV_VOCAB + "Uknown"
TYPE_PERSON = PREFIX_SCHEMA + "Person"
TYPE_PLACE = PREFIX_SCHEMA + "Place"
TYPE_COUNTRY = PREFIX_CIV_VOCAB + "Country"
TYPE_REGION = PREFIX_CIV_VOCAB + "Region"
TYPE_PROVINCE = PREFIX_CIV_VOCAB + "Province"
TYPE_MUNICIPALITY = PREFIX_CIV_VOCAB + "Municipality"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"

EVENT_LOCATION = PREFIX_CIV_VOCAB + "eventLocation"
EVENT_DATE = PREFIX_CIV_VOCAB + "eventDate"
REGISTRATION_LOCATION = PREFIX_CIV_VOCAB + "registrationLocation"
REGISTRATION_DATE = PREFIX_CIV_VOCAB + "registrationDate"
REGISTRATION_ID = PREFIX_CIV_VOCAB + "registrationID"
REGISTRATION_SEQUENCE = PREFIX_CIV_VOCAB + "registrationSeqID"


BIRTH_DATE_FLAG = PREFIX_CIV_VOCAB + "birthDateFlag"

PERSON_ID = PREFIX_CIV_VOCAB + "personID"
GIVEN_NAME = PREFIX_SCHEMA + "givenName"
FAMILY_NAME = PREFIX_SCHEMA + "familyName"
GENDER = PREFIX_SCHEMA + "gender"
OCCUPATION = PREFIX_CIV_VOCAB + "occupationTitle"
FAMILY_NAME_PREFIX = PREFIX_CIV_VOCAB + "prefixFamilyName"
AGE = PREFIX_CIV_VOCAB + "age"

ROLE_NEWBORN = PREFIX_CIV_VOCAB + "newborn"
ROLE_MOTHER = PREFIX_CIV_VOCAB + "mother"
ROLE_FATHER = PREFIX_CIV_VOCAB + "father"
ROLE_DECEASED = PREFIX_CIV_VOCAB + "deceased"
ROLE_PARTNER = PREFIX_CIV_VOCAB + "partner"
ROLE_BRIDE = PREFIX_CIV_VOCAB + "bride"
ROLE_BRIDE_MOTHER = PREFIX_CIV_VOCAB + "motherBride"
ROLE_BRIDE_FATHER = PREFIX_CIV_VOCAB + "fatherBride"
ROLE_GROOM = PREFIX_CIV_VOCAB + "groom"
ROLE_GROOM_MOTHER = PREFIX_CIV_VOCAB + "motherGroom"
ROLE_GROOM_FATHER = PREFIX_CIV_VOCAB + "fatherGroom"
ROLE_UNKNOWN = PREFIX_CIV_VOCAB + "unknown"



def isNaN(num):
    if num == "\\N" or num == "":
        return True
    else:
        return False

def isNaN_Number(num):
    if num == 0:
        return True
    else:
        return False

def transformToInt(someNumber):
    return '"' + str(someNumber) + '"^^<http://www.w3.org/2001/XMLSchema#int>'

def transformToString(someString):
    return '"' + str(someString) + '"^^<http://www.w3.org/2001/XMLSchema#string>'

def transformToDate(someDate):
    return '"' + str(someDate) + '"^^<http://www.w3.org/2001/XMLSchema#date>'

def createQuad(s, p, o):
    return s + ' ' + p + ' ' + o + ' <https://druid.datalegend.net/LINKS/civ/graphs/' + DATASET_NAME + '> .\n'

def getRole(role_number, registrationID):
    if str(role_number) == '1':
        return ROLE_NEWBORN
    if str(role_number) == '2':
        return ROLE_MOTHER
    if str(role_number) == '3':
        return ROLE_FATHER
    if str(role_number) == '4':
        return ROLE_BRIDE
    if str(role_number) == '5':
        return ROLE_BRIDE_MOTHER
    if str(role_number) == '6':
        return ROLE_BRIDE_FATHER
    if str(role_number) == '7':
        return ROLE_GROOM
    if str(role_number) == '8':
        return ROLE_GROOM_MOTHER
    if str(role_number) == '9':
        return ROLE_GROOM_FATHER
    if str(role_number) == '10':
        return ROLE_DECEASED
    if str(role_number) == '11':
        return ROLE_PARTNER
    else:
        print("Role number:", role_number, "- Certificate ID:", registrationID)
        return "UNKNOWN"


def createEventURI(registrationID):
    return "<" + PREFIX_DATASET + "event/e-" + (str(registrationID)) + ">"

def createPersonURI(personID):
    return "<" + PREFIX_DATASET + "person/p-" + (str(personID)) + ">"

def createTripleRegistrationID(registrationURI, registrationID):
    p = "<" + PREFIX_CIV_VOCAB + "registrationID" + ">"
    o = transformToInt(registrationID)
    return createQuad(registrationURI,p,o)

def createRegistrationTypeURI(registrationType, registrationMainType):
    if registrationType == 'g':
        return "<" + TYPE_BIRTH + ">"
    if registrationType == 'h':
        return "<" + TYPE_MARRIAGE + ">"
    if registrationType == 'o':
        return "<" + TYPE_DEATH + ">"
    if registrationType == 's':
        return "<" + TYPE_DIVORCE + ">"
    else:
        if str(registrationMainType) == '1':
            return "<" + TYPE_BIRTH + ">"
        if str(registrationMainType) == '2':
            return "<" + TYPE_MARRIAGE + ">"
        if str(registrationMainType) == '3':
            return "<" + TYPE_DEATH + ">"
        else:
            return "<" + TYPE_UKNOWN + ">"

def createTripleRegistrationType(registrationURI, registrationType, registrationMainType):
    p = "<" + RDF_TYPE + ">"
    o = createRegistrationTypeURI(registrationType, registrationMainType)
    return createQuad(registrationURI, p, o)

def createDate(year, month, day):
    fixedYear = str(year).zfill(4)
    fixedMonth = str(month).zfill(2)
    fixedDay = str(day).zfill(2)
    return transformToDate(fixedYear + "-" + fixedMonth + "-" + fixedDay)

def createTripleRegistrationDate(registrationURI, fixedDate):
    p = "<" + REGISTRATION_DATE + ">"
    return createQuad(registrationURI, p , fixedDate)

def createTripleEventDate(registrationURI, fixedDate):
    p = "<" + EVENT_DATE + ">"
    return createQuad(registrationURI, p , fixedDate)

def createLocationURI(locationID):
    return "<" + PREFIX_DATASET + "place/l-" + str(locationID) + ">"

def createTripleRegistrationLocation(registrationURI, locationID):
    p = "<" + REGISTRATION_LOCATION + ">"
    o = createLocationURI(locationID)
    return createQuad(registrationURI,p ,o)

def createTripleEventLocation(registrationURI, locationID):
    p = "<" + EVENT_LOCATION + ">"
    o = createLocationURI(locationID)
    return createQuad(registrationURI,p ,o)

def createTripleRegistrationSeq(registrationURI, registrationSeqID):
    p = "<" + PREFIX_CIV_VOCAB + "registrationSeqID" + ">"
    o = transformToString(registrationSeqID)
    return createQuad(registrationURI, p, o)

def createTripleScanURL(registrationURI, scanURL):
    p = "<" + PREFIX_CIV_VOCAB + "scanURL" + ">"
    o = transformToString(scanURL)
    return createQuad(registrationURI, p, o)

def createTripleLinksBase(registrationURI, not_linksbase):
    p = "<" + PREFIX_CIV_VOCAB + "linksBase" + ">"
    o = transformToString(not_linksbase)
    return createQuad(registrationURI, p, o)

def createTriplesRegisterEvent(registrationURI, eventURI):
    p = "<" + REGISTER_EVENT + ">"
    return createQuad(registrationURI, p, eventURI)

def createTriplePersonType(personURI):
    p = "<" + RDF_TYPE + ">"
    o = "<" + TYPE_PERSON + ">"
    return createQuad(personURI, p, o)

def createTriplePersonID(personURI, personID):
    p = "<" + PREFIX_CIV_VOCAB + "personID" + ">"
    o = transformToInt(personID)
    return createQuad(personURI, p, o)

def createTripleGender(personURI, gender):
    p = "<" + GENDER + ">"
    o = transformToString(gender)
    return createQuad(personURI, p, o)

def createTripleGivenName(personURI, givenName):
    p = "<" + GIVEN_NAME + ">"
    o = transformToString(givenName)
    return createQuad(personURI, p, o)

def createTripleFamilyName(personURI, familyName):
    p = "<" + FAMILY_NAME + ">"
    o = transformToString(familyName)
    return createQuad(personURI, p, o)

def createTriplePrefix(personURI, prefix):
    p = "<" + FAMILY_NAME_PREFIX + ">"
    o = transformToString(prefix)
    return createQuad(personURI, p, o)

def createTripleOccupation(personURI, occupation):
    p = "<" + OCCUPATION + ">"
    o = transformToString(occupation)
    return createQuad(personURI, p, o)

def createTripleAge(personURI, age):
    p = "<" + AGE + ">"
    o = transformToInt(age)
    return createQuad(personURI, p, o)

def createTripleRole(eventURI, normalisedRole, personURI):
    p = "<" + normalisedRole + ">"
    return createQuad(eventURI, p, personURI)


def convertRegistrationsToRDF(inputData, outputData):
    start_time = datetime.now()
    f = open(outputData,"w+")
    ch_size = 10000
    df_chunk = pd.read_csv(inputData, chunksize=ch_size, sep=";", low_memory=False, keep_default_na=False)
    counter = 0
    for chunk in df_chunk:
        print("# " + str(counter) + " rows")
        counter = counter + ch_size
        filebuffer = []
        for index, row in chunk.iterrows():
            registrationID = row['id_registration']
            if not isNaN(registrationID):
                eventURI = createEventURI(registrationID)
                filebuffer.append(createTripleRegistrationID(eventURI, registrationID))
                registrationType = row['registration_type']
                registrationMainType = row['registration_maintype']
                if not isNaN(registrationType):
                    filebuffer.append(createTripleRegistrationType(eventURI, registrationType, registrationMainType))
                year = row['registration_year']
                month = row['registration_month']
                day = row['registration_day']
                if not isNaN_Number(year) and not isNaN_Number(month) and not isNaN_Number(day):
                    if not isNaN(year) and not isNaN(month) and not isNaN(day):
                        correctedDate = createDate(year, month, day)
                        filebuffer.append(createTripleRegistrationDate(eventURI, correctedDate))
                locationID = row['registration_location']
                if not isNaN(locationID):
                    filebuffer.append(createTripleRegistrationLocation(eventURI, locationID))
                registrationSeqID = row['registration_seq']
                if not isNaN(registrationSeqID):
                    filebuffer.append(createTripleRegistrationSeq(eventURI, registrationSeqID))
                scanURL = row['source_digital_original']
                if not isNaN(scanURL):
                    filebuffer.append(createTripleScanURL(eventURI, scanURL))
        f.writelines(filebuffer)
    f.close()
    print("Program Finished!")
    time_elapsed = datetime.now() - start_time
    print('Time elapsed (hh:mm:ss) {}'.format(time_elapsed))


# In[ ]:


def convertPersonsToRDF(inputData, outputData):
    start_time = datetime.now()
    f = open(outputData,"w+")
    ch_size = 10000
    df_chunk = pd.read_csv(inputData, chunksize=ch_size, sep=";", low_memory=False, error_bad_lines=False, keep_default_na=False, encoding = 'latin-1')
    counter = 0
    for chunk in df_chunk:
        print("# " + str(counter) + " rows")
        counter = counter + ch_size
        filebuffer = []
        for index, row in chunk.iterrows():
            personID = row['id_person']
            registrationID = row['id_registration']
            if not isNaN(personID) and not isNaN(registrationID):
                personURI = createPersonURI(personID)
                eventURI = createEventURI(registrationID)
                filebuffer.append(createTriplePersonType(personURI))
                filebuffer.append(createTriplePersonID(personURI, personID))
                givenName = row['firstname']
                if not isNaN(givenName):
                    filebuffer.append(createTripleGivenName(personURI, givenName))
                familyName = row['familyname']
                if not isNaN(familyName):
                    filebuffer.append(createTripleFamilyName(personURI, familyName))
                prefix = row['prefix']
                if not isNaN(prefix):
                    filebuffer.append(createTriplePrefix(personURI, prefix))
                gender = row['sex']
                if not isNaN(gender):
                    filebuffer.append(createTripleGender(personURI, gender))
                occupation = row['occupation']
                if not isNaN(occupation):
                    occupationFixed = re.sub('[^A-Za-z0-9 ]+', '', occupation)
                    filebuffer.append(createTripleOccupation(personURI, occupationFixed))
                age = row['age_year']
                if not isNaN(age):
                    filebuffer.append(createTripleAge(personURI, age))
                role = row['role']
                if not isNaN(role):
                    normalisedRole = getRole(role, registrationID)
                    if normalisedRole != "UNKNOWN":
                        filebuffer.append(createTripleRole(eventURI, normalisedRole, personURI))
                    main = False
                    if normalisedRole == ROLE_NEWBORN:
                        main = True
                        year = row['birth_year']
                        month = row['birth_month']
                        day = row['birth_day']
                        locationID = row['birth_location']
                    if normalisedRole == ROLE_BRIDE:
                        main = True
                        year = row['mar_year']
                        month = row['mar_month']
                        day = row['mar_day']
                        locationID = row['mar_location']
                    if normalisedRole == ROLE_DECEASED:
                        main = True
                        year = row['death_year']
                        month = row['death_month']
                        day = row['death_day']
                        locationID = row['death_location']
                    if main == True:
                        if not isNaN_Number(year) and not isNaN_Number(month) and not isNaN_Number(day):
                            if not isNaN(year) and not isNaN(month) and not isNaN(day):
                                fixedDate = createDate(year, month, day)
                                filebuffer.append(createTripleEventDate(eventURI, fixedDate))
                        if not isNaN(locationID):
                            filebuffer.append(createTripleEventLocation(eventURI, locationID))
        f.writelines(filebuffer)
    f.close()
    print("Program Finished!")
    time_elapsed = datetime.now() - start_time
    print('Time elapsed (hh:mm:ss) {}'.format(time_elapsed))

registrations_csv_path = "registrations.csv"
output_file_registrations = "registrations.nq"
convertRegistrationsToRDF(registrations_csv_path, output_file_registrations)
persons_csv_path = "persons.csv"
output_file_persons = "persons.nq"
convertPersonsToRDF(persons_csv_path, output_file_persons)

# using the terminal, you can later merge the resulting files using the following cat command:
# cat registrations.nq persons.nq > merged-dataset.nq
