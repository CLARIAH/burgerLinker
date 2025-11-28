# CSV to RDF Conversion

BurgerLinker is designed to operate on data that conforms to the *Resource Description Framework* (RDF). This form of data is also called *Linked Data*. The scripts in this repository facilitate the conversion of civil registry data from CSV format to RDF. A mapping file is used to tailor the conversion to a dataset.

## Prerequisites

The conversion scripts require a working [Python](https://www.python.org/) installation and the latest version of the [pyRDF](https://gitlab.com/wxwilcke/pyRDF) library. The *pyRDF* library can be installed using [PIP](https://pip.pypa.io/en/stable/), the package installer for Python. Instructions on how to do so are given next. These instructions assume that the system has a working and updated [Python](https://www.python.org/) installation. *It is strongly recommended to first set up a clean [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) before continuing with the installation (see next header).*

1) To install *pyRDF*:

    $ python -m pip install pyRDF

### Python Virtual Environment

A [Python virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments) provides a clean and isolated environment that minimizes issues with dependencies and other packages. It is strongly recommended to run these scripts from a virtual environment. Instructions on how to set up such an environment are given next.

1) Create a Python virtual environment in the current directory. Replace `<name>` by the name you want this environment to be called.

        $ python -m venv <name>

2) Activate the environment. This step has to be repeated each time you want to run the scripts.

    a) for Windows

        $ <name>\Scripts\activate

    b) for Mac / Linux

        $ source <name>/bin/activate

The Python virtual environment is now set up and activated. Any calls to Python or PIP will now use this environment. The scripts can now be run from the activated virtual environment by executing the three steps listed above.

3) Deactivate the environment.

    a) for Windows

        $ <name>\Scripts\deactivate

    b) for Mac / Linux

        $ source <name>/bin/deactivate

## Usage


```
csv_to_*.py [-h] -c CONFIG [-d DELIMITER] [--namespace NAMESPACE] input

positional arguments:
  input                 CSV file with data about individuals

options:
  -h, --help            show this help message and exit
  -c, --config CONFIG   A CSV to RDF mapping in CSVW metadata format
  -d, --delimiter DELIMITER
                        Delimiter to use when processing lines
  --namespace NAMESPACE
                        Base namespace to define entities in
```

The conversion relies on an accurate mapping file (`CONFIG`) to relate table headers to RDF elements. Instructions on how to create a mapping for your data are given below. A pre-made mapping has already been provided for *LINKS* data.

### Examples

Convert a LINKS dataset to PiCo:

    $ python csv_to_pico.py --config LINKS.json --namespace "https://iisg.knaw.nl/links/" data/persons.csv > data/persons-pico.nt

Convert a custom dataset to CIV:

    $ python csv_to_civ.py --config MyMapping.json --delimiter "," data/persons.csv > data/persons-civ.nt

## Mapping

The mapping file must be a valid JSON file that conforms to the *CSV on the Web* (CSVW) standard. Only a minimal subset of the standard is supported by the conversion scripts. For more advanced conversion needs, please consider *CoW*.

A valid skeleton mapping file might look like this:

```json
{
  "@context": "http://www.w3.org/ns/csvw",
  "dc:title": "CSV to RDF header map",
  "dc:description": "A CSVW metadata specification that defines a mapping between the headers of a CSV file and the necessary variables in a BurgerLinker conversion script.",
  "dc:creator": "...",
  "dc:date": "...",
  "url": "persons.csv",
  "tableSchema": {
    "columns": [{
        "titles": "id_person",
        "name": "subjectID"
    }]
  }
}
```

The mapping above is defined for file *persons.csv* (as specified with `"url:"`). If this file is not found the conversion will fail.

Exactly one column map has been defined: column *id_person* corresponds to *subjectID*. When looking for *subjectID* the scripts will now know to look for the column *id_person*.

To complete the mapping, add all necessary mappings. The complete set of keys are:

```
- eventID
- eventDate
- subjectID
- subjectFirstName
- subjectFamilyName
- subjectFamilyNamePrefix
- subjectGender
- subjectAge
- subjectRole
```

### Translation

Values in a column can be translated into appropriate values using *Web Annotations*. These must be added to a column map using the "notes" key, and must specify a body of type `Choice` which lists all necessary translations. Each translation must provide the expected value (`rdf:value`) and desired translation (`rdfs:label`). An example is given below: 

```json
{
  "titles": "sex",
  "name": "subjectGender",
  "notes": [{
      "type": "Annotation",
      "body": {
          "type": "Choice",
          "items": [{
              "rdf:value": "f",
              "rdfs:label": "female"
          },{
              "rdf:value": "m",
              "rdfs:label": "male"
          }]
      }
  }]
}
```

### Variables

Variables take the form *"{...}"* and can be used to dynamically assign names or values. The variable name must equal one of the supported mapping keys. An attribute of that element can be accessed by appending a dot ('.') followed by the property. An example is given below:

```json
{
  "titles": "{subjectRole.sdo:alternateName}_year",
  "name": "eventYear"
},{
  "titles": "{subjectRole.sdo:alternateName}_month",
  "name": "eventMonth"
},{
  "titles": "{subjectRole.sdo:alternateName}_day",
  "name": "eventDay"
},{
  "value": "{eventYear}-{eventMonth}-{eventDay}",
  "virtual": true,
  "name": "eventDate"
}
```

In the snippet above, `eventDate` is a virtual column, which means that the column does not actually exist in the CSV file. Instead, the value is computed by retrieving the `eventYear`, `eventMonth`, and `eventDay` keys. Rather than retrieving the actual values, the conversion script will return the value from their corresponding `sdo:alternateName` properties:

```json
"titles": "role",
"name": "subjectRole",
"notes": [{
  "type": "Annotation",
  "body": {
      "type": "Choice",
      "items": [{
          "rdf:value": "1",
          "rdfs:label": "newborn",
          "sdo:alternateName": "birth"
      }, ...
      }]
}]
```

