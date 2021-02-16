## **burgerLinker -** Civil Registries Linking Tool

### Purpose
This tool is being developed to improve and replace the current [LINKS](https://iisg.amsterdam/en/hsn/projects/links) software. Points of improvement are:
- extremely fast and scalable matching procedure (using Levenshtein automaton and HDT);
- considers all first names of the individuals with multiple first names in order to find a candidate match;
- blocking is not required (i.e. all candidate records can be considered for matching, with no restrictions on their registration date or location, and no requirements on blocking parts of their individual names);
- detected links contains detailed provenance metadata, and can be saved in different formats (CSV and RDF are covered in the current version);
- allows family and life course reconstruction (by computing the transitive closure over all detected links);
- open software

### Use case
Historians use archival records to describe persons' lives. Each record (e.g. a marriage record) just describes a point in time. Hence historians try to link multiple records on the same person to describe a life course. This tool focuses on "just" the linkage of civil records. By doing so, pedigrees of humans can be created over multiple generations for research on social inequality, especially in the part of health sciences where the focus is on gene-social contact interactions.

### User profile
The software is designed for the so called "digital historians" (e.g. someone with basic command line skills) who are interested in using the Dutch civil registries for their studies or linking their data to it.

### Data
In its current version, the tool cannot be used to match entities from just any source. The current tool is solely focused on the linkage of civil records, relying on the sanguineous relations on the civil record, modelled according to our [Civil Registries schema](assets/civil-registries-schema.ttl). An overview of the Civil Registries schema is available as a [PNG file](assets/civil-registries-schema.png).

### Previous work
So far, (Dutch) civil records have been linked by bespoke programming by researchers, sometimes supported by engineers. Specifically the IISG-LINKS program has a pipeline to link these records and provide them to the Central Bureau of Genealogy (CBG). Because the number of records has grown over time and the IISG-LINKS takes an enormous amount of time (weeks) to LINK all records currently present, *burgerLinker* is designed to do this much faster (full sample takes less than 48 hours).

The Golden Agents project has brought about [Lenticular Lenses](https://www.goldenagents.org/tools/lenticular-lenses/) a tool designed to link persons across sources of various nature. We have engaged with the Lenticular Lenses team on multiple occasions (a demo-presentation, two person-vocabulary workshops, and a specific between-teams-workshop). From those meetings we have adopted the [ROAR vocabulary](https://leonvanwissen.nl/vocab/roar/docs/) for work in CLARIAH-WP4. On the specific *burgerLinker* and lenticular lenses tool, however we found that the prerequisite in Lenticular Lenses to allow for heterogenous sources, conflicted with the *burgerLinker* prerequisite to be fast: one reason for it to be fast is the limited set of sources that *burgerLinker* allows for.

The only other set of initiatives that we are aware of are bespoke programming initiatives by domain specific researchers, with country and time specific rules for linking in for example R. These linkage tools are on the whole slow. What we did do is make our own rule set for linking modular, to allow in the future for country and time specific rule sets to be incorporated in *burgerLinker*.

---

### Installation requirements
- Only the [JAVA Runtime Environment (JRE)](https://www.oracle.com/java/technologies/javase-jre8-downloads.html), which is free and installed on almost every computer these days.

### Input requirements
- Only one RDF dataset, describing the civil registries that are modelled according to our simple [Civil Registries schema](assets/LINKS-schema.png), based on Schema.org and BIO vocabularies. You can browse the [RDF file](assets/civil-registries-schema.ttl) in any triple store (e.g. [Druid](https://druid.datalegend.net/)) or ontology editor (e.g. [Protégé](https://protege.stanford.edu/)).

For efficient querying (i.e. lower memory usage with fast search), the matching tool requires the dataset to be compressed and given as an HDT file with its index ([What is HDT?](http://www.rdfhdt.org/what-is-hdt/)).

The tool allows the conversion of an RDF file (any serialisation) to HDT using the `--function convertToHDT`.

### Output format
Two possible output formats to represent the detected links:
- CSV file (default if no output format is specified by the user)
- N-QUADS file (it can be specified in the parameters of the tool using `--format RDF`)

### Main dependencies
The code of this tool make use of two main libraries:
- [Levenshtein automata](https://github.com/universal-automata/liblevenshtein-java) (MIT License)
- [RDF-HDT](https://github.com/rdfhdt/hdt-java) (LGPL License)

### Tool functionalities

Functionalities that are supported in the current version: (case insensitive)

- `ConvertToHDT`: compress an RDF dataset given as input to an HDT file that will be generated in the same directory. This function can also be used for merging two HDT files into one (see Example 3 below)

- `ShowDatasetStats`: display some general stats about the HDT dataset, given as input.

- `Within_B_M`: link *newborns* in Birth Certificates to *brides/grooms* in Marriage Certificates (reconstructs life course)

- `Between_B_M`: link *parents of newborns* in Birth Certificates to *brides & grooms* in Marriage Certificates (reconstructs family ties)

- `Between_M_M`: link *parents of brides/grooms* in Marriage Certificates to *brides & grooms* in Marriage Certificates (reconstructs family ties)

- `Closure`: compute the transitive closure of all detected links to get a unique identifier per individual. The output of this function is a new RDF dataset, where linked individuals are replaced by the same identifier in the civil registries dataset.


### Tool parameters
Parameters that can be provided as input to the linking tool:
- `--function`:        (required) one of the functionalities listed below

- `--inputData`:       (required) path of the HDT dataset
- `--outputDir`:       (required) path of the directory for saving the indices and the detected links
- `--maxLev`:          (optional, default = 4) integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link
- `--fixedLev`:        (optional, default = False) add this flag without a value (i.e. True) for applying the same maximum Levenshtein distance independently from the string lengths
- `--format`:          (optional, default = CSV) one of the two Strings: 'RDF' or 'CSV', indicating the desired format for saving the detected links between certificates
- `--debug`:           (optional, default = error) one of the two Strings: 'error' (only display error messages in console) or 'all' (show all warning in console)

---

## Examples

- Example 1. Run the help command of the software:

`java -jar burgerLinker.jar --help`

---

- Example 2. Link *parents of newborns* to *brides & grooms*:

`java -jar burgerLinker.jar --function Between_B_M --inputData dataDirectory/myCivilRegistries.hdt --outputDir . --format CSV  --maxLev 3 --fixedLev`

These arguments indicate that the user wants to:

    [Between_B_M] link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates,
    [dataDirectory/myCivilRegistries.hdt] in the civil registries dataset myCivilRegistries.hdt modelled according to our civil registries RDF schema,
    [.] save the detected links in the current directory,
    [CSV] as a CSV file,
    [3] allowing a maximum Levenshtein of 3 per name (first name or last name),
    [fixedLev] independently from the length of the name.

---

- Example 3. Generate an HDT file and its index from an RDF dataset:

`java -jar burgerLinker.jar --function ConvertToHDT --inputData dataDirectory/myCivilRegistries.nq --outputDir .`

This will generate the HDT file 'myCivilRegistries.hdt' and its index 'myCivilRegistries.hdt.index' in the same directory.
The index should be kept in the same directory of the HDT file to speed up all queries.

---

- Example 4. Merge two HDT files into one:

`java -jar burgerLinker.jar --function ConvertToHDT --inputData dataDirectory/hdt1.hdt,dataDirectory/hdt2.hdt --outputDir . `

This will generate a third HDT file 'merged-dataset.hdt' and its index 'merged-dataset.hdt.index' in the same directory.
The two input HDT files are separated by a comma ',' without a space)

---

## Possible direct extensions
It would be possible to add more general matching functionalities that are not dependent on the Civil Registries schema.
One possible way would be to provide a JSON Schema as an additional input to any given dataset, specifying the (i) Classes that the user wish to match their instances (e.g. sourceClass: iisg:Newborn ; targetClass: iisg:Groom), and the (ii) Properties that should be considered in the matching (e.g. schema:givenName; schema:familyName).

Subsequently, the fast matching algorithm could be used for many other linkage purposes (in Digital Humanities), e.g. places, occupations and products.
