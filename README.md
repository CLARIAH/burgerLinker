## **burgerLinker -** Civil Registries Linking Tool

Further details regarding the data standardisation and the data model are available in the [burgerLinker Wiki](https://github.com/CLARIAH/burgerLinker/wiki) or via the [burgerLinker lecture](https://vimeo.com/573950112).


### Purpose
This tool is being developed to improve and replace the current [LINKS](https://iisg.amsterdam/en/hsn/projects/links) software. Points of improvement are:
- extremely fast and scalable matching procedure (using Levenshtein automaton and HDT);
- searches candidate matches based on main individuals and relations, or if need be, allows for matching of the main individual only;
- when matching two individuals with multiple first names, at least two names need to be similar in order to find a candidate match; when matching individuals with multiple first names to individuals with only one first name any first name that is identical results in a match(!);
- blocking is not required (i.e. all candidate records can be considered for matching, with no restrictions on their registration date or location, and no requirements on blocking parts of their individual names);
- candidate matches contain detailed metadata on why they are suggested, and can be saved in different formats (CSV and RDF are covered in the current version);
- allows family and life course reconstruction (by computing the transitive closure over all detected links);
- open software.

To download the latest version of the tool click [releases](https://github.com/CLARIAH/burgerLinker/releases) on the right of the screen.

### Use case
Historians use archival records to describe persons' lives. Each record (e.g. a marriage record) just describes a point in time. Hence historians try to link multiple records on the same person to describe a life course. This tool focuses on "just" the linkage of civil records. By doing so, pedigrees of humans can be created over multiple generations for research on social inequality, especially in the part of health sciences where the focus is on gene-social contact interactions.

### User profile
The software is designed for the so called "digital historians" (e.g. humanities scholars with basic command line skills) who are interested in using the Dutch civil registries for their studies, or for linking their data to it.

### Data
In its current version, the tool cannot be used to match entities from just any source. The current tool is solely focused on the linkage of civil records, relying on the sanguineous relations on the civil record, modelled according to our [Civil Registries schema](assets/CIV.ttl). An overview of the Civil Registries schema is available as a [PNG file](/assets/CIV.png), and you can browse it on [Druid](https://druid.datalegend.net/LINKS/civ).

### Previous work
So far, (Dutch) civil records have been linked by bespoke programming by researchers, sometimes supported by engineers. Specifically the IISG-LINKS program has a pipeline to link these records and provide them to the Central Bureau of Genealogy (CBG). Because the number of records has grown over time and the IISG-LINKS takes an enormous amount of time (weeks) to LINK all records currently present, *burgerLinker* is designed to do this much faster (full sample takes less than 48 hours).

The Golden Agents project has brought about [Lenticular Lenses](https://www.goldenagents.org/tools/lenticular-lenses/) a tool designed to link persons across sources of various nature. We have engaged with the Lenticular Lenses team on multiple occasions (a demo-presentation, two person-vocabulary workshops, and a specific between-teams-workshop). From those meetings we have adopted the [ROAR vocabulary](https://leonvanwissen.nl/vocab/roar/docs/) for work in CLARIAH-WP4. On the specific *burgerLinker* and lenticular lenses tool, however we found that the prerequisite in Lenticular Lenses to allow for heterogenous sources, conflicted with the *burgerLinker* prerequisite to be fast: one reason for it to be fast is the limited set of sources that *burgerLinker* allows for.

The only other set of initiatives that we are aware of are bespoke programming initiatives by domain specific researchers, with country and time specific rules for linking in for example R. These linkage tools are on the whole slow. What we did do is make our own rule set for linking modular, to allow in the future for country and time specific rule sets to be incorporated in *burgerLinker*.

---

### Operating Systems
- This tool is tested on Linux and Mac OS.
- Windows users are advised to use the Docker image.


### Installation requirements
- Only the [JAVA Runtime Environment (JRE)](https://www.oracle.com/java/technologies/javase-jre8-downloads.html), which is free and installed on almost every computer these days.

### Input requirements
- Only one RDF dataset, describing the civil registries that are modelled according to our simple [Civil Registries schema](assets/CIV.png) (see [Wiki](https://github.com/CLARIAH/burgerLinker/wiki/Adapting-the-data-model) for more details regarding the data model and the conversion to RDF).

- For efficient querying (i.e. lower memory usage with fast search), the matching tool requires the dataset to be compressed and given as an [HDT](http://www.rdfhdt.org/what-is-hdt/) file with its index. The tool allows the conversion of any valid RDF file to HDT using the `--function convertToHDT` (see Example 2 below).

### Output format
Two possible output formats to represent the detected links:
- CSV file (default if no output format is specified by the user)
- N-QUADS file (it can be specified in the parameters of the tool using `--format RDF`)

### Main dependencies
This tool mainly rely on two open-source libraries:
- [Levenshtein automata](https://github.com/universal-automata/liblevenshtein-java) (MIT License)
- [RDF-HDT](https://github.com/rdfhdt/hdt-java) (LGPL License)

### Tool functionalities

Functionalities that are supported in the current version: (case insensitive)

- `ConvertToHDT`: compress an RDF dataset given as input to an HDT file that will be generated in the same directory. This function can also be used for merging two HDT files into one (see Example 3 below)

- `ShowDatasetStats`: display some general stats about the HDT dataset, given as input.

- `Within_B_M`: link *newborns* in Birth Certificates to *brides/grooms* in Marriage Certificates (reconstructs life course)

- `Within_B_D`: link *newborns* in Birth Certificates to *deceased* individuals in Death Certificates (reconstructs life course)

- `Between_B_M`: link *parents of newborns* in Birth Certificates to *brides & grooms* in Marriage Certificates (reconstructs family ties)

- `Between_B_D`: link *parents of newborns* in Birth Certificates to *deceased & partner* in Death Certificates (reconstructs family ties)

- `Between_M_M`: link *parents of brides/grooms* in Marriage Certificates to *brides & grooms* in Marriage Certificates (reconstructs family ties)

- `Between_D_M`: link *parents of deceased* in Death Certificates to *brides & grooms* in Marriage Certificates (reconstructs family ties)

- `Closure`: compute the transitive closure of all detected links to get a unique identifier per individual. The output of this function is a new RDF dataset, where linked individuals are replaced by the same identifier in the civil registries dataset.


### Tool parameters
Parameters that can be provided as input to the linking tool:
- `--function`:        *(required)* one of the functionalities listed below

- `--inputData`:       *(required)* path of the HDT dataset
- `--outputDir`:       *(required)* path of the directory for saving the indices and the detected links
- `--maxLev`:          *(optional, default = 4)* integer between 0 and 4, indicating the maximum Levenshtein distance per first or last name allowed for accepting a link
- `--fixedLev`:        *(optional, default = False)* add this flag without a value (i.e. True) for applying the same maximum Levenshtein distance independently from the string lengths
- `--ignoreDate`:        *(optional, default = False)* add this flag without a value (i.e. True) for ignoring the date consistency check before saving a link. By default, the tool only saves links that are  temporally consistent (e.g. when linking newborns to deceased individuals, the tool checks whether the date of death is later than the individual's date of birth)
- `--ignoreBlock`:        *(optional, default = False)* add this flag without a value (i.e. True) for not requiring the equality of the last names' first letter of the matched individuals. By default, the tool only saves links between individuals that at least have the same first letter of their last names
- `--singleInd`:        *(optional, default = False)* add this flag without a value (i.e. True) for allowing the match of the main individual, without the requirement of matching their parents as well
- `--format`:          *(optional, default = CSV)* one of the two Strings: 'RDF' or 'CSV', indicating the desired format for saving the detected links between certificates
- `--debug`:           *(optional, default = error)* one of the two Strings: 'error' (only display error messages in console) or 'all' (show all warning in console)

---

### Examples

- Example 1. Run the help command of the software:

`java -jar burgerLinker.jar --help`

---

- Example 2. Generate an HDT file and its index from an RDF dataset:

`java -jar burgerLinker.jar --function ConvertToHDT --inputData dataDirectory/myCivilRegistries.nq --outputDir .`

This will generate the HDT file 'myCivilRegistries.hdt' and its index 'myCivilRegistries.hdt.index' in the same directory.
The index should be kept in the same directory of the HDT file to speed up all queries.

:warning:

This is the most memory-intensive step of the tool. Therefore, for avoiding running out of memory for larger datasets, we recommend (i) running this step on a machine with enough memory, and (ii) changing the initial lower bound and upper bound of the JAVA heap memory size, by adding the `-Xms` and `-Xmx` flags.

As an example, here are the flags used for generating the HDT file of all Dutch birth and marriage certificates:

`java -Xms64g -Xmx96g -jar burgerLinker.jar --function ConvertToHDT --inputData dataDirectory/myCivilRegistries.nq --outputDir .`

---

- Example 3. Merge two HDT files into one:

`java -jar burgerLinker.jar --function ConvertToHDT --inputData dataDirectory/hdt1.hdt,dataDirectory/hdt2.hdt --outputDir . `

This will generate a third HDT file 'merged-dataset.hdt' and its index 'merged-dataset.hdt.index' in the same directory.

:warning:

The two HDT files given as input are only separated by `,` (without empty space)

---

- Example 4. Link *parents of newborns* to *brides & grooms*:

`java -jar burgerLinker.jar --function Between_B_M --inputData dataDirectory/myCivilRegistries.hdt --outputDir . --format CSV  --maxLev 3 --fixedLev`

These arguments indicate that the user wants to:

    [Between_B_M] link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates,
    [dataDirectory/myCivilRegistries.hdt] in the civil registries dataset myCivilRegistries.hdt modelled according to our civil registries RDF schema,
    [.] save the detected links in the current directory,
    [CSV] as a CSV file,
    [3] allowing a maximum Levenshtein of 3 per name (first name or last name),
    [fixedLev] independently from the length of the name.

---

- Example 5. Family Reconstruction

`java -jar burgerLinker.jar --function closure --inputData dataDirectory/myCivilRegistries.hdt --outputDir myResultsDirectory `

This command computes the transitive closure of all links existing in the directory `myResultsDirectory`, and generates a new `finalDataset.nt.gz` dataset in this directory by replacing all matched individuals' identifiers from the `myCivilRegistries.hdt` input dataset with the same unique identifier.

**How?**

The directory `myResultsDirectory` must contain the CSV files that resulted from the linking functions described above, without changing the file names (the tool finds these files using a regular expression search in this directory). It can contain one, or all of the following CSV files, with X being any integer from 0 to 4:
- within-B-M-maxLev-X.csv
- within-B-D-maxLev-X.csv
- between-B-M-maxLev-X.csv
- between-B-D-maxLev-X.csv
- between-M-M-maxLev-X.csv
- between-D-M-maxLev-X.csv

The function will first transform the links in these CSV files, that are asserted between identifiers of certificates, into links between individuals. Since identity links are transitive and symmetric, this function computes the transitive closure of all these transformed individual links, and generates new identifiers for each resulted equivalence class.

Example:
- :newborn1 owl:sameAs :bride1
- :bride1 owl:sameAs :mother1

This means that all these identifiers (:newborn1, :bride1, and :mother1) refer to the same individual, appearing in different roles in different civil certificates. This function generates a new dataset, replacing all occurrences of these three identifiers with a single unique identifier (e.g. :i-1). This process allows the reconstruction of historical families, without the need of writing complex queries or following a large number of identity links across the dataset.

- Example 6. Link individuals without the requirement of linking one of the parents

Convert a file hkh-maids.nt to HDT
`java -jar burgerLinker.jar --function convertToHDT --inputData maids/maids-dataset/maids.nt --outputDir maids/maids-dataset/`

Merge the resulting HDT dataset of hkh-maids to the HDT file of the marriages:
`nohup java -Xms128g -Xmx192g -jar burgerLinker.jar --function convertToHDT --inputData maids/maids-dataset/maids.hdt,civ-reg-2021/HDT/marriages.hdt --outputDir maids/maids-and-marriages-dataset/ &`

Run Within_B_M with the singleInd flag on the resulted mergedDataset:
`nohup java -Xms128g -Xmx192g -jar burgerLinker.jar --function within_B_M --inputData maids/maids-and-marriages-dataset/merged-dataset.hdt --outputDir maids/results/ --maxLev 1 --ignoreDate --singleInd &`

Links are saved in the following CSV file (around 100K links detected with the above parameters):
`maids/results/within_b_m-maxLev-1-singleInd-ignoreDate/results/within-B-M-maxLev-1-singleInd-ignoreDate.csv`

NB: when running burgerLinker with nohup, the progress of the linking is saved in the nohup.out file. You can track the progress using `tail -f :
tail -f nohup.out`.

---
## Post-processing rules

### Date filtering assumptions
- Persons will not become older than 110 years of age
- Persons can marry at age 13
- Children are born to: 
    1. married parents, 
    2. up to 9 months after a married father perished,
    3. up to 5 years before the parents married IF acknowledged by the father, or
    4. up to 10 years before the parents married IF acknowledged by the father from birth
- Women can give birth to children between age 14 and 50 years 
- Men can become father at age 14, and stop reproducing after their wife turns 50

---

## Possible direct extensions
It would be possible to add more general matching functionalities that are not dependent on the Civil Registries schema.
One possible way would be to provide a JSON Schema as an additional input to any given dataset, specifying the (i) Classes that the user wish to match their instances (e.g. sourceClass: iisg:Newborn ; targetClass: iisg:Groom), and the (ii) Properties that should be considered in the matching (e.g. schema:givenName; schema:familyName).

Subsequently, the fast matching algorithm could be used for many other linkage purposes (in Digital Humanities), e.g. places, occupations and products.
