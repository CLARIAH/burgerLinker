# **burgerLinker -** Civil Registries Linking Tool

BurgerLinker is a tool for linking civil registry data by matching individuals
across life events. Matches are made by lexical comparison of person names
(using dynamic Levenshtein distance) while also taking into account that of a
person's parents, partner, or spouse. Temporal information (e.g. event dates
and person age) and domain knowledge (expert rules) are used to further improve
results. 

BurgerLinker is designed to work on datasets that are modelled using the
Resource Description Framework (RDF) in which data are stored as *statements*
of three elements: *subject*, *predicate*, and *object*. These three elements
are given as International Resource Identifiers (IRI) or, in the case of the
*object*, as a raw value called a literal with an optional datatype or language
tag. Datasets of this form are also called Linked Data.


## Use Case

Historians use archival records to describe persons' lives. Each record (e.g. a
marriage record) just describes a point in time. Hence historians try to link
multiple records on the same person to describe a life course. This tool
focuses on "just" the linkage of civil records. By doing so, pedigrees of
humans can be created over multiple generations for research on social
inequality, especially in the part of health sciences where the focus is on
gene-social contact interactions.

BurgerLinker is being developed to improve and replace the current
[LINKS](https://iisg.amsterdam/en/hsn/projects/links) software. Points of
improvement are:
- extremely fast and scalable matching procedure (using Levenshtein automaton);
- searches candidate matches based on main individuals and relations, or if
  need be, allows for matching of the main individual only. (Thus the focus is
  on finding candidate matches (recall), not the quality of possible matches
  (precision), that is being developed via another CLARIAH tool);
- when matching two individuals with multiple first names, at least two names
  need to be similar in order to find a candidate match; when matching
  individuals with multiple first names to individuals with only one first name
  any first name that is identical results in a match(!);
- blocking is not required (i.e. all candidate records can be considered for
  matching, with no restrictions on their registration date or location, and no
  requirements on blocking parts of their individual names);
- candidate matches contain detailed metadata on why they are suggested, and
  can be saved in different formats (CSV and RDF are covered in the current
  version);
- allows family and life course reconstruction (by computing the transitive
  closure over all detected links);
- open software.


The latest version of BurgerLinker is a rework of the original BurgerLinker,
now referred to as the *legacy* version. This version was specifically
designed for and limited to the CIV data model and is currently deprecated.
For archival purposes, the legacy code remains available as [Legacy
branch](https://github.com/CLARIAH/burgerLinker/tree/legacy). 

### Previous work

So far, (Dutch) civil records have been linked by bespoke programming by
researchers, sometimes supported by engineers. Specifically the IISG-LINKS
program has a pipeline to link these records and provide them to the Central
Bureau of Genealogy (CBG). Because the number of records has grown over time
and the IISG-LINKS takes an enormous amount of time (weeks) to LINK all records
currently present, *burgerLinker* is designed to do this much faster (full
sample takes less than 48 hours).

The Golden Agents project has brought about [Lenticular
Lenses](https://www.goldenagents.org/tools/lenticular-lenses/) a tool designed
to link persons across sources of various nature. We have engaged with the
Lenticular Lenses team on multiple occasions (a demo-presentation, two
person-vocabulary workshops, and a specific between-teams-workshop). From those
meetings we have adopted the [ROAR
vocabulary](https://leonvanwissen.nl/vocab/roar/docs/) for work in CLARIAH-WP4.
On the specific *burgerLinker* and lenticular lenses tool, however we found
that the prerequisite in Lenticular Lenses to allow for heterogenous sources,
conflicted with the *burgerLinker* prerequisite to be fast: one reason for it
to be fast is the limited set of sources that *burgerLinker* allows for.

The only other set of initiatives that we are aware of are bespoke programming
initiatives by domain specific researchers, with country and time specific
rules for linking in for example R. These linkage tools are on the whole slow.
What we did do is make our own rule set for linking modular, to allow in the
future for country and time specific rule sets to be incorporated in
*burgerLinker*.

At the ESSHC 2023 we learned of
[population-linkage](https://github.com/stacs-srg/population-linkage) and
[hope](https://github.com/stacs-srg/population-linkage/issues/4) to set up
talks to discuss the similarities and differences in our approaches. Also at
the ESSHC 2023, we learned of the Norwegian effort for historical record
linking:
[https://github.com/uit-hdl/rhd-linking](https://github.com/uit-hdl/rhd-linking)
(for documentation see:
[https://munin.uit.no/handle/10037/28399](https://munin.uit.no/handle/10037/28399)).

Further details regarding the data standardisation and the data model are
available in the [burgerLinker
Wiki](https://github.com/CLARIAH/burgerLinker/wiki) or via the [burgerLinker
lecture](https://vimeo.com/573950112). Also see the paper on the [application
of burgerLinker](https://hlcs.nl/article/view/14685/16325) in academia (LINKS).


---

## Installation

BurgerLinker is written in the JAVA programming language which can run on
(nearly) any device and operating system. To run JAVA programs on a device,
ensure that the (free) [JAVA Runtime Environment (JRE)](https://www.oracle.com/java/technologies/javase-jre8-downloads.html)
is installed on your system or download and install the JRE from [here](https://www.oracle.com/java/technologies/javase-jre8-downloads.html).

Next, obtain the latest [BurgerLinker JAR file](https://github.com/CLARIAH/burgerLinker/releases)
(JAVA executable) from the [GitHub repository](https://github.com/CLARIAH/burgerLinker)
and place it in an appropriate directory on your device. BurgerLinker can now
be run by opening a terminal in the same directory as the JAR file, and by
running the following command:

    java -jar burgerlinker.jar --help

### Building from Source

BurgerLinker can be compiled from the source code to obtain a version with the
latest (and sometimes still experimental) changes. This is done by compiling
the code using the JAVA building tool [Maven](https://maven.apache.org/). Instructions are given below.

1. Clone this repository

    git clone https://github.com/CLARIAH/burgerLinker

2. Enter the cloned source directory

    cd burgerLinker/

3. Compile the code and create a `jar` file (the results is stored in the `target` directory)

    mvn package

**NOTE: Using an unofficial build for production usage is discouraged**

## Usage

BurgerLinker offers numerous options. The complete set of options is shown
below. As evident by the *[REQUIRED]* keyword, all invocations of the
BurgerLinker tool require specifying a work directory. All other options are
optional.

```
        -wd, --workdir
          [REQUIRED] Path of the directory for storing intermediate and final results.

        -i, --input
          [OPTIONAL] Comma-separated path(s) to one or more RDF graphs, or a web address to a SPARQL endpoint.
        -f, --function
          One of the functionalities listed below or all functions in sequence if omitted.
          FUNCTIONS:
          - Within_B_M:  Link newborns in Birth Certificates to brides/grooms in Marriage Certificates
          - Within_B_D:  Link newborns in Birth Certificates to deceased individuals in Death Certificates
          - Between_B_M: Link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates
          - Between_B_D: Link parents of newborns in Birth Certificates to deceased and their partner in Death Certificates
          - Between_M_M: Link parents of brides/grooms in Marriage Certificates to brides and grooms in Marriage Certificates
          - Between_D_M: Link parents of deceased in Death Certificates to brides and grooms in Marriage Certificates
          - Closure:     Compute the transitive closure between the found links. The output is a set of reconstructed individuals
        -m, --model
          [OPTIONAL] Path to an appropriate data model specification (YAML) or its filename (shorthand). Defaults to CIV.
        -rs, --ruleset
          [OPTIONAL] Path to a rule set definition (YAML) or its filename (shorthand). Defaults to default.

        --max-lev
          [OPTIONAL] Integer between 0 and 4 (default) indicating the maximum Levenshtein distance per first or last name allowed for accepting a link.
        --fixed-lev
          [OPTIONAL] Disable automatic adjustment of maximum Levenshtein distance to string length.
        -ns, --namespace
          [OPTIONAL] Namespace to use for reconstructed individuals. Defaults to blank nodes: '_:'.
        --ignore-relatives
          [OPTIONAL] Disable lexical comparison of related individuals (eg, parents of subject).
        --ignore-date
          [OPTIONAL] Disable temporal validation checks between candidate links.
        --ignore-block
          [OPTIONAL] Disable filtering on first letter of family name prior to lexical comparison.
        --format
          [OPTIONAL] Store the intermediate results as CSV (default) or RDF

        --query
          [OPTIONAL] Execute a custom SPARQL query on the RDF store and print the results.
        --reload
          [OPTIONAL] Reload RDF data (provided as input) instead of reusing an existing RDF store.
        --append
          [OPTIONAL] Append RDF data (provided as input) to existing RDF store.
        -h, --help
          [OPTIONAL] Print this help and exit.
        --debug
          [OPTIONAL] Enable debug messages.
```
---

### Examples

- Example 1. Run the help command of the software:

`java -jar burgerLinker.jar --help`

---

- Example 2. Parse a new dataset (modelled with PiCo) and execute all functions in sequence:

`java -jar burgerLinker.jar --input dataDirectory/myCivilRegistries.nt --model PiCo-SDO --workdir myProject/  --max-lev 3 --fixed-lev`

These arguments indicate that the user wants to:

    [--input dataDirectory/myCivilRegistries.nt] 
       parse the myCivilRegistries.nt dataset (N-Triples format) modelled according to the Persons-in-Context data model

    [--model PiCo-SDO]
       tell the parses to use the PiCo-SDO data model (with names modelled using schema.org)

    [--workdir myProject/]
       create (if needed) and use the `myProject/` directory to store intermediate and final results

    [--max-lev 3]
       allowing a maximum Levenshtein distance of 3 per name (first name or last name)

    [--fixed-lev]
       disable automatic adjustment of maximum Levenshtein distance to name length

---

- Example 3. Parse a new dataset (modelled with CIV) and Link *parents of newborns* to *brides & grooms*:

`java -jar burgerLinker.jar --input dataDirectory/myCivilRegistries1.nq,dataDirectory/myCivilRegistries2.nq --model CIV --workdir myProject/ --function between_b_m  --max-lev 4 --ruleset myRules`

These arguments indicate that the user wants to:

    [--input dataDirectory/myCivilRegistries1.nq,dataDirectory/myCivilRegistries2.nq] 
       parse both input datasets (N-Quads format) which are modelled according to the IISG's Civil Registries data model

    [--model CIV]
       tell the parses to use the CIV data model

    [--workdir myProject/]
       create (if needed) and use the `myProject/` directory to store intermediate and final results

    [--function between_b_m]
       link parents of newborns in Birth Certificates to brides and grooms in Marriage Certificates

    [--max-lev 4]
       allowing a maximum Levenshtein distance of 4 per name (first name or last name)

    [--ruleset myRules]
       use a custom rule set to filter the links

---

- Example 4. Create Family Reconstruction (using an existing RDF data store)

`java -jar burgerLinker.jar --model PiCo-PNV --workdir myProject/ --function closure --namespace https://data.iisg.nl/myProject#`

These arguments indicate that the user wants to:

    [--model PiCo-PNV]
       tell the tool to use the PiCo-PNV data model (with names modelled using Person Name Vocabulary)

    [--workdir myProject/]
       load the RDF data store in the `myProject/` directory and to store intermediate and final results there

    [--function closure]
       compute the transitive closure between the found links. The output is a set of reconstructed individuals

    [--namespace https://data.iisg.nl/myProject#]
       use the provided IRI as base namespace for the new identifiers given to reconstructed individuals

## Data Requirements

BurgerLinker expects the input dataset(s) to conform to the Resource
Description Framework (RDF) and to be formatted in one of many W3C recommended
serialization formats (e.g. N-Triples, N-Quads, JSON-LD, or HDT).

BurgerLinker accommodates different data modelling practices by moving the
data model specification from the tool's internals to the user. This is
achieved by deferring the retrieval of relevant information to a SPARQL query
engine. User-provided queries tell the query engine how the data is modelled.

BurgerLinker ships with several pre-made data model specifications:

- **PiCo-SDO** [Person-in-Context](https://github.com/CBG-Centrum-voor-familiegeschiedenis/PiCo) data model with names modelled using *schema.org* (SDO)
- **PiCo-PNV** [Person-in-Context](https://github.com/CBG-Centrum-voor-familiegeschiedenis/PiCo) data model with names modelled using *Person Name Vocabulary* (PNV)
- **CIV** [Civil Registries schema](assets/CIV.ttl) data model created by the IISG (deprecated)

Run BurgerLinker with the `--model <name of data model>` option to set the preferred data model.

### Custom Data Model

Support for a custom data model can be added by creating a new *yaml* file in the
`res/data_model` directory and by writing queries for birth, death, and
marriage certificates. The three queries should be valid SPARQL queries and must
be saved in the same *yaml* file.

A minimal data model specification takes the following form:

    ---
    BIRTHS: >
      SELECT *
      WHERE {
          ?s ?p ?o .
      }

    DEATHS: >
      SELECT *
      WHERE {
          ?s ?p ?o .
      }

    MARRIAGES: >
      SELECT *
      WHERE {
          ?s ?p ?o .
      }
    
    ...

Be aware that the queries must define all variables required by BurgerLinker.
Inspect the queries of the pre-made data model specifications to see all
required variables.

## How BurgerLinker Works

BurgerLinker uses lexical comparisons on person names to find candidate links.
To improve accuracy, the names of relations (parents, partner, etc) are
likewise matched and taken into account. Temporal consistencies and domain
knowledge are exploited to further improve accuracy. 

Each execution of a *within* or *between* function will produce a CSV file
containing linked events. These files are stored in the provided working
directory (`<workdir>/<function>/results/`) and are named according to their
function and used options:

- within-B-M-maxLev-X.csv
- within-B-D-maxLev-X.csv
- between-B-M-maxLev-X.csv
- between-B-D-maxLev-X.csv
- between-M-M-maxLev-X.csv
- between-D-M-maxLev-X.csv

Once the link sets are generated, the *closure* function will use the links in
these files to match individuals across events. Since identity links are
transitive and symmetric this function boils downs to computing transitive
closure. Newly matched individuals are given new identifiers which link to all
found matches. The output is a new N-Triple file located in
`<workdir>/closure/results/`.

---

## Post-processing rules

Links created by the execution of a *between* and *within* function can be
filtered to exclude unlikely matched. Filtering is done post processing and
works by comparing event dates. For example, whether one's registered age at
death (roughly) matches the difference between the date on the person's birth
and death certificate.

The default rules are as following:

- Persons will not become older than 110 years of age
- Persons can marry at age 13
- Children are born to: 
    1. married parents, 
    2. up to 9 months after a married father perished,
    3. up to 5 years before the parents married IF acknowledged by the father, or
    4. up to 10 years before the parents married IF acknowledged by the father from birth
- Women can give birth to children between age 14 and 50 years 
- Men can become father at age 14, and stop reproducing after their wife turns 50

The default rule set is located in the `res/rule_sets/` directory.

### Custom rule set

A custom rule set can be used to tailor the filtering step. To use custom rules
for filtering, copy the default rule set (`default.yaml`) under a different
name (e.g. `myRules.yaml`) and open the file in your preferred text editor.

Each rule contains three lines: a *name*, a *description*, and a *condition*.
An example rule is shown below:

    name: "within_b_d.timegapdiff"
    description: "The allowed difference in years between the birth and death of an individual"
    condition: "diff >= 0 && diff <= 110"

This rule ensures that the difference between the birth and death of an individual
must not exceed 110 years, and that the death should always be preceded by the
birth.

Rules can be customized by modifying the conditions. Valid conditions are
logical comparisons between `diff` and a numerical value (e.g. `<`, `>=`, `==`,
and `!=`) either solitary or paired with one more comparisons via logical
operators (`&&` for *AND* or `||` for *OR*).

Run BurgerLinker with the `--ruleset myRules` option to use the custom rule set
during post processing.
