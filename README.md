![alt text](https://github.com/CHC-Computations/Harmonize/blob/main/logo-1.png?raw=true)
# Data of the Polish Literary Bibliography in neo4j

The resource consists of bibliographic data selected from the ["Polish Literary Bibliography"](https://pbl.ibl.waw.pl/) („Polska Bibliografia Literacka”, PBL). The PBL contains information about literature, theater, and film. The PBL team — the Department of Current Bibliography of the Institute of Literary Research of the Polish Academy of Sciences (Pracownia Bibliografii Bieżącej Instytutu Badań Literackich Polskiej Akademii Nauk) — has been operating in Poznań since 1948. The creator and the first Director of the PBL authors’ team was Professor Stefan Vrtel-Wierczyński. The „Polish Literary Bibliography” documents books published in Poland in Polish and other languages as well as books published abroad in Polish and other languages when they pertain to Polish writers or when their author is Polish. Records of non-literary books on literature are, whenever possible, made by the bibliographer “with the book in the hand” (from autopsy).

## Contributors

**Institute of Literary Research of the Polish Academy of Sciences**, [www.ibl.waw.pl](https://ibl.waw.pl/)

## Resource design
The collected texts come from different sources.
- subcorpus of anthologies (program texts, criticism, and literary theory), 
- subcorpus of literary persons (selections of writings), 
- subcorpus of periodicals, 
- subcorpus of monographs.

### Balancing criteria

- literary era (or sub-period), 
- branch of literary studies (history, criticism, theory), 
- length of text, current of literary studies (e.g., structuralism, deconstruction), 
- place of publication (e.g., Lviv, Krakow, Warsaw), 
- gender of the author, 
- reception (number of reprints, citations, mentions in syllabuses).

Each criterion will be assigned a minimum and maximum value for the percentage of texts from each pool (e.g., a minimum of 20% of critical-literary texts, but no more than 33%), then we will determine the proportions for each period, taking into account the unevenness of production (e.g., a minimum of 5% and a maximum of 65% of titles from a given period should have a female author) - we will use expert consultation in determining the proportions.

### Limitations

- imprecise definition of the population – there is no complete list of publications in the field of literary studies, and the lists that can be compiled from available bibliographic data are not exhaustive
- uneven availability of publications in digital form – in particular, there is little availability of texts published between 1920 and 1990
- copyright – licenses prevent the use of particular texts in corpus work

### Statistics

The corpus contains 13 949, most of which are post-1989 texts. The corpus still needs to be balanced.

<p align="center">
  <img src="KDL_statistics.png" alt="KDL statistics" width="65%">
</p>

## Access

The corpus will soon be published in this GitHub repository as a set of .txt files.

### Metadata

The description of the corpus texts includes the following metadata:
```
identifier
type
title
author
author_gender
source
source_number
source_place
source_date
publication_date
publication_place
pages
```
The table with metadata is presented [here](https://github.com/CHC-Computations/Korpus-Dyskursu-Literaturoznawczego/blob/main/KDL_resources.xlsx).

## Use in the [GoLEM service](https://chrc.clarin-pl.eu/files/golem)

Graph Literary Machine Explorer (GoLEM) is a system for advanced analysis and visualization of the connections between terms, entities, and vocabularies (topics) in scientific texts, primarily in texts in the field of literary studies, in synchronous and diachronic dimensions.
GoLEM will offer the possibility to work on ready-made corpora or corpora uploaded by the user. A KDL will be made available as part of the service.
The following services are envisaged:
- Entity analysis: entity recognition and time-varying frequency analysis, analysis of relationships between entities in selected textual wholes (sentence, paragraph, whole document, user-defined window) and between texts or sub-corpus highlighted based on metadata; the processing pipeline will include separation of footnotes and bibliography, recognition of correlations, NEDs, and NELs (disambiguation of names of people and places)
- Analysis of terms/concepts: recognition of literary and literature terms (eventually also terms from other disciplines) and analysis of their frequency of occurrence in the corpus, in individual texts and sub-corpus taking into account changes over time, analysis of changes in the meaning of terms over time and within different sub-corpus
- Vocabulary analysis: semi-supervised topic modeling, LDA including literary entities and terms, "contextualized" topic modeling using language models.

## Licence

All texts in this collection are in the public domain. No rights reserved, texts are available under Creative Commons Attribution 4.0 International Licence [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

![alt_text](https://github.com/CHC-Computations/Harmonize/blob/main/Zrzut%20ekranu%202022-12-19%20o%2017.48.49.png?raw=true)
