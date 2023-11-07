![alt text](https://github.com/CHC-Computations/Harmonize/blob/main/logo-1.png?raw=true)
# Data of the Polish Literary Bibliography in neo4j

The resource consists of bibliographic data selected from the ["Polish Literary Bibliography"](https://pbl.ibl.waw.pl/) („Polska Bibliografia Literacka”, PBL). The PBL contains information about literature, theater, and film. The PBL team — the Department of Current Bibliography of the Institute of Literary Research of the Polish Academy of Sciences (Pracownia Bibliografii Bieżącej Instytutu Badań Literackich Polskiej Akademii Nauk) — has been operating in Poznań since 1948. The creator and the first Director of the PBL authors’ team was Professor Stefan Vrtel-Wierczyński. The „Polish Literary Bibliography” documents books published in Poland in Polish and other languages as well as books published abroad in Polish and other languages when they pertain to Polish writers or when their author is Polish. Records of non-literary books on literature are, whenever possible, made by the bibliographer “with the book in the hand” (from autopsy).

## Contributors

**Institute of Literary Research of the Polish Academy of Sciences**, [www.ibl.waw.pl](https://ibl.waw.pl/)

## Resource design
The resource of bibliographic data consists of the following data entities:
- Person, 
- Journal, 
- Journal Article, 
- Book,
- Publisher,
- Location,
- Prize.
And following relations among these entities:
- Article published in a Journal (PublishedJournal),
- Book published by a Publisher (PublishedBook),
- Article or Book written by a Person (Wrote),
- Person awarded in a Prize (Awarded),
- Location of a Publisher, Journal, or printing of the Book (LocatedIn),
- Book or Journal about a Person (IsAbout).

### Data structure
#### Entities
```
Person:
- personId
- name
- surname
- gender (M/F/U)
- born
- died
- birthPlace
- deathPlace
- creator (TRUE/FALSE)
- secondary (TRUE/FALSE)
- debutant (TRUE/FALSE)
Journal (miejsce publikacji)
- journalId
- name
- ISSN
JournalArticle (artykuł przedmiotowy/utwór w czasopiśmie)
- jArticleId
- title
- genre
- issue
- year
- numberOfPages
- type (Literature/Secondary)
Book
- bookId
- title
- year
- genre
- numberOfPages
- type (Literature/Secondary)
Publisher
- publisherId
- name
Location
- locationId
- city
- country
- coordinates
- region
Prize
- prizeId
- name
- year
```
#### Relations
```
PublishedJournal
- jArticleId
- journalId
PublishedBook
- bookId
- publisherId
Wrote
- personId
- bookId / jArticleId
Awarded
- personId
- prizeId
LocatedIn
- publisherId / journalId / bookId
- locationId
IsAbout
- bookId / jArticleId
- personId
```

### Limitations

The „Polish Literary Bibliography” publishing series has been printed since 1954, with 45 annuals published by 2000, containing materials for the years 1944/45–1988.
The printed volumes of the PBL have been digitalized and they are available in the [Digital Repository of Scientific Institutes](https://rcin.org.pl/dlibra/publication/79343).
The online PBL database is limited to data from 1989–2023.

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
