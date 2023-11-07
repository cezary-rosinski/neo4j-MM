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

The dataset consists of 1,074,952 Entities and 2,074,321 Relations.

<p align="center">
  <img src="Number of Entities.png" alt="Number of Entitiess – statistics" width="65%">
</p>

<p align="center">
  <img src="Number of Relations.png" alt="Number of Relations – statistics" width="65%">
</p>

## Access

TBD

### Metadata

TBD

## How to use

TBD

## Licence

All texts in this collection are in the public domain. No rights reserved, texts are available under Creative Commons Attribution 4.0 International Licence [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

![alt_text](https://github.com/CHC-Computations/Harmonize/blob/main/Zrzut%20ekranu%202022-12-19%20o%2017.48.49.png?raw=true)
