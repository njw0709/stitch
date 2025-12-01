---
title: 'STITCH: A Spatio-Temporal Integration Tool for Contextual and Historical enrichment of survey data.'
tags:
  - Python
  - Spatio-Temporal Integration
  - Social Science
  - Automated Data Pipeline

authors:
  - name: Jong Woo Nam
    orcid: 0009-0004-1421-1463
    equal-contrib: false
    corresponding: true
    affiliation: 1 
  - name: Eun Young Choi
    orcid: 0000-0002-7587-6272
    equal-contrib: false
    affiliation: 2
  - name: Jennifer A. Ailshire
    orcid: 0000-0002-4476-9458
    corresponding: false
    affiliation: 2

affiliations:
 - name: Neuroscience Graduate Program, University of Southern California, United States
   index: 1
   ror: 00hx57361
 - name: Leonard Davis School of Gerontology, University of Southern California, United States
   index: 2
date: 30 October 2025
bibliography: paper/paper.bib
---

# Summary

STITCH is a Python-based framework for linking diverse data sources across geospatial and temporal dimensions to enable enrichment of individual-level observational data with contextual and historical data. Primarily motivated to augment Health and Retirement Study (HRS),  the largest on-going nationally representative survey study in the United States, with spatio-temporal data (e.g., air quality, weather, neighborhood characteristics), STITCH is designed to efficiently link large-sized spatio-temporal data to each survey participants, based on their reported geographical locations (e.g. census tract (FIPS code), ZIP code, etc.). It also supports the integration of participants’ residential histories, enabling accurate linkage of contextual data to periods of residence and relocation. Designed for local deployment, STITCH provides a reproducible and user-friendly solution for spatio-temporal data integration.

# Statement of need

High resolution spatio-temporal data provide opportunities for researchers to better understand longitudinal trajectories of participants in survey-based studies by precisely situating each of them within spatio-temporal contexts. For example, Choi et al., by linking Health and Retirement Study (HRS) to daily heat index of each participants' reported residences, discovered that exposure to extreme heat accelerates aging, as measured by blood-based epigenetic measures. However, as data become higher in resolution both spatial and temporal dimensions, they become very large (> 1GB), which makes processing, as well as linkage to other datasets, difficult to be done efficiently.

In order to gain access to data linked to high resolution spatio-temporal data, researchers either had to 1) write their own script in their preferred programming language; 2) use someone else's code that does linkage and adapt it for their needs; or 3) have someone else share already-linked data. Although many programming languages, such as python, R, and Stata, offer ways to merge two datasets, merging of multiple datasets with precise time-lags, while incorporating cases like participants moving, becomes a challenging programming task for many researchers not well-versed in data management. 

As an alternative, one could share already-linked data for the community to use. As each research question require linkage to different contextual data, sharing data linked to some specific contextual information may not satisfy most researchers' needs. To make problems worse, most survey-based studies which include sensitive information such as participants' residential information, are heavily protected to ensure privacy. As such, these data can live only on network-restricted computers. Consequently, linked data cannot just be transferred over any network or be taken out of computers without permission.

Hence, we write an efficient and reproducible data linkage pipeline, to share and meet the needs of researchers who are required to do data linkages in their own local environment. STITCH is primarily developed in python. To lower the barrier of usage, STITCH comes in three flavors: 1) a graphical user interface, for those who are not proficient programmers; 2) a command-line interface for those who wants programmatic access to the tool; and finally 3) fully open-sourced python package for proficient programmers who wants to fully customize the processing pipeline for their specific needs.

Although primarily motivated by linkage to survey-based data, STITCH can be used to efficiently link a smaller dataset with time and location information to a larger dataset that links to the provided time and location. 

# Overview of the Data STITCHing Pipeline

STITCH essentially is a tool that efficiently performs multiple time-lagged merges of a smaller dataset with higher-resolution spatio-temporal dataset. Given some temporal point of observation (e.g. interview date), relevant time-lagged contextual information gets extracted and merged as additional columns to the primary data. For instance, if an user asks for 30-day history of air pollution from the interview date, STITCH calculates each lagged time points from each participant's interview date, and extracts relevant data to create columns such as 0-day prior air pollution, 1-day prior air pollution, ...., 30-day prior air pollution.
 
## Required data sources and their formats

STITCH takes in three data sources. 

First, STITCH asks for the primary dataset to link the contextual information to. This dataset is required to have columns with time information (e.g. interview date), and location information (e.g. 11 digit census-tract-level FIPS code). If using residential history, which we describe in the following paragraph, additional column with participant id must be provided.

| Participant ID | Interview Date         | Census Tract FIPS Code | Some Other Variable  |
| -------------- | ---------------------- | ---------------------- | -------------------- |
| 1              | March 2, 2023          | 12345678910            | Yes                  |
| 2              | February 17, 2023      | 67890123456            | No                   |
| 3              | January 30, 2023       | 23456789012            | Yes                  |

Second, STITCH optionally asks for a dataset with residential history. This dataset contains information about when and where each participants moved. If not provided, location information from the primary dataset is used for data linkage for all time-lags. Below is an example table with residential history information.

| Participant ID | Moved Indicator   | Year | Month | Census Tract FIPS Code  |
|----------------|-------------------|------|-------|-------------------------|
| 1              | 999.0             | 2010 | 2     | 27503002857            |
| 1              | move              | 2011 | 1     | 31093008015            |
| 2              | 999.0             | 2010 | 3     | 25328004727            |
| 2              | move              | 2011 | 1     | 50262000210            |
| 3              | 999.0             | 2012 | 3     | 67890023156            |
| 3              | move              | 2013 | 4     | 31093008015            |
| 3              | move              | 2014 | 10    | 50262000210            |
| 3              | move              | 2016 | 1     | 98765391820            |


Residential history dataset is required to have 5 columns: participant id, moved indicator, year, month, and location. Participant IDs are used to link residential history to the rows of the primary dataset. Moved indicator indicates what information each row contain. Each row can indicate two different time points: when the participant entered the study, and when the participant moved. In above example, 999.0 is designated as the indicator for when the participant entered the study, and "move" is used to indicate a change in residence for the participant. Because the dataset is in long format, a participant with multiple residential change histories will have multiple rows with "move" indicator (for instance, participant #3). 

Currently, the time information of each row is separated into two columns indicating year and month, respectively. This is done to accomodate realistic constraints of survey-based studies where information about exactly which month of the year the move happened is missing. If month information is missing, STITCH assumes that the move (or entering the survey) happened at January of the year. 

Finally, STITCH asks for the directory location of where contextual data are saved in the local environment. Contextual data is required to be saved in yearly files, with consistent naming scheme. For example, daily air pollution data might be saved as separate .csv files per year within a directory:

PM2.5/
|-- 2010_daily_pm25.csv
|-- 2011_daily_pm25.csv
|-- 2012_daily_pm25.csv
|-- 2013_daily_pm25.csv

Here, each csv files are required to be in long-format, with date, location, and measurement in separate columns.

| Date               | Census Tract FIPS Code | PM2.5     |
|--------------------|------------------------|-----------|
| January 1, 2010    | 12345678910            | X.X       |
| January 1, 2010    | 67890123456            | Y.Y       |
| ...                | ...                    | ...       |
| December 31, 2010  | 12345678910            | A.A       |
| December 31, 2010  | 67890123456            | B.B       |


Given the time and location information provided by the first two data sources STITCH efficiently extracts rows with matching date and location (FIPS code), and merges contextual information to the primary dataset. 

## General description of processing pipeline



## Limitations

* Daily lags only
* 




# Citations

<!-- Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)" -->

# Figures

<!-- Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% } -->

# Acknowledgements



# References