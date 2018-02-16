#coding: utf-8
#
from gdelt_data_parser import gdelt_data_parser as gdp
import re

def cust_parse_gkg_data(line):
    '''
    parser for GDELT data,
    based on http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf

    :param line:
    :return:
    '''
    field_ids = ['GKGRECORDID', #(string) Each GKG record is assigned a globally unique identifier
                 'V2.1DATE', #(integer) This is the date in YYYYMMDDHHMMSS format on which the news media used to construct this GKG file was published.
                 'V2SOURCECOLLECTIONIDENTIFIER',#(integer) This is a numeric identifier that refers to the source collection the document came from and is used to interpret the DocumentIdentifier in the next column
                 'V2SOURCECOMMONNAME', #(text) This is a human-friendly identifier of the source of the document
                 'V2DOCUMENTIDENTIFIER', #(text) This is the unique external identifier for the source document.
                 'V1COUNTS', #(semicolon-delimited blocks, with pound symbol delimited fields) This is the list of Counts found in this document
                 'V2.1COUNTS',  #. (semicolon-delimited blocks, with pound symbol  delimited fields) This field is identical to the V1COUNTS field except that it adds a final additional field to the end of each entry that records its approximate character offset in the document, allowing it to be associated with other entries from other “V2ENHANCED” fields (or Events) that appear in closest proximity to it.
                 'V1THEMES', # (semi-colon-delimited) This is the list of all Themes found in the document.
                 'V2ENHANCEDTHEMES', #(semicolon-delimited blocks, with comma-delimited fields) This= contains a list of all GKG themes referenced in the document
                 'V1LOCATIONS', #semicolon-delimited blocks, with pound symbol delimited fields) This is a list of all locations found in the text, extracted through the Leetaru (2012) algorithm
                 'V2ENHANCEDLOCATIONS', #(semicolon-delimited blocks, with pound symbol  delimited fields) This field is identical to the V1LOCATIONS field
                 'V1PERSONS', # (semicolon-delimited) This is the list of all person names found in the text, extracted through the Leetaru (2012) algorithm
                 'V2ENHANCEDPERSONS', #(semicolon-delimited blocks, with comma-delimited fields) This contains a list of all person names referenced in the document, along with the character offsets of approximately where in the document they were found
                 'V1ORGANIZATIONS', #(semicolon-delimited) This is the list of all company and organization names found in the text, extracted through the Leetaru (2012) algorithm
                 'V2ENHANCEDORGANIZATIONS', #semicolon-delimited blocks, with comma-delimited fields) This contains a list of all organizations/companies referenced in the document, along with the character offsets of approximately where in the document they were found.
                 'V1.5TONE', #This field contains a comma-delimited list of six core emotional dimensions
                 'V2.1ENHANCEDDATES', #(semicolon-delimited blocks, with comma-delimited fields) This contains a list of all date references in the document, along with the character offsets of approximately where in the document they were found
                 'V2GCAM', #(comma-delimited blocks, with colon-delimited key/value pairs) The Global Content Analysis Measures (GCAM) system runs an array of content analysis systems over each document and compiles their results into this field
                 'V2.1SHARINGIMAGE', #Many news websites specify a so-called sharing image for each article in which the news outlet manually specifies a particular image to be displayed when that article is shared via social media or other formats
                 'V2.1RELATEDIMAGES', #(semicolon-delimited list of URLs)
                 'V2.1SOCIALIMAGEEMBEDS', #(semicolon-delimited list of URLs). News websites are increasingly embedding image-based social media posts inline in their articles to illustrate them with realtime reaction or citizen reporting from the ground
                 'V2.1SOCIALVIDEOEMBEDS', #(semicolon-delimited list of URLs). News websites are increasingly embedding videos inline in their articles to illustrate them with realtime reaction or citizen reporting from the ground
                 'V2.1QUOTATIONS', #(pound-delimited  blocks, with pipe-delimited (|) fields). News coverage frequently features excerpted statements from participants in an event and/or those affected by it and these quotations can offer critical insights into differing perspectives and emotions surrounding that event
                 'V2.1ALLNAMES', #(semicolon-delimited blocks, with comma-delimited fields) This field contains a list of all proper names referenced in the document, along with the character offsets of approximately where in the document they were found
                 'V2.1AMOUNTS',# (semicolon-delimited blocks, with comma-delimited fields) This field contains a list of all precise numeric amounts referenced in the document, along with the character  offsets of approximately where in the document they were found.
                 'V2.1TRANSLATIONINFO', #(semicolon-delimited fields) This field is used to record provenance information for machine translated documents indicating the original source language and the citation of the translation system used to translate the document for processing
                 'V2EXTRASXML' # (special XML formatted) This field is reserved to hold special non-standard data applicable to special subsets of the GDELT collection
                 ]

    #Parse the whole line
    fields = line.split('\t')
    gkg_dict = dict(zip(field_ids, fields))

    #parse parts

    gkg_dict['V2ENHANCEDPERSONS'] = gdp.parse_gkg_subsection(gkg_dict['V2ENHANCEDPERSONS'], ';', ',', ['PERSON', 'CHAR_OFFSET'])

    gkg_dict['V1.5TONE'] = gdp.parse_gkg_subsection(gkg_dict['V1.5TONE'], ';', ',',
                     ['TONE', 'POS_SCORE', 'NEG_SCORE', 'POLARITY', 'ACT_REF_DENS', 'SELF_REF_DENS', 'WORD_COUNT'])

    #parse URL
    pattern = '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)'
    gkg_dict['V2DOCUMENTIDENTIFIER'] = re.match(pattern, gkg_dict['V2DOCUMENTIDENTIFIER'])[0]

    #cut date
    gkg_dict['V2.1DATE'] = gkg_dict['V2.1DATE'][0:8]

    ret_list = {k:gkg_dict[k] for k in ('GKGRECORDID','V2ENHANCEDPERSONS','V1.5TONE','V2DOCUMENTIDENTIFIER','V2.1DATE') if k in gkg_dict}
    return ret_list



from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)

#read all export.CSV data into a single RDD
lines = sc.textFile("hdfs:///user/jdoering/gdelt/gkf_sub/*.csv", 100)

#parse data
parsed_data = lines.map(cust_parse_gkg_data).collect()

for l in parsed_data[1:10]:
    print(l)
