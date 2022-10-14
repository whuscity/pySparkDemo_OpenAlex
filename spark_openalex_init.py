import findspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # apache 查询使用文档
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import getopt

class OpenAlex():
  # return stream path
  def getFullpath(self, streamName):
    path = r'/home/Public/OpenAlex/csv-files/' + self.streams[streamName][0]
    return path

  datatypedict = {
    'bool' : BooleanType(),
    'int' : IntegerType(),
    'uint' : IntegerType(),
    'long' : LongType(),
    'ulong' : LongType(),
    'float' : FloatType(),
    'string' : StringType(),
    'DateTime' : DateType(),
  }

  # return stream schema
  def getSchema(self, streamName):
    schema = StructType()
    for field in self.streams[streamName][1]:
      fieldname, fieldtype = field.split(':')
      nullable = fieldtype.endswith('?')
      if nullable:
        fieldtype = fieldtype[:-1]
      schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
    return schema

  # return stream dataframe
  def getDataframe(self, streamName):
    return spark.read.format('csv').options(header='true', delimiter='\t').schema(self.getSchema(streamName)).load(self.getFullpath(streamName))

  # define stream dictionary
  streams = {
    'Authors' : ('authors.tsv', ['id:string', 'orcid:string?', 'display_name:string?', 'display_name_alternatives:string?', 'works_count:int?', 'cited_by_count:int?', 'last_known_institution:string?', 'works_api_url:string', 'updated_date:DateTime?']),
    'AuthorsIds' : ('authors_ids.tsv', ['author_id:string', 'openalex:string?', 'orcid:string?', 'scopus:string?', 'twitter:string?', 'wikipedia:string?', 'mag:string?']),
    'AuthorsCountsByYear' : ('authors_counts_by_year.tsv', ['author_id:string', 'year:int', 'works_count:int?', 'cited_by_count:int?']),
    'Concepts' : ('concepts.tsv', ['id:string', 'wikidata:string?', 'display_name:string?', 'level:int?', 'description:string?', 'works_count:int?', 'cited_by_count:int?', 'image_url:string?', 'image_thumbnail_url:string?', 'works_api_url:string', 'updated_date:DateTime?']),
    'ConceptsAncestors' : ('concepts_ancestors.tsv', ['concept_id:string', 'ancestor_id:string']),
    'ConceptsCountsByYear' : ('concepts_counts_by_year.tsv', ['concept_id:string', 'year:int', 'works_count:int?', 'cited_by_count:int?']),
    'ConceptsIds' : ('concepts_ids.tsv', ['concept_id:string', 'openalex:string?', 'wikidata:string?', 'wikipedia:string?', 'umls_aui:string?', 'umls_cui:string', 'mag:string?']),
    'ConceptsRelatedConcepts' : ('concepts_related_concepts.tsv' , ['concept_id:string', 'related_concept_id:string', 'score:float?']),
    'Institutions' : ('institutions.tsv', ['id:string', 'ror:string?', 'display_name:string?', 'country_code:string?', 'type:string?', 'homepage_url:string?', 'image_url:string?', 'image_thumbnail_url:string?', 'display_name_acroynyms:string?', 'display_name_alternatives:string?', 'works_count:int?', 'cited_by_count:int?', 'works_api_url:string', 'updated_date:DateTime?']),
    'InstitutionsAssociatedInstitutions' : ('institutions_associated_institutions.tsv', ['institution_id:string?', 'associated_institution_id:string?', 'relationship:string?']),
    'InstitutionsCountsByYear' : ('institutions_counts_by_year.tsv', ['institution_id:string', 'year:int', 'works_count:int?', 'cited_by_count:int?']),
    'InstitutionsGeo' : ('institutions_geo.tsv', ['institution_id:string', 'city:string?', 'geonames_city_id:string?', 'region:string?', 'country_code:string?', 'country:string?', 'latitude:float?', 'longitude:float?']),
    'InstitutionsIds' : ('institutions_ids.tsv', ['institution_id:string', 'openalex:string?', 'ror:string?', 'grid:string?', 'wikipedia:string?', 'wikidata:string?', 'mag:string?']),
    'Works' : ('works.tsv', ['id:string', 'doi:string?', 'title:string?', 'display_name:string?', 'publication_year:int?', 'publication_date:string?', 'type:string?', 'cited_by_count:int?', 'is_retracted:bool?', 'is_paratext:bool?', 'host_venue:string?']),
    'WorksAuthorships' : ('works_authorships.tsv', ['work_id:string?', 'author_position:string?', 'author_id:string?', 'institution_id:string?']),
    'WorksAlternateHostVenues' : ('works_alternate_host_venues.tsv', ['work_id:string?', 'alternate_host_venue:string?']),
    'WorksBiblio' : ('works_biblio.tsv', ['work_id:string', 'volume:string?', 'issue:string?', 'first_page:string?', 'last_page:string?']),
    'WorksConcepts' : ('works_concepts.tsv', ['work_id:string?', 'concept_id:string?', 'score:float?']),
    'WorksHostVenues' : ('works_host_venues.tsv', ['work_id:string?', 'venue_id:string?', 'url:string?', 'is_oa:bool?', 'version:string?', 'licence:string?']),
    'WorksIds' : ('works_ids.tsv', ['work_id:string', 'openalex:string?', 'doi:string?', 'mag:string?', 'pmid:string?']),
    'WorksMesh' : ('works_mesh.tsv', ['work_id:string?', 'descriptor_ui:string?', 'descriptor_name:string?', 'qualifier_ui:string?', 'qualifier_name:string?']),
    'WorksOpenAccess' : ('works_open_access.tsv', ['work_id:string', 'is_oa:bool?', 'oa_status:string?', 'oa_url:string?']),
    'WorksRelatedWorks': ('works_related_works.tsv', ['work_id:string?', 'related_work_id:string?']),
    'WorksReferencedWorks' : ('works_referenced_works.tsv', ['work_id:string?', 'referenced_work_id:string?']),
    'Venues' : ('venues.tsv', ['id:string', 'issn_l:string?', 'display_name:string?', 'publisher:string?', 'works_count:int?', 'cited_by_count:int?', 'is_oa:bool?', 'is_in_doaj:bool?', 'homepage_url:string?', 'works_api_url:string?', 'updated_date:DateTime?']),
    'VenuesCountsByYear' : ('venues_counts_by_year.tsv', ['venue_id:string', 'year:int', 'works_count:int?', 'cited_by_count:int?']),
    'VenuesIds' : ('venues_ids.tsv', ['venue_id:string', 'openalex:string?', 'issn_l:string?', 'issn:string?', 'mag:string?'])
  }

if __name__ == '__main__':
    findspark.init('/opt/spark-3.1.2')

    opts, _ = getopt.getopt(sys.argv[1:], 'p:', ["help", "appname=", "port="])
    sparks = SparkSession \
        .builder \
        .master("spark://c8mao:7077")\
        .config("spark.executor.cores","4")\
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .appName("OpenAlex_Username_Demo")
    appnameFlag = False
    for opt, value in opts:
        if opt in ("-h", "--help"):
            print("Usage:\n\t\t-h, --help\thelp for spark_openalex_init.py\n\t\t-p=<port>, --port=<port>\tassign app spark ui to <port>.\n\t\t--appname=<name>\tset app name to <name>.")
            exit(0)
        elif opt == "--appname":
            sparks.appName(value)
            appnameFlag = True
        elif opt in ("-p", "--port"):
            sparks.config("spark.ui.port",value)

    if appnameFlag == False:
        spark.appName("OpenAlex_Default")
    
    spark = sparks.getOrCreate()
    display(spark)

    OA = OpenAlex()

    to_array = udf(lambda x: x.replace("\"","").replace("[","").replace("]","").split(','), ArrayType(StringType()))

    Authors = OA.getDataframe('Authors')
    AuthorsIds = OA.getDataframe('AuthorsIds')
    AuthorsCountsByYear = OA.getDataframe('AuthorsCountsByYear')
    Concepts = OA.getDataframe('Concepts')
    ConceptsAncestors = OA.getDataframe('ConceptsAncestors')
    ConceptsCountsByYear = OA.getDataframe('ConceptsCountsByYear')
    ConceptsIds = OA.getDataframe('ConceptsIds')
    ConceptsRelatedConcepts = OA.getDataframe('ConceptsRelatedConcepts')
    Institutions = OA.getDataframe('Institutions')
    InstitutionsAssociatedInstitutions = OA.getDataframe('InstitutionsAssociatedInstitutions')
    InstitutionsCountsByYear = OA.getDataframe('InstitutionsCountsByYear')
    InstitutionsGeo = OA.getDataframe('InstitutionsGeo')
    InstitutionsIds = OA.getDataframe('InstitutionsIds')
    Works = OA.getDataframe('Works')
    WorksAuthorships = OA.getDataframe('WorksAuthorships')
    WorksAlternateHostVenues = OA.getDataframe('WorksAlternateHostVenues')
    WorksBiblio = OA.getDataframe('WorksBiblio')
    WorksConcepts = OA.getDataframe('WorksConcepts')
    WorksHostVenues = OA.getDataframe('WorksHostVenues')
    WorksIds = OA.getDataframe('WorksIds')
    WorksMesh = OA.getDataframe('WorksMesh')
    WorksOpenAccess = OA.getDataframe('WorksOpenAccess')
    WorksRelatedWorks = OA.getDataframe('WorksRelatedWorks')
    WorksReferencedWorks = OA.getDataframe('WorksReferencedWorks')
    Venues = OA.getDataframe('Venues')
    VenuesCountsByYear = OA.getDataframe('VenuesCountsByYear')
    VenuesIds = OA.getDataframe('VenuesIds')


    Authors = Authors.withColumn("display_name_alternatives_array", to_array(Authors.display_name_alternatives))
    Institutions = Institutions.withColumn("display_name_alternatives_array", to_array(Institutions.display_name_alternatives))
    Venues = Venues.withColumn("display_name_array", to_array(Venues.display_name))
    VenuesIds = VenuesIds.withColumn("issn_array", to_array(VenuesIds.issn))