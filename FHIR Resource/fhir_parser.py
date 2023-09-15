######################################
#####Importing required modules#######
######################################
from pyspark.sql.functions import from_json, col, explode, regexp_replace, when, isnull, input_file_name, split, size
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, MapType, BooleanType
from pyspark.sql.session import SparkSession

from pyspark.context import SparkConf
from datetime import datetime


spark = SparkSession.builder.master("local[1]") \
                            .appName("fhir_parser") \
                            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
                            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
                            .getOrCreate()



if __name__ == "__main__":
    #Schema initially obtained from df.schema in previous run
    #Modified as per requirement
    #Reading name as array of JSON strings
    #Reading address as array of JSON strings
    #Reading telecom as array of JSON strings
    schema = StructType([
                StructField('_birthDate', StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueDateTime', StringType(), True)]), True), True)]), True), StructField('_gender', StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True)]), True), True)]), True), StructField('active', BooleanType(), True), StructField('address', ArrayType(StringType(), True), True), StructField('birthDate', StringType(), True), StructField('contact', ArrayType(StructType([StructField('address', StructType([StructField('city', StringType(), True), StructField('district', StringType(), True), StructField('line', ArrayType(StringType(), True), True), StructField('period', StructType([StructField('start', StringType(), True)]), True), StructField('postalCode', StringType(), True), StructField('state', StringType(), True), StructField('type', StringType(), True), StructField('use', StringType(), True)]), True), StructField('gender', StringType(), True), StructField('name', StructType([StructField('_family', StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True), StructField('valueString', StringType(), True)]), True), True)]), True), StructField('family', StringType(), True), StructField('given', ArrayType(StringType(), True), True)]), True), StructField('organization', StructType([StructField('display', StringType(), True), StructField('reference', StringType(), True)]), True), StructField('period', StructType([StructField('start', StringType(), True)]), True), StructField('relationship', ArrayType(StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), True), StructField('telecom', ArrayType(StructType([StructField('system', StringType(), True), StructField('value', StringType(), True)]), True), True)]), True), True), StructField('deceasedBoolean', BooleanType(), True), StructField('deceasedDateTime', StringType(), True), StructField('gender', StringType(), True), StructField('id', StringType(), True), StructField('identifier', ArrayType(StructType([StructField('assigner', StructType([StructField('display', StringType(), True)]), True), StructField('period', StructType([StructField('start', StringType(), True)]), True), StructField('system', StringType(), True), StructField('type', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), StructField('use', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('link', ArrayType(StructType([StructField('other', StructType([StructField('reference', StringType(), True)]), True), StructField('type', StringType(), True)]), True), True), StructField('managingOrganization', StructType([StructField('display', StringType(), True), StructField('reference', StringType(), True)]), True), StructField('meta', StructType([StructField('versionId', StringType(), True)]), True), StructField('name', ArrayType(StringType(), True), True), StructField('photo', ArrayType(StructType([StructField('contentType', StringType(), True), StructField('data', StringType(), True)]), True), True), StructField('resourceType', StringType(), True), StructField('telecom', ArrayType(StringType(), True), True), StructField('text', StructType([StructField('div', StringType(), True), StructField('status', StringType(), True)]), True)])
    
    ################################################
    ########Reading all files in directory##########
    ################################################
    filename = '*'
    df = spark.read.schema(schema) \
      .option("multiline","true") \
      .json(f's3://sample-bucket-hek/fhir_patients/{filename}.json')
    
    ################################################
    ########Extracting official given name##########
    ################################################
    df = (df.withColumn("name_array", explode(col("name"))) #Put each name struct in its own row
    .withColumn("name_struct", from_json(col("name_array"), MapType(StringType(),StringType()))) #Convert each json string to struct (to filter by key later on)
    .withColumn("final_given_name", col("name_struct").given).filter(col("name_struct.use") == 'official') #Get only the official given names
    .withColumn("official_given_name", regexp_replace('final_given_name', '[\[\]\"]', ''))  #Remove brackets [ and ] and quotation marks
    )
    
    ##########################################################
    ########Extracting home address from JSON string##########
    ##########################################################
    #If null then Null
    #If text is not available then line is home address else text is home address
    #Assuming every array of struct/json string for address column has home address as first element or only element
    df = df.withColumn('home_address', when(isnull(col("address")), 'Null') \
                   .otherwise(
                      when(isnull(from_json(col("address")[0], MapType(StringType(),StringType())).text),
                           from_json(col("address")[0], MapType(StringType(),StringType())).line)
                      .otherwise(from_json(col("address")[0], MapType(StringType(),StringType())).text)))
    
    ##################################################
    ########Extracting city from JSON string##########
    ##################################################
    #If null then Null
    #If city is not available then null
    #Else city field used
    #Assuming every array of struct/json string for address column has city field in first element or only element
    df = df.withColumn('city', when(isnull(col("address")), 'Null') \
                   .otherwise(
                      when(isnull(from_json(col("address")[0], MapType(StringType(),StringType())).city),
                           'Null')
                      .otherwise(from_json(col("address")[0], MapType(StringType(),StringType())).city)))
                      
    #########################################################
    ########Extracting phone value from JSON string##########
    #########################################################
    #If null then Null
    #If value is not available in first element, then try second element
    #Assuming every array of struct/json string for telecom column has phone in first element or second element, if not null
    df = df.withColumn('phone', when(isnull(col("telecom")), 'Null') \
                   .otherwise(
                      when(isnull(from_json(col("telecom")[0], MapType(StringType(),StringType())).value),
                           from_json(col("telecom")[1], MapType(StringType(),StringType())).value)
                      .otherwise(from_json(col("telecom")[0], MapType(StringType(),StringType())).value))
                      )
    
    #########################################################
    ########Filename tagging for data lineage purposes#######
    #########################################################
    df = (df.withColumn('filename', input_file_name()) #getting whole filepath
      .withColumn('filename', split(col('filename'), '/')) #split s3 path using /
      .withColumn('filename', col('filename')[size(col('filename'))-1]) #get last element from array
      )
      
    #########################################################
    ########Save dataframe to CSV, with required fields######
    #########################################################
    output_path = 's3://sample-bucket-hek/fhir_output/'
    final_df = df.select("official_given_name", "birthDate", "gender", "active", "home_address", "city", "phone", "filename")
    final_df.coalesce(1) \
    .write.option("header",True) \
     .csv(output_path)