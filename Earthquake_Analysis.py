# Import Packages 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, avg,concat, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import configparser

#Import UDF Functions
from Categorize_Magnitude import magnitude_category_udf
from Calculate_Distance import calculate_distance_udf

#Output Handling 
from WriteProcessedFile import outputfile

#Loading the Config file
config = configparser.ConfigParser()
config.read('configfile.cfg')

input_file = config.get('INPUT','FILE_PATH')
spark_output = config.get('OUTPUT','OUTPUT_PATH')
merged_file = config.get('OUTPUT','MERGED_FILE')



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Earthquake Analysis") \
    .getOrCreate()


#Schema for Earthquake database.csv 
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("Type", StringType(), True),
    StructField("Depth", FloatType(), True),
    StructField("Depth Error", FloatType(), True),
    StructField("Depth Seismic Stations", FloatType(), True),
    StructField("Magnitude", FloatType(), True),
])
# Load the dataset into a PySpark DataFrame
df = spark.read.csv(input_file, schema=schema,header=True,inferSchema=True)

#Dropping Not Needed Middle Columns
df = df.drop("Depth Error", "Depth Seismic Stations")

# Converting 'Date' and 'Time' into a single 'Timestamp' column
df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"))

# Filter earthquakes with magnitude > 5.0
df_filtered = df.filter(col("Magnitude") > 5.0)

# Group by type and calculate average depth and magnitude
df_grouped = df_filtered.groupBy("Type").agg(
    avg("Depth").alias("Average Depth"),
    avg("Magnitude").alias("Average Magnitude")
)
df_grouped.show()

# Adding a new column 'Magnitude' Using UDF function to categorize magnitude 
df_filtered = df_filtered.withColumn("Magnitude Category", magnitude_category_udf(col("Magnitude")))

# Adding a new column 'DistanceFromRef' Using UDF function which calculates the distance from Ref
ref = [0,0] #Passing Reference as (0.0)
df_filtered = df_filtered.withColumn("DistanceFromRef", calculate_distance_udf(col("Latitude"), col("Longitude"),lit(ref[0]),lit(ref[1])))

# Save the processed DataFrame to a CSV file
df_filtered.write.mode('overwrite').csv(spark_output, header=True)

#Using Created Merge Function
outputfile(spark_output,merged_file)

