# Import Packages 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, avg,concat, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType



#Import UDF Functions
from Categorize_Magnitude import magnitude_category_udf
from Calculate_Distance import calculate_distance_udf

#Output Handling 
from WriteProcessedFile import outputfile


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Earthquake Analysis") \
    .getOrCreate()



path = r"C:\Users\aswan\OneDrive\Documents\Earthquake_Analysis\data\database.csv"

#Schema for Earthquake database.csv 
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("Type", StringType(), True),
    StructField("Depth", FloatType(), True),
    StructField("Magnitude", FloatType(), True),
])
# Load the dataset into a PySpark DataFrame
df = spark.read.csv(path, header=True,schema=schema)

# Converting 'Date' and 'Time' into a single 'Timestamp' column
df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"))

# Filter earthquakes with magnitude > 5.0
df_filtered = df.filter(col("Magnitude") > 5.0)

# Group by type and calculate average depth and magnitude
df_grouped = df_filtered.groupBy("Type").agg(
    avg("Depth").alias("Average Depth"),
    avg("Magnitude").alias("Average Magnitude")
)

# Adding a new column 'Magnitude' Using UDF function to categorize magnitude 
df_filtered = df_filtered.withColumn("Magnitude Category", magnitude_category_udf(col("Magnitude")))

# Adding a new column 'DistanceFromRef' Using UDF function which calculates the distance from Ref
ref = [0,0] #Passing Reference as (0.0)
df_filtered = df_filtered.withColumn("DistanceFromRef", calculate_distance_udf(col("Latitude"), col("Longitude"),lit(ref[0]),lit(ref[1])))

# df_filtered.printSchema()
# Save the processed DataFrame to a CSV file
output_path =  r"C:\Users\aswan\OneDrive\Documents\Earthquake_Analysis\data\output"
# # # df.coalesce(1).write.option("header","true").format("csv").save(output_path)

# # # df_grouped.show()
df_filtered.write.mode('overwrite').csv(output_path, header=True)

merged_file =  r"C:\Users\aswan\OneDrive\Documents\Earthquake_Analysis\Earthquake_Data_Processed.csv"
outputfile(output_path,merged_file)

