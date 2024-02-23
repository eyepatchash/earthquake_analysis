from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


#Function to categorize magnntude into Low,Moderate and High
def magnitude_category(magnitude):
    if magnitude <= 5.5:
        return "Low"
    elif magnitude <= 6.5:
        return "Moderate"
    else:
        return "High"

# Adding as Pyspark UDF 
magnitude_category_udf = udf(magnitude_category, StringType())
