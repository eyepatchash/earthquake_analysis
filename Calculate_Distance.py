from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math

# Calculates the Distance to Earthquake from Reference Point 
#Logic : This can be done using Haversine Formula since we have lat and long 
def calculate_distance(lat, lon, ref_lat, ref_lon):

    # Convert lat and long from degrees to radians since the formula needs them radians
    lat, lon, ref_lat, ref_lon = map(math.radians, [lat, lon, ref_lat, ref_lon])
    
    # Formula 
    dlon = lon - ref_lon
    dlat = lat - ref_lat

    a = math.sin(dlat / 2)**2 + math.cos(ref_lat) * math.cos(lat) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Radius of Earth in kilometers
    R = 6371.0
    distance = R * c
    return distance

# Adding as Pyspark UDF 
calculate_distance_udf = udf(calculate_distance, DoubleType())

