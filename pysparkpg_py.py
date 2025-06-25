

!pip install pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Trip Details") \
    .getOrCreate()
trip_data = [
    (1, "North India Tour", "2025-06-10", "2025-06-20"),
    (2, "South India Tour", "2025-07-01", "2025-07-10")
]
trip_columns = ["TripID", "TripName", "StartDate", "EndDate"]
trips_df = spark.createDataFrame(trip_data, trip_columns)
traveller_data = [
    (101, "Rajesh Kumar", "Chennai", 1),
    (102, "Niranjan", "Delhi", 1),
    (103, "Muthukrishnan", "Hyderabad", 2),
    (104, "NJ", "Kolkata", 2)
]
traveller_columns = ["TravellerID", "Name", "City", "TripID"]
travellers_df = spark.createDataFrame(traveller_data, traveller_columns)
spots_data = [
    (1, "Taj Mahal", "Agra"),
    (1, "Golden Temple", "Amritsar"),
    (2, "Meenakshi Temple", "Madurai"),
    (2, "Marina Beach", "Chennai")
]
spots_columns = ["TripID", "SpotName", "Location"]
spots_df = spark.createDataFrame(spots_data, spots_columns)
print("Trip Details:")
trips_df.show()
print(" Traveller Addresses:")
travellers_df.show()
print(" Visiting Spots:")
spots_df.show()
full_trip_info = travellers_df.join(trips_df, "TripID") \
    .join(spots_df, "TripID") \
    .select("TripName", "Name", "City", "SpotName", "Location", "StartDate", "EndDate")
print(" Complete Trip Overview:")
full_trip_info.show(truncate=False)
spark.stop()
