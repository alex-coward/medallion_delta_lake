from pyspark.sql import SparkSession
from geopy.distance import geodesic
from pyspark.sql import Row
from pyspark.sql.functions import expr
from delta.tables import DeltaTable

TABLES_BUCKET = "medallion-tables"

# Specify the paths to Delta Lake JARs
# Delta Lake 2.30 is the version that is compatible with Spark 3.3.2
# Spark 3.3.2 and Scala 2.12 are the versions installed on Dataproc 2.1.x clusters used in this project
delta_core_jar = "/usr/lib/spark/jars/delta-core_2.12-2.3.0.jar"
delta_storage_jar = "/usr/lib/spark/jars/delta-storage-2.3.0.jar"

# Build the SparkSession w/ delta lake jars
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", f"{delta_core_jar},{delta_storage_jar}") \
    .config("spark.driver.extraClassPath", f"{delta_core_jar}:{delta_storage_jar}") \
    .config("spark.executor.extraClassPath", f"{delta_core_jar}:{delta_storage_jar}") \
    .getOrCreate()

# Transform train_bronze delta table to train_silver delta table

# Load bronze table as DataFrame
train_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/train_bronze')

# Drop any duplicates created when bronze table creation is run,
# since raw data is appended to bronze tables on re-run 
train_silver_df = train_bronze_df.dropDuplicates()

# Drop 528 null values
train_silver_df = train_silver_df.dropna(how='any')

# Remove county with county_id 12, which is not an actual county but rather is listed
# as unknown and only makes up 1.5% of total data
train_silver_df = train_silver_df.filter(train_silver_df['county'] != 12)

# This code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    train_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/train_silver')
except:
    delta_table = DeltaTable.forPath(spark, f'gs://{TABLES_BUCKET}/train_silver')
    
    # Define the merge column and non-merge columns
    merge_column = 'row_id'
    non_merge_columns = [col for col in train_silver_df.columns if col != merge_column]

    # Create a dictionary for update and insert operations
    update_expr = {f"destination.{col}": f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in train_silver_df.columns}

    # Perform the merge operation
    delta_table.alias("destination").merge(
        train_silver_df.alias("source"),
        f"destination.{merge_column} = source.{merge_column}"
    ).whenMatchedUpdate(
        condition=' OR '.join([f"destination.{col} <> source.{col}" for col in non_merge_columns]),
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete(
    ).execute()


# Transform client_bronze delta table to client_silver delta table

# Load bronze table as DataFrame
client_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/client_bronze')

# Drop any duplicates created when bronze table creation is run, since raw data is appended to bronze tables on re-run 
client_silver_df = client_bronze_df.dropDuplicates()

# Drop any null values
client_silver_df = client_silver_df.dropna(how='any')

# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    client_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/client_silver')
except:
    delta_table =  DeltaTable.forPath(spark,f'gs://{TABLES_BUCKET}/client_silver')

    # Define merge and non-merge columns
    merge_columns = ['product_type', 'county', 'is_business', 'date']
    non_merge_columns = [col for col in client_silver_df.columns if col not in merge_columns]

    # Create the condition for when records match based on merge columns
    on_conditions = [f"destination.{col} = source.{col}" for col in merge_columns]
    on_clause = ' AND '.join(on_conditions)

    # Create expressions for updating the destination when there are changes in non-merge columns
    update_expr = {f"{col}": f"source.{col}" for col in non_merge_columns}
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Insert expression with all columns from the source
    insert_expr = {col: f"source.{col}" for col in client_silver_df.columns}

    # Perform the merge operation
    delta_table.alias("destination").merge(
        client_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete(
    ).execute()

# Transform electricity_prices_bronze delta table to electricity_prices_silver delta table

# Load bronze table as DataFrame
electricity_prices_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/electricity_prices_bronze')

# Drop any duplicates created when bronze table creation is run, since raw data is appended to bronze tables on re-run 
electricity_prices_silver_df = electricity_prices_bronze_df.dropDuplicates()

# Change name of 'forecast_date' column to 'electricity_effective_datetime',
# since 'forecast_date' is misleading as the prices are not forecasts but 
# rather are just effective at a future date.
electricity_prices_silver_df = electricity_prices_silver_df.withColumnRenamed('forecast_date', 'electricity_effective_datetime')

# Code can be used to show that data comes in on a delayed basis
#db_1 = electricity_prices_silver_df.filter('data_block_id = 1').count()
#db_1_before = electricity_prices_silver_df.filter('data_block_id = 1').where("forecast_date < '2021-09-01 00:00:00'").count()
#db_1_after = electricity_prices_silver_df.filter('data_block_id = 1').where("forecast_date > '2021-09-01 23:00:00'").count()
#print(f'Count of Data Block 1 data points: {db_1:,}')
#print(f'Count of Data Block 1 data points before 2021-09-01 00:00:00 : {db_1_before:,}')
#print(f'Count of Data Block 1 data points after 2021-09-01 23:00:00 : {db_1_after:,}')

# Create a new column, 'electricity_available_datetime' with a value equal to 1 day after 'electricity_effective_datetime',
# which will aid in creating joins in downstream tasks
electricity_prices_silver_df = electricity_prices_silver_df.withColumn('electricity_available_datetime', expr('electricity_effective_datetime + INTERVAL 1 DAY'))

# Code can be used to show null values in data
#electricity_nulls = {col:electricity_prices_silver_df.filter(electricity_prices_silver_df[col].isNull()).count() for col in electricity_prices_silver_df.columns}
#print(electricity_nulls)

# Drop any null values
electricity_prices_silver_df = electricity_prices_silver_df.dropna(how='any')

# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    electricity_prices_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/electricity_prices_silver')
except:
    delta_table =  DeltaTable.forPath(spark,f'gs://{TABLES_BUCKET}/electricity_prices_silver')
    # Define the merge column and non-merge columns
    merge_column = 'electricity_effective_datetime'
    non_merge_columns = [col for col in electricity_prices_silver_df.columns if col != merge_column]

    # Create expressions for the update and insert operations
    update_expr = {col: f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in electricity_prices_silver_df.columns}

    # Create the condition for when records match based on the merge column
    on_clause = f"destination.{merge_column} = source.{merge_column}"

    # Create the update condition string
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Perform the merge operation
    delta_table.alias("destination").merge(
        electricity_prices_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete().execute()

# Transform gas_prices_bronze delta table to gas_prices_silver delta table

# Load bronze table as DataFrame
gas_prices_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}//gas_prices_bronze')

# Drop any duplicates created when bronze table creation is run, since raw data is appended to bronze tables on re-run 
gas_prices_silver_df = gas_prices_bronze_df.dropDuplicates()

# Rename forecast_date column to the more accurate 'gas_effective_date'
gas_prices_silver_df = gas_prices_silver_df.withColumnRenamed('forecast_date', 'gas_effective_date')

# Code showing that the first day of data isn't available until data_block_id 1, meaning the data is delayed by 1 day**
#gas_prices_bronze_df.filter('data_block_id = 1').show()
#gas_prices_bronze_df.filter('data_block_id = 2').show()


# Code can be used to show null values in data
#gas_nulls = {col:gas_prices_bronze_df.filter(gas_prices_bronze_df[col].isNull()).count() for col in gas_prices_bronze_df.columns}
#print(gas_nulls)

# Drop any null values
gas_prices_silver_df = gas_prices_silver_df.dropna(how='any')

# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    gas_prices_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/gas_prices_silver')
except:
    delta_table = DeltaTable.forPath(spark, f'gs://{TABLES_BUCKET}/gas_prices_silver')

    # Define the merge column and non-merge columns
    merge_column = 'gas_effective_date'
    non_merge_columns = [col for col in gas_prices_silver_df.columns if col != merge_column]

    # Create expressions for the update and insert operations
    update_expr = {col: f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in gas_prices_silver_df.columns}

    # Create the condition for when records match based on the merge column
    on_clause = f"destination.{merge_column} = source.{merge_column}"

    # Create the update condition string
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Perform the merge operation
    delta_table.alias("destination").merge(
        gas_prices_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete().execute()


# Transform weather_station_to_county_bronze delta table to weather_station_to_county_silver delta table

# Load bronze table as DataFrame
weather_station_to_county_mapping_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/weather_station_to_county_mapping_bronze')

# Change name of 'county' column to clearer 'county_id'
weather_station_to_county_mapping_bronze_df = weather_station_to_county_mapping_bronze_df.withColumnRenamed('county', 'county_id')
# Use geopy to determine closest county to the weather stations with no county assigned and assign them to that county

# Create DataFrame that only includes stations assigned to a county. Latitude rounded to ensure proper matching when using geopy.
location_county_no_nulls = weather_station_to_county_mapping_bronze_df.selectExpr('Round(latitude, 1) AS latitude', 'longitude', 'county_id', 'county_name').dropna(how='any')

# Create rdd of tuples for locations with labeled counties
labeled_locations = location_county_no_nulls.rdd.map(tuple).collect()

# Create DataFrame with Null values for county
location_county_nulls = weather_station_to_county_mapping_bronze_df.filter('county_id IS NULL')

# Create DataFrame with only latitude and longitude of the stations with no county assigned. Latitude rounded to ensure proper matching when using geopy.
location_county_nulls = location_county_nulls.selectExpr('ROUND(latitude, 1) AS latitude', 'longitude')

# Create rdd of tuples for locations without labeled counties
unlabeled_locations = location_county_nulls.rdd.map(tuple).collect()

# Function that takes in a latitude, longitude tuple as location, 
# iterates over all locations in labeled_locations and uses geopy's geodesic function 
# to determine the closest county for the unlabeled latitude, longitude coordinates
def find_closest_county(location, labeled_locations):
    min_distance = float('inf')

    for labeled_location in labeled_locations:
        distance = geodesic(location, labeled_location[:2]).kilometers
        if distance < min_distance:
            min_distance = distance
            closest_county = labeled_location[2]
            closest_county_name = labeled_location[3]

    return closest_county, closest_county_name

# Empty list to store assigned counties
assigned_counties = []

# Iterates over unlabeled_locations, finding closest county using find_closest_county function and appeneding each county name, location, and county id number to 
for location in unlabeled_locations:
    county_id, county_name = find_closest_county(location, labeled_locations)
    assigned_counties.append((county_name, location[1], location[0], county_id))
    #print(f'Location {location} is closest to {county_name}, county id {county_id}')

# Creates dataframe with data points with newly-assigned station location and county info
assigned_counties_df = spark.createDataFrame([Row(county_name=entry[0], longitude=entry[1], latitude=entry[2], county_id=entry[3]) for entry in assigned_counties])

# Creates new silver DataFrame that is union of
# 1) the original station DataFrame with stations with unassigned counties removed
# and  2) the new DataFrame that only has stations with newly assigned counties based on above work with geopy
weather_station_to_county_mapping_silver_df = weather_station_to_county_mapping_bronze_df.dropna(how='any').union(assigned_counties_df)

# Rounds latitude values so they all are in the same format
weather_station_to_county_mapping_silver_df = weather_station_to_county_mapping_silver_df.selectExpr('county_name', 'longitude', 'ROUND(latitude, 1) AS latitude', 'county_id')

# Code can be used to show null values in data
#station_nulls = {col:weather_station_to_county_mapping_silver_df.filter(weather_station_to_county_mapping_silver_df[col].isNull()).count() for col in weather_station_to_county_mapping_silver_df.columns}
#print(station_nulls)

# Drop any null values
weather_station_to_county_mapping_silver_df = weather_station_to_county_mapping_silver_df.dropna(how='any')

# Save weather_station_to_county_mapping_silver_df to a silver table, using overwrite mode since data updates aren't expceted or required
weather_station_to_county_mapping_silver_df.write.format('delta').mode('overwrite').save(f'gs://{TABLES_BUCKET}/weather_station_to_county_mapping_silver')


# Transform historical_weather_bronze delta table to historical_weather_silver delta table

# Load bronze table as DataFrame
historical_weather_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/historical_weather_bronze')

# Drop any duplicates created when bronze table creation is run, since raw data is appended to bronze tables on re-run 
historical_weather_silver_df  = historical_weather_bronze_df.dropDuplicates()

# Load weather_station_to_county_mapping_silver_df from table and join with historical_weather on latitude, longitude
# to include county info with the historical weather data
weather_station_to_county_mapping_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/weather_station_to_county_mapping_silver')
historical_weather_silver_df = historical_weather_bronze_df.join(weather_station_to_county_mapping_silver_df, ['latitude', 'longitude'], 'left')

# Code to show that the historical weather data for each day is split across 2 data_block_ids, 
# with the 1st 11 hours are available on one day and the remaining hours are available on the next day

#historical_weather_silver_df.select('datetime', 'data_block_id').filter('data_block_id = 1').distinct().orderBy('datetime', 'data_block_id').show()
#historical_weather_silver_df.select('datetime', 'data_block_id').filter('data_block_id = 2').distinct().orderBy('datetime', 'data_block_id').show(25)

# Assign data from the 1st 11 hours and 2nd 13 hours to different availability datetimes to be able to join with wide table**
historical_weather_silver_df = historical_weather_silver_df.withColumn(
                                                           'historical_weather_available_datetime', 
                                                           expr('CASE WHEN HOUR(datetime) < 11 THEN datetime + INTERVAL 1 DAY ELSE datetime + INTERVAL 2 DAY END')
                                                                      )

# Code can be used to show null values in data
#historical_nulls = {col:historical_weather_silver_df.filter(historical_weather_silver_df[col].isNull()).count() for col in historical_weather_silver_df.columns}
#print(historical_nulls)

# Drop any null values
historical_weather_silver_df = historical_weather_silver_df.dropna(how='any')


# Code showing that a small number of locations and times have multiple historical weather reports
#print(historical_weather_silver_df.select("latitude", "longitude", "historical_weather_available_datetime", "datetime", "county", "county_name", "data_block_id").distinct().count())
#print(historical_weather_silver_df.select("latitude", "longitude", "historical_weather_available_datetime", "datetime", "county", "county_name").count())



# SQL code to group by identifier columns and take average of historical weather data so each unique location
# and time only has 1 set of historical weather values

# Creates clauses to be used for creating averages and group_bys in the sql query below,
# with columns in group_columuns being used for group by and all others taking the average value.
group_columns = ['latitude', 'longitude', 'historical_weather_available_datetime', 'datetime', 'data_block_id', 'county_name', 'county_id']
avg_columns = [col for col in historical_weather_silver_df.columns if col not in group_columns]

group_clause = ', '.join([f'{col}' for col in group_columns])
avg_clause = ', '.join([f'avg({col}) AS {col}' for col in avg_columns])

historical_weather_silver_df.createOrReplaceTempView('temp_table')

avg_sql = f'''
SELECT {avg_clause}, {group_clause}
  FROM temp_table
 GROUP BY {group_clause}
'''

historical_weather_silver_df = spark.sql(avg_sql)


# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    historical_weather_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/historical_weather_silver')
except:
    delta_table = DeltaTable.forPath(spark, f'gs://{TABLES_BUCKET}/historical_weather_silver')

    # Define the merge columns and non-merge columns
    merge_columns = ['latitude', 'longitude', 'historical_weather_available_datetime', 'datetime']
    non_merge_columns = [col for col in historical_weather_silver_df.columns if col not in merge_columns]

    # Create expressions for the update and insert operations
    update_expr = {col: f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in historical_weather_silver_df.columns}

    # Create the condition for when records match based on the merge columns
    on_clause = ' AND '.join([f"destination.{col} = source.{col}" for col in merge_columns])

    # Create the update condition string
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Perform the merge operation
    delta_table.alias("destination").merge(
        historical_weather_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete().execute()

# Transform forecast_weather_bronze delta table to forecast_weather_silver delta table

# Load bronze table as DataFrame
forecast_weather_bronze_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/forecast_weather_bronze')

# Drop any duplicates created when bronze table creation is run, since raw data is appended to bronze tables on re-run 
forecast_weather_silver_df = forecast_weather_bronze_df.dropDuplicates()

# Code can be used to show null values in data
#forecast_nulls = {col:forecast_weather_silver_df.filter(forecast_weather_bronze_df[col].isNull()).count() for col in forecast_weather_bronze_df.columns}
#print(forecast_nulls)

# Drop any null values
forecast_weather_silver_df = forecast_weather_silver_df.dropna(how='any')

# Load weather_station_to_county_mapping_silver_df from table and join with historical_weather on latitude, longitude
# to include county info with the historical weather data
weather_station_to_county_mapping_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/weather_station_to_county_mapping_silver')
forecast_weather_silver_df = forecast_weather_silver_df.join(weather_station_to_county_mapping_silver_df, ['latitude', 'longitude'], 'left')

# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used
try:
    forecast_weather_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/forecast_weather_silver')
except:
    delta_table = DeltaTable.forPath(spark, f'gs://{TABLES_BUCKET}/forecast_weather_silver')

    # Define the merge columns and non-merge columns
    merge_columns = ['latitude', 'longitude', 'origin_datetime', 'forecast_datetime', 'hours_ahead']
    non_merge_columns = [col for col in forecast_weather_silver_df.columns if col not in merge_columns]

    # Create expressions for the update and insert operations
    update_expr = {col: f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in forecast_weather_silver_df.columns}

    # Create the condition for when records match based on the merge columns
    on_clause = ' AND '.join([f"destination.{col} = source.{col}" for col in merge_columns])

    # Create the update condition string
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Perform the merge operation
    delta_table.alias("destination").merge(
        forecast_weather_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete().execute()