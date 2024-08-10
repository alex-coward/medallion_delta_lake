from pyspark.sql import SparkSession

FILES_BUCKET = "medallion-files"
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


# Create Train Bronze Table using append mode to be able to keep old data and add new data
train_file_path = f'gs://{FILES_BUCKET}/raw_data/train.csv'
train_df = spark.read.csv(train_file_path, header=True, inferSchema=True)
train_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/train_bronze')

# Create Client Bronze Table using append mode to be able to keep old data and add new data
client_file_path = f'gs://{FILES_BUCKET}/raw_data/client.csv'
client_df = spark.read.csv(client_file_path, header=True, inferSchema=True)
client_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/client_bronze')

# Create County ID To Name Map Bronze Table using append mode to be able to keep old data and add new data
county_file_path = f'gs://{FILES_BUCKET}/raw_data/county_id_to_name_map.json'
county_df = spark.read.json(county_file_path)
county_df.write.format('delta').mode('overwrite').save(f'gs://{TABLES_BUCKET}/county_bronze')

# Create Electricity Prices Bronze Table using append mode to be able to keep old data and add new data
electricity_file_path = f'gs://{FILES_BUCKET}/raw_data/electricity_prices.csv'
electricity_df = spark.read.csv(electricity_file_path, header=True, inferSchema=True)
electricity_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/electricity_prices_bronze')

# Create Historical Weather Bronze Table using append mode to be able to keep old data and add new data
historical_file_path = f'gs://{FILES_BUCKET}/raw_data/historical_weather.csv'
historical_df = spark.read.csv(historical_file_path, header=True, inferSchema=True)
historical_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/historical_weather_bronze')

#  Create Forecast Weather Bronze Table using append mode to be able to keep old data and add new data
forecast_file_path = f'gs://{FILES_BUCKET}/raw_data/forecast_weather.csv'
forecast_df = spark.read.csv(forecast_file_path, header=True, inferSchema=True)
forecast_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/forecast_weather_bronze')

# Create Gas Prices Bronze Table using append mode to be able to keep old data and add new data
gas_file_path = f'gs://{FILES_BUCKET}/raw_data/gas_prices.csv'
gas_df = spark.read.csv(gas_file_path, header=True, inferSchema=True)
gas_df.write.format('delta').mode('append').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/gas_prices_bronze')

# Create Weather Station to County Mapping Bronze Table using overwrite mode since data updates aren't expected or required
station_file_path = f'gs://{FILES_BUCKET}/raw_data/weather_station_to_county_mapping.csv'
station_df = spark.read.csv(station_file_path, header=True, inferSchema=True)
station_df.write.format('delta').mode('overwrite').save(f'gs://{TABLES_BUCKET}/weather_station_to_county_mapping_bronze')