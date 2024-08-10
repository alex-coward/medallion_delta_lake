from pyspark.sql import SparkSession
from geopy.distance import geodesic
from pyspark.sql import Row
from pyspark.sql.functions import col
from delta.tables import DeltaTable

TABLES_BUCKET = "medallion-tables"

# Specify the paths to Delta Lake JARs
# Delta Lake 2.30 is the version that is compatible with Spark 3.3.2
# Spark 3.3.2 and Scala 2.12 are the versions installed on Dataproc 2.1.x clusters used in this project
delta_core_jar = "/usr/lib/spark/jars/delta-core_2.12-2.3.0.jar"
delta_storage_jar = "/usr/lib/spark/jars/delta-storage-2.3.0.jar"

# Build the SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", f"{delta_core_jar},{delta_storage_jar}") \
    .config("spark.driver.extraClassPath", f"{delta_core_jar}:{delta_storage_jar}") \
    .config("spark.executor.extraClassPath", f"{delta_core_jar}:{delta_storage_jar}") \
    .getOrCreate()

# Load silver tables into dataframes
train_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/train_silver')
client_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/client_silver')
electricity_prices_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/electricity_prices_silver')
gas_prices_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/gas_prices_silver')
historical_weather_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/historical_weather_silver')
forecast_weather_silver_df = spark.read.format('delta').load(f'gs://{TABLES_BUCKET}/forecast_weather_silver')

# Joins to create wide table for ML

# Rename client_silver columns that will be joined into wide table to aid with identifying them
client_silver_df = client_silver_df.withColumnRenamed('date', 'client_date').withColumnRenamed('product_type', 'client_product_type').withColumnRenamed('county', 'client_county').withColumnRenamed('is_business', 'client_is_business').withColumnRenamed('data_block_id', 'client_data_block_id')

# Perform left join of train_silver_df with client_silver_df on 'product_type', 'county', 'is_business', 'data_block_id' columns**
wide_silver_df = train_silver_df.join(client_silver_df, (train_silver_df.product_type == client_silver_df.client_product_type) & (train_silver_df.county == client_silver_df.client_county) & (train_silver_df.is_business == client_silver_df.client_is_business) &(train_silver_df.data_block_id == client_silver_df.client_data_block_id), 'left')

# Code can be used to show null values in data
#wide_nulls = {col:wide_silver_df.filter(wide_silver_df[col].isNull()).count() for col in wide_silver_df.columns}
#print(wide_nulls)

# Given that Client had no data for the first 2 data_block_ids, those rows have null values for client info and are dropped
# wide_silver_df.filter('data_block_id = 0 OR data_block_id = 1').show()
wide_silver_df = wide_silver_df.filter(train_silver_df['data_block_id'] != 0).filter((train_silver_df['data_block_id'] != 1))


# Code can be used to show null values in data
#wide_nulls = {col:wide_silver_df.filter(wide_silver_df[col].isNull()).count() for col in wide_silver_df.columns}
#print(wide_nulls)

# There are still some null values, owing to the fact that
# not all time periods in the original client table have data for 
# all of the 68 distinct product_type, county, is_business combinations
# (see for example data_block_id 2 having 61). The 2784 nulls are dropped.

#client_silver_df.filter('data_block_id = 2').show()
#train_silver_df.select('prediction_unit_id').distinct().count()

wide_silver_df = wide_silver_df.dropna(how='any')

# Rename electricity_prices_silver columns that will be joined into wide table to aid with identifying them
electricity_prices_silver_df = electricity_prices_silver_df.withColumnRenamed('euros_per_mwh', 'electricity_euros_per_mwh').withColumnRenamed('origin_date', 'electricity_origin_date').withColumnRenamed('data_block_id', 'electricity_data_block_id')

# Join wide_silver_df with electricity_prices on wide_silver_df.datetime == electricity_prices_silver_df.electricity_available_datetime
wide_silver_df = wide_silver_df.join(electricity_prices_silver_df, wide_silver_df.datetime == electricity_prices_silver_df.electricity_available_datetime, 'left')

# Code can be used to provide a count to show the number rows is still the same and show columns**
#print(wide_silver_df.count())
#wide_silver_df.columns

# Code can be used to show that the 2 data_block_id columns are identical
#wide_silver_df.filter(wide_silver_df['data_block_id'] != wide_silver_df['electricity_data_block_id']).count()

# Drop the data_block_id columns that came from electricity_prices
wide_silver_df = wide_silver_df.drop(col('electricity_data_block_id'))

# Code can be used to show null values in data
#wide_nulls = {col:wide_silver_df.filter(wide_silver_df[col].isNull()).count() for col in wide_silver_df.columns}
#print(wide_nulls)

# Small number of null values, from incomplete electricity price information (2 dates have only 23 predictions while the rest have 24). Nulls are dropped.
wide_silver_df = wide_silver_df.dropna(how='any')

# # Rename gas_prices_silver columns that will be joined into wide table to aid with identifying them
gas_prices_silver_df = gas_prices_silver_df.withColumnRenamed('lowest_price_per_mwh', 'gas_lowest_price_per_mwh').withColumnRenamed('highest_price_per_mwh', 'gas_highest_price_per_mwh').withColumnRenamed('origin_date', 'gas_origin_date').withColumnRenamed('data_block_id', 'gas_data_block_id')

# Join wide_silver with gas_prices on data_block_id, then drop the duplicative gas_data_block_id
wide_silver_df = wide_silver_df.join(gas_prices_silver_df, wide_silver_df.data_block_id == gas_prices_silver_df.gas_data_block_id, 'left')
wide_silver_df = wide_silver_df.drop(col('gas_data_block_id'))

# Code can be used to show there are no nulls
#wide_nulls = {col:wide_silver_df.filter(wide_silver_df[col].isNull()).count() for col in wide_silver_df.columns}
#print(wide_nulls)

# Historical weather currently includes multiple reports 
# from different weather stations at different lat/long locations for some counties.
# Code below averages forecast weather data for the same county and day/time
# to be able to join with wide table efficiently

historical_weather_silver_df.createOrReplaceTempView('temp_historical_weather')

query = '''
SELECT datetime AS historical_weather_datetime, historical_weather_available_datetime,
       data_block_id AS historical_weather_data_block_id, county_name AS historical_weather_county_name, county_id AS historical_weather_county_id, avg(temperature) AS historical_temperature,
       avg(dewpoint) AS historical_dewpoint, avg(rain) AS historical_rain, avg(snowfall) AS historical_snowfall, avg(surface_pressure) AS historical_surface_pressure, avg(cloudcover_high) AS historical_cloudcover_high, avg(cloudcover_mid) AS historical_cloudcover_mid, avg(cloudcover_low) AS historical_cloudcover_low, avg(cloudcover_total) AS historical_cloudcover_total,
       avg(windspeed_10m) AS historical_windspeed_10m, avg(winddirection_10m) AS historical_winddirection_10m,
       avg(shortwave_radiation) AS historical_shortwave_radiation,
       avg(direct_solar_radiation) AS historical_direct_solar_radiation,
       avg(diffuse_radiation) AS historical_diffuse_radiation
  FROM temp_historical_weather
 GROUP BY historical_weather_data_block_id, historical_weather_datetime, historical_weather_available_datetime, historical_weather_county_id, historical_weather_county_name'''

historical_weather_silver_df = spark.sql(query)

# Join wide_silver_df with historical_weather_silver_df on data_block_id == historical_weather_block id and county == historical_weather_county_id and datetime == historical_weather_available_datetime**
wide_silver_df = wide_silver_df.join(historical_weather_silver_df, (wide_silver_df.data_block_id == historical_weather_silver_df.historical_weather_data_block_id) & (wide_silver_df.county == historical_weather_silver_df.historical_weather_county_id) & (wide_silver_df.datetime == historical_weather_silver_df.historical_weather_available_datetime), 'left')


# Multiple forecasts are available for each datetime,
# forecasts which are less than 24 hours of origin_datetime and more than 47 hours
# are dropped to aid in creating wide table

forecast_weather_silver_df = forecast_weather_silver_df.filter('hours_ahead > 23 and hours_ahead <48')
forecast_weather_silver_df.orderBy('origin_datetime', 'hours_ahead').show()

# Forecast weather currently includes multiple reports 
# from different weather stations at different lat/long locations for some counties.
# Code below averages forecast weather data for the same county and day/time
# to be able to join with wide table efficiently

forecast_weather_silver_df.createOrReplaceTempView('temp_forecast_weather')
query = '''
SELECT origin_datetime AS forecast_weather_origin_datetime, hours_ahead AS forecast_weather_hours_ahead,    
       forecast_datetime AS forecast_weather_forecast_datetime,
       data_block_id AS forecast_weather_data_block_id, county_name AS forecast_weather_county_name, county_id AS forecast_weather_county_id, avg(temperature) AS forecast_weather_temperature,
       avg(dewpoint) AS forecast_weather_dewpoint,  avg(snowfall) AS forecast_weather_snowfall, avg(cloudcover_high) AS forecast_weather_cloudcover_high, avg(cloudcover_mid) AS forecast_weather_cloudcover_mid, avg(cloudcover_low) AS forecast_weather_cloudcover_low, avg(cloudcover_total) AS forecast_weather_cloudcover_total,
       avg(direct_solar_radiation) AS forecast_weather_direct_solar_radiation,
       avg(surface_solar_radiation_downwards) AS forecast_weather_surface_solar_radiation_downwards
  FROM temp_forecast_weather
  GROUP BY forecast_weather_data_block_id, forecast_weather_origin_datetime, forecast_weather_county_id, forecast_weather_county_name, forecast_weather_hours_ahead, forecast_weather_forecast_datetime'''

forecast_weather_silver_df = spark.sql(query)

# Join wide_silver_df with forecast_weather_silver_df on data_block_id == forecast_weather_block id and county == forecast_weather_county_id and datetime == forecast_weather_forecast_datetime**
wide_silver_df = wide_silver_df.join(forecast_weather_silver_df, (wide_silver_df.data_block_id == forecast_weather_silver_df.forecast_weather_data_block_id) & (wide_silver_df.county == forecast_weather_silver_df.forecast_weather_county_id) & (wide_silver_df.datetime == forecast_weather_silver_df.forecast_weather_forecast_datetime), 'left')

# Drop unnecessary columns

drop_columns = ['client_is_business', 'client_product_type', 'client_county','client_date',
 'client_data_block_id', 'electricity_effective_datetime','electricity_origin_date','electricity_available_datetime', 'gas_origin_date', 'gas_effective_date',
 'gas_available_date','historical_weather_data_block_id', 'historical_weather_county_name',
 'historical_weather_county_id', 'forecast_weather_data_block_id','historical_weather_available_datetime', 'historical_weather_datetime','forecast_weather_county_name', 'forecast_weather_county_id', 'forecast_weather_origin_datetime','forecast_weather_forecast_datetime',
 'forecast_weather_hours_ahead', 'electricity_available_datetime']

wide_silver_df = wide_silver_df.drop(*drop_columns)

# Code will try to save the DataFrame as a delta table, but if the table already exists, it will use upserts to merge in changes
# Delta Lake 2.30 does not permit using "NOT MATCHED BY SOURCE" in SQL so PySpark is used

try:
    wide_silver_df.write.format('delta').mode('error').option('mergeSchema', 'true').partitionBy('data_block_id').save(f'gs://{TABLES_BUCKET}/enefit_gold_table')
except:
    delta_table = DeltaTable.forPath(spark, f'gs://{TABLES_BUCKET}/enefit_gold_table')

    # Define the merge columns and non-merge columns
    merge_columns = ['datetime', 'county', 'product_type', 'is_business', 'is_consumption']
    non_merge_columns = [col for col in wide_silver_df.columns if col not in merge_columns]

    # Create expressions for the update and insert operations
    update_expr = {col: f"source.{col}" for col in non_merge_columns}
    insert_expr = {col: f"source.{col}" for col in wide_silver_df.columns}

    # Create the condition for when records match based on the merge columns
    on_clause = ' AND '.join([f"destination.{col} = source.{col}" for col in merge_columns])

    # Create the update condition string
    update_conditions = ' OR '.join([f"destination.{col} != source.{col}" for col in non_merge_columns])

    # Perform the merge operation
    delta_table.alias("destination").merge(
        wide_silver_df.alias("source"),
        on_clause
    ).whenMatchedUpdate(
        condition=update_conditions,
        set=update_expr
    ).whenNotMatchedInsert(
        values=insert_expr
    ).whenNotMatchedBySourceDelete().execute()