from pyspark.sql.functions import year, when
from pyspark.sql import SparkSession

# Databricks notebook source
# Read data from the table into a DataFrame
spark = SparkSession.builder.appName("InjuryDataAnalysis").getOrCreate()
df = spark.table("my_database.serious_injury_outcome_indicators_2000_2022")

# Assume the 'Period' column is of date or timestamp type
df_transformed = df.withColumn("year", year("Period")) \
                   .withColumn("before_2010", 
                               when(year("Period") < 2010, 1).otherwise(0)
                               )

df.createOrReplaceTempView("injury_data")
# Execute the SQL query
df_result = spark.sql("""
SELECT
    Age,
    SUM(Data_value) AS total_cases
FROM
    injury_data
WHERE
    Severity = 'Fatal'
GROUP BY
    Age
ORDER BY
    total_cases DESC
""")

# Show the result
df_result.show()

# Save the DataFrame as a new table in your database
df_result.write.mode("overwrite").saveAsTable("my_database.injury_outcome_summary")

