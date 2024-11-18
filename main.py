# Databricks notebook source
# Read data from the table into a DataFrame
df = spark.table("my_database.serious_injury_outcome_indicators_2000_2022")

from pyspark.sql.functions import year, when

# Assume the 'Period' column is of date or timestamp type
df_transformed = df.withColumn("year", year("Period")) \
                   .withColumn("before_2010", when(year("Period") < 2010, 1).otherwise(0))

from pyspark.sql.functions import sum as sum_

# Filter the DataFrame where Severity is 'FATAL'
df_fatal = df.filter(df.Severity == 'Fatal')

# Group by 'Age' and compute the total 'Data_value'
df_aggregated = df_fatal.groupBy("Age").agg(sum_("Data_value").alias("total_cases"))

# Order by 'total_cases' in descending order
df_result = df_aggregated.orderBy(df_aggregated.total_cases.desc())

# Show the result
df_result.show()

# Save the DataFrame as a new table in your database
df_result.write.mode("overwrite").saveAsTable("my_database.injury_outcome_summary")

