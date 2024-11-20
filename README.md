# data_pipeline_w-_databricks
[![CI](https://github.com/aoaow/data_pipeline_w-_databricks/actions/workflows/cicd.yml/badge.svg)](https://github.com/aoaow/data_pipeline_w-_databricks/actions/workflows/cicd.yml)

---


## **Project Overview**

This project demonstrates the creation of a data pipeline using Databricks and Spark SQL. The pipeline reads data from a Databricks table containing serious injury outcome indicators from 2000 to 2022, performs data transformations and aggregations using Spark SQL, and writes the results to a new table for further analysis.

![Successful Implementation in Databricks](screenshot.png)
## **Features**

- **Data Source**: Databricks table `my_database.serious_injury_outcome_indicators_2000_2022`.
- **Data Transformation**: Add a new column `year` that the injury case took place and a boolean column indicating whether the case happened before 2010 or not.
- **Data Processing**: Count the total case number in each severity level.
- **Data Sink**: The aggregated results are saved to a new Databricks table `my_database.injury_outcome_summary`.

## **Dependencies**

- **Spark SQL**: For data processing with Spark.
- **Databricks CLI**: For interacting with Databricks from the command line (if automating deployment).

## **Contributing**

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## **License**

This project is licensed under the MIT License.
