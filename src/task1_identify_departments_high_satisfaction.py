from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    # Total count of employees per department
    total_df = df.groupBy("Department").agg(count("*").alias("total_count"))

    # Count of high satisfaction and engagement employees per department
    high_satisfaction_df = df.filter(
        (col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High")
    ).groupBy("Department").agg(count("*").alias("high_satisfaction_count"))

    # Calculate the percentage of high-satisfaction employees
    result_df = total_df.join(high_satisfaction_df, on="Department") \
        .withColumn("Percentage", spark_round((col("high_satisfaction_count") / col("total_count")) * 100, 2)) \
        .filter(col("Percentage") > 5) \
        .select("Department", "Percentage")

    return result_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "input/employee_data.csv"
    output_file = "outputs/task1/Identify_departments.txt"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
