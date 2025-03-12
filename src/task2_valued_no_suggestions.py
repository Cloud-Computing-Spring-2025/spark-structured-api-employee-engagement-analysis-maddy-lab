from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_valued_no_suggestions(df):
    # Total number of employees
    total_employees = df.count()

    # Filter employees who feel valued but haven't provided suggestions
    valued_no_suggestions_df = df.filter(
        (col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False)
    )

    # Number of such employees
    valued_no_suggestions_count = valued_no_suggestions_df.count()

    # Calculate proportion
    proportion = round((valued_no_suggestions_count / total_employees) * 100, 2)

    return valued_no_suggestions_count, proportion

def write_output(number, proportion, output_path):
    with open(output_path, 'w') as f:
        f.write(f"Number of Employees Feeling Valued without Suggestions: {number}\n")
        f.write(f"Proportion: {proportion}%\n")

def main():
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "input/employee_data.csv"
    output_file = "outputs/task2/valued_no_suggestions.txt"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 2
    number, proportion = identify_valued_no_suggestions(df)
    
    # Write the result to a text file
    write_output(number, proportion, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
