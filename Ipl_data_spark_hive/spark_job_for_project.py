from pyspark.sql import SparkSession
from datetime import datetime


def process_data(input_file_name):
    # Create a Spark session
    spark = SparkSession.builder.appName("DataprocOrderProcessing").getOrCreate()

    try:
        # Extract the date part from the input file name
        date_str = input_file_name.split('_')[1]

        # Convert the date part from 'YYYYMMDD' to '%Y-%m-%d' format
        formatted_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        # strptime (string parse time) from the datetime module to parse the date_str variable

        # Define your GCS bucket and paths
        bucket = "daily-csv-files"
        # Define the path where the files are located for the given date
        input_path = f"gs://{bucket}/Match_{formatted_date}.csv"
        output_path = f"gs://{bucket}/daily_analysis_{formatted_date}.csv"

        # Filter matches where Win_Type is '1' 
        df_filtered = df.filter(col("Win_Type") == 1)

        # Calculate average Win_Margin for each Team_1
        df_avg_win_margin = df_filtered.groupBy("Team_1").agg(avg("Win_Margin").alias("Avg_Win_Margin"))

        # Count number of matches won by each team
        df_match_wins = df.groupBy("Match_Winner").agg(count("Match_Id").alias("Total_Wins"))

        # Join the average win margin with match wins
        df_analysis = df_avg_win_margin.join(df_match_wins, df_avg_win_margin.Team_1 == df_match_wins.Match_Winner, "inner") \
                               .select(df_avg_win_margin.Team_1, "Avg_Win_Margin", "Total_Wins")

        # Show the resulting analysis
        df_analysis.show()
        # Create database if not exists
        spark.sql("CREATE DATABASE IF NOT EXISTS IPl_analysis")

        # Save the DataFrames as Hive tables
        df_filtered.write.mode('overwrite').saveAsTable("IPl_analysis.df_filtered")
        df_avg_win_margin.write.mode('overwrite').saveAsTable("IPl_analysis.df_avg_win_margin")
        df_match_wins.write.mode('overwrite').saveAsTable("IPl_analysis.df_match_wins")
        df_analysis.write.mode('overwrite').saveAsTable("IPl_analysis.df_analysis")
   

        print("Processing completed and tables saved successfully.")

    finally:
        # Stop the SparkSession when done
        spark.stop()





