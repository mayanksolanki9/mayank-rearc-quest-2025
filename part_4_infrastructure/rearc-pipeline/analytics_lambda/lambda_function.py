import boto3
import pandas as pd
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TIME_SERIES_KEY = "pr/pr.data.0.Current"

def lambda_handler(event, context):
    """
    This function is triggered by an SQS message that contains S3 event data.
    It loads the BLS time-series data and the newly uploaded population data,
    runs the analytics, and logs the results to CloudWatch.
    """
    s3_client = boto3.client('s3')

    try:
        sqs_body = json.loads(event['Records'][0]['body'])
        s3_event = json.loads(sqs_body['Message'])
        
        bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
        population_key = s3_event['Records'][0]['s3']['object']['key']

        logger.info(f"Processing file: s3://{bucket_name}/{population_key}")
        
        # Load time-series data
        ts_obj = s3_client.get_object(Bucket=bucket_name, Key=TIME_SERIES_KEY)
        df_series = pd.read_csv(ts_obj['Body'], sep='\t')

        # Load population data
        pop_obj = s3_client.get_object(Bucket=bucket_name, Key=population_key)
        population_data = json.loads(pop_obj['Body'].read().decode('utf-8'))
        df_population = pd.DataFrame(population_data['data'])
        
        # Clean the data
        df_series.columns = df_series.columns.str.strip()
        df_series['series_id'] = df_series['series_id'].str.strip()
        df_series['period'] = df_series['period'].str.strip()
        df_series['value'] = pd.to_numeric(df_series['value'], errors='coerce')
        df_series.dropna(subset=['value'], inplace=True)
        df_population['Year'] = df_population['Year'].astype(int)

        logger.info("Data cleaning complete.")

        # Run the analytics and log the results
        # Question 1
        pop_filtered = df_population[(df_population['Year'] >= 2013) & (df_population['Year'] <= 2018)]
        pop_mean = pop_filtered['Population'].mean()
        logger.info(f"[Analytics Q1] Mean Population (2013-2018): {pop_mean:,.0f}")

        # Question 2
        yearly_sum = df_series.groupby(['series_id', 'year'])['value'].sum().reset_index()
        max_value_idx = yearly_sum.groupby('series_id')['value'].idxmax()
        best_year_report = yearly_sum.loc[max_value_idx].head()
        logger.info(f"[Analytics Q2] Best Year Report (Top 5):\n{best_year_report.to_string()}")

        # Question 3
        series_filtered = df_series[(df_series['series_id'] == 'PRS30006032') & (df_series['period'] == 'Q01')].copy()
        joined_df = pd.merge(series_filtered, df_population, left_on='year', right_on='Year', how='inner')
        final_report = joined_df[['series_id', 'year', 'period', 'value', 'Population']]
        logger.info(f"[Analytics Q3] Joined Report:\n{final_report.to_string()}")

        return {'statusCode': 200, 'body': json.dumps('Analysis complete!')}

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise e