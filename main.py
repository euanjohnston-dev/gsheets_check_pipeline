import logging
from gsheets_check import load_pipeline_with_ranges
import base64
from slack_messaging import pipeline_success, pipeline_failure


def gsheets_check_pipeline(data, context): 
    """Triggered by a Pub/Sub message."""
    logging.info(f"Function triggered by Pub/Sub event: {data}")

    try:
        message = base64.b64decode(data['data']).decode('utf-8')
        logging.info(f"Received message data: {message}")
       
        load_pipeline_with_ranges()
        pipeline_success()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        pipeline_failure(e)
        logging.error(f"Error executing DBT command: {e}")
