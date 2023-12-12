import logging
from gsheets_check import load_pipeline_with_ranges
import base64
import cProfile
from duplicate_group_attribution import run_dupe_attribution
import os
from dlt.common.runtime.slack import send_slack_message
from dotenv import load_dotenv

def notify_on_completion(hook):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                message = f"Function {func.__name__} completed successfully"
                send_slack_message(hook, message)
                return result
            except Exception as e:
                message = f"Function {func.__name__} failed. Error: {str(e)}"
                send_slack_message(hook, message)
                raise
        return wrapper
    return decorator

load_dotenv() #  utilised to got local env variable 
slack_webhook_url = os.environ.get('SLACK_WEBHOOK_URL')

@notify_on_completion(slack_webhook_url)
def gsheets_check_pipeline(data, context):  
    """Triggered by a Pub/Sub message."""
    logging.info(f"Function triggered by Pub/Sub event: {data}")

    try:
        message = base64.b64decode(data['data']).decode('utf-8')
        logging.info(f"Received message data: {message}")

        load_pipeline_with_ranges()
        run_dupe_attribution()

        logging.info("DBT command executed successfully.")
    except Exception as e:
        logging.error(f"Error executing DBT command: {e}")