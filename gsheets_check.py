from typing import Sequence
import dlt
from google_sheets import google_spreadsheet

def load_pipeline_with_ranges():

    spreadsheet_url_or_id = "1ft6plZYbFxscYRwNLCHLz5YFTnzgnBIqRrtVbvLYvU0" 
    range_names = ["upload_duplicates", "upload_distinct", "upload_new_duplicate_pairs"] 

    pipeline = dlt.pipeline(
        pipeline_name="google_sheets_pipeline",
        destination='bigquery',
        full_refresh=False, 
        staging='filesystem', 
        dataset_name="sheets_check",
    )

    data = google_spreadsheet(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        range_names=range_names,
        get_sheets=False,
        get_named_ranges=False,
    )

    info = pipeline.run(data, write_disposition="append")
    print(info)


def gsheets_check_pipeline():
    load_pipeline_with_ranges()
