from google.cloud import bigquery

def get_project_variables():
    return 'propertyanalytics-404010', 'sheets_check', 'upload_duplicates', 'duplicate_group_id'

def get_max_duplicate_group_id(project_id, dataset_id, table_id, column_name):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    sql_query = f"SELECT MAX(CAST({column_name} AS NUMERIC)) AS max_value FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(sql_query)
    result = next(iter(query_job.result()))
    
    # Convert Decimal to int or float, depending on the nature of your data
    max_value = int(result['max_value'])
    
    return max_value

def get_duplicates_to_assign(project_id):
    client = bigquery.Client(project=project_id)
    dataset_id = 'sheets_check'
    table_id = 'upload_new_duplicate_pairs'
    sql_query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(sql_query)
    results = query_job.result()
    duplicates_to_assign = [tuple(row.values()) for row in results]
    return duplicates_to_assign

def get_existing_duplicates(project_id):
    client = bigquery.Client(project=project_id)
    dataset_id = 'dbt_curated'
    table_id = 'dim_property_duplicates'
    sql_query = f"SELECT duplicate_property_code FROM `{project_id}.{dataset_id}.{table_id}`"
    query_job = client.query(sql_query)
    existing_duplicates = [row[0] for row in query_job.result()]
    return existing_duplicates

def remove_existing_duplicates(duplicates_to_assign, existing_duplicates):
    return [tpl for tpl in duplicates_to_assign if not any(str(element) in tpl for element in existing_duplicates)]

def assign_group_id(duplicates_to_assign, max_value):
    position = 3

    duplicates_to_assign = [tpl[:position-1] + (index +  max_value + 1,) + tpl[position-1:] for index, tpl in enumerate(duplicates_to_assign)]

    duplicates_to_load = []

    for tpl in duplicates_to_assign:
        # Split the tuple at position 2
        position_1_tuple = tpl[:1] + tpl[2:]
        position_2_tuple = tpl[1:2] + tpl[2:]

        # Append the new tuples to the result list
        duplicates_to_load.append(position_1_tuple)
        duplicates_to_load.append(position_2_tuple)

    print(duplicates_to_load)
    
    return duplicates_to_load

def load_to_big_query(project_id, duplicates_to_load):
    
    dataset_id = 'duplicate_processing'
    table_id = 'additional_duplicates'

    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Get the schema of the table
    table = client.get_table(table_ref)
    
    # Ensure the schema matches the tuples
    schema = table.schema
    assert len(schema) == len(duplicates_to_load[0]), "Schema does not match the tuples"

    # Convert tuples to Row objects
    errors = client.insert_rows(table, duplicates_to_load)

    # Load the rows into the table
    if errors:
        print(f"Errors encountered while inserting rows: {errors}")
    else:
        print("Rows successfully inserted into BigQuery table.")



def run_dupe_attribution():
    project_id, dataset_id, table_id, column_name = get_project_variables()
    max_value = get_max_duplicate_group_id(project_id, dataset_id, table_id, column_name)
    duplicates_to_assign = get_duplicates_to_assign(project_id)
    existing_duplicates = get_existing_duplicates(project_id)
    duplicates_to_assign = remove_existing_duplicates(duplicates_to_assign, existing_duplicates)
    if len(duplicates_to_assign) !=0:
        print("values to assign")
        duplicates_to_load = assign_group_id(duplicates_to_assign, max_value)
        load_to_big_query(project_id, duplicates_to_load)
    else:
        print("No values to assign")
