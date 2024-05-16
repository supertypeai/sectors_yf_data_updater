import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from neon_connector.neon_connector import NeonConnector

from usyfdataupdater import USYFDataUpdater

# In GCF, the main function should accept a request object
# def main(request):
#     request_dict = request.get_json()

def main(request_dict):
    target_table = request_dict.get("target_table", "")
    batch_size = request_dict.get("batch_size", 100)
    batch_num = request_dict.get("batch_num", "")

    if not target_table or not batch_num:
        return "Missing required parameters: target_table and batch_num", 400
    
    load_dotenv()
    connection_string = os.getenv('NEON_DATABASE_URL')
    neon_connector = NeonConnector(connection_string)

    updater = USYFDataUpdater()
    updater.upsert_data_to_db(neon_connector, target_table, batch_size, batch_num)

    return f"Successfully upserted {target_table} table. The following data weren't updated due to errors: {updater.unadded_data}"

if __name__ == "__main__":
    # example request
    main({"target_table": "daily_data", "batch_num": 1, "batch_size":5})