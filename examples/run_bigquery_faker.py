# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
import os
import warnings

import pandas as pd
import pandas_gbq
from google.cloud import secretmanager
from google.oauth2 import service_account

import airbyte as ab
from airbyte.caches.bigquery import BigQueryCache


warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")


source = ab.get_source(
    "source-faker",
    config={"count": 10, "seed": 0, "parallelism": 1, "always_updated": False},
    install_if_missing=True,
)

# load secrets from GSM using the GCP_GSM_CREDENTIALS env variable
secret_client = secretmanager.SecretManagerServiceClient.from_service_account_info(
    json.loads(os.environ["GCP_GSM_CREDENTIALS"])
)


# to-do: make sure database & account id are read from PY_AIRBYTE_BIGQUERY_CREDS
"""secret = json.loads(
    secret_client.access_secret_version(
        name="projects/dataline-integration-testing/secrets/PY_AIRBYTE_BIGQUERY_CREDS/versions/latest"
    ).payload.data.decode("UTF-8")
)"""

# to-do remove following line after above to-do is done
secret = {
    "project_id": "dataline-integration-testing",
}

# to-do: the path should be read from environment variable
cache = BigQueryCache(
    project_name=secret["project_id"],
    credentials_path="/Users/bindipankhudi/Downloads/dataline-integration-testing-15037634a1d1.json",
)

print(cache.get_sql_alchemy_url())
run_test_code = False

credentials = service_account.Credentials.from_service_account_file(
    "/Users/bindipankhudi/Downloads/dataline-integration-testing-15037634a1d1.json"
)

if run_test_code:
    # Sample DataFrame with products data
    data = {
        "id": [103, 256, 365],
        "make": ["honda", "Fake", "Audii"],
        "model": ["CRV", "X5", "Infinity"],
        "year": [2018, 2019, 2020],
        "price": [25000, 45000, 35000],
        "created_at": [
            "2022-02-01T17:02:19+00:00",
            "2022-02-01T17:02:19+00:00",
            "2022-02-01T17:02:19+00:00",
        ],
        "updated_at": [
            "2022-02-01T17:02:19+00:00",
            "2022-02-01T17:02:19+00:00",
            "2022-02-01T17:02:19+00:00",
        ],
    }

    sales_data = pd.DataFrame(data)
    sales_data["created_at"] = pd.to_datetime(sales_data["created_at"])
    sales_data["updated_at"] = pd.to_datetime(sales_data["updated_at"])

    print(sales_data)

    # Temporary table name
    temp_table_name = "products"

    pandas_gbq.to_gbq(
        dataframe=sales_data,
        destination_table=f"airbyte_raw.{temp_table_name}",
        project_id="dataline-integration-testing",
        if_exists="append",
        credentials=credentials,
    )

else:  # create a cache
    source.check()

    source.select_streams(["products"])
    result = source.read(cache)

    for name, records in result.streams.items():
        print(f"Stream {name}: {len(records)} records")
