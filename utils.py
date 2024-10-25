import pandas_gbq
from google.oauth2 import service_account


def init_pandas_gbq():
    credentials = service_account.Credentials.from_service_account_file(
    './climathon-439621-27dd2b3424e4.json'
    )

    pandas_gbq.read_gbq(
        """
        SELECT 'test'
        """, 
        project_id='climathon-439621', 
        credentials=credentials
    )