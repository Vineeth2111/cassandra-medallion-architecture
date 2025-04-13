import pandas as pd
import uuid
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# ✅ Load token info from downloaded JSON
with open('linkedin-token.json') as f:
    secrets = json.load(f)

ASTRA_CLIENT_ID = secrets["clientId"]
ASTRA_CLIENT_SECRET = secrets["secret"]

cloud_config = {
    'secure_connect_bundle': './secure-connect-linkedin.zip'
}

auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('linkedin')

print("✅ Successfully connected to Astra DB!")

