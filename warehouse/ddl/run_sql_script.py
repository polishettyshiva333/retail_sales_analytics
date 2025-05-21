import os
import snowflake.connector
import yaml
from dotenv import load_dotenv
load_dotenv()  # This will read .env and set the env vars


# Load YAML config
with open("config/sales_config.yaml") as f:
    config = yaml.safe_load(f)

# Inject password from env
config["snowflake"]["password"] = os.getenv("SNOWFLAKE_PASSWORD")

# Check for missing password
if not config["snowflake"]["password"]:
    raise ValueError("Missing SNOWFLAKE_PASSWORD environment variable")

# Establish connection
conn = snowflake.connector.connect(
    user=config["snowflake"]["user"],
    password=config["snowflake"]["password"],
    account=config["snowflake"]["account"],
    role=config["snowflake"]["role"],
    warehouse=config["snowflake"]["warehouse"]
)

cursor = conn.cursor()

# Load sql script
with open('warehouse/ddl/init_schema.sql', 'r') as file:
    sql_script = file.read()

# Split and execute each statement
for stmt in sql_script.strip().split(';'):
    stmt = stmt.strip()
    if not stmt or stmt.startswith('--') or stmt.startswith('/*'):
        continue 
    cursor.execute(stmt)


# Cleanup
cursor.close()
conn.close()