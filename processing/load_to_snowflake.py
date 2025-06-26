import subprocess
import os
import yaml # type: ignore
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

#Get Database, Schema, and Stage from environment variables
db = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
stage = os.getenv("SNOWFLAKE_STAGE")

base_path = "/Users/shivacharan/retail_sales_analytics/data/processed/"
latest_folder = sorted(
    [f for f in os.listdir(base_path) if f.startswith("run_")],
    reverse=True
)[0]


local_folder = os.path.join(base_path, latest_folder)

#local_folder = "Users/shivacharan/retail_sales_analytics/data/processed/"
today = datetime.today().strftime("%Y-%m-%d")
stage_path = f"{db}.{schema}.{stage}/{today}"
final_file_path = f"{local_folder}/retail_sales_data.parquet"


# Load yaml config
with open("/Users/shivacharan/retail_sales_analytics/config/sales_config.yaml") as f:
    config = yaml.safe_load(f)


# Rename part file to a fixed name
for file in os.listdir(local_folder):
    if file.startswith("part-") and file.endswith(".parquet"):
        os.rename(os.path.join(local_folder, file), final_file_path)
        break


user=config["snowflake"]["user"]
account=config["snowflake"]["account"]
password =  os.getenv("SNOWSQL_PWD")
# Upload to Snowflake internal stage
put_cmd = [
    "snowsql",
    "-a", account,
    "-u", user,
    "-q", f"PUT file://{final_file_path} @{stage_path} AUTO_COMPRESS=TRUE;"
]

print("▶️ Uploading to Snowflake stage...")
try:
    subprocess.run(put_cmd, check=True)
    print("✅ PUT command ran successfully.")

except subprocess.CalledProcessError as e:
    print("❌ PUT command failed with error:")
    print(e)