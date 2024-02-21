from pathlib import Path
import os, sys
sys.path.append(str(Path(__file__).parent.parent))
import utils

from dotenv import load_dotenv
load_dotenv(str(Path(__file__).parent.parent.parent / ".envrc"))

def run_query(filename):
    bq = utils.BigQuery(
        project=os.environ["BIGQUERY_PROJECT"],
        dataset=os.environ["BIGQUERY_DATASET"],
        credentials_path=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    )
    with open(filename) as f:
        query = f.read()
    bq.run_query(query, as_pandas=False)

def main():
    folder = Path(__file__).parent
    files = [str(x) for x in folder.glob("*.sql")]
    for fl in sorted(files):
        run_query(fl)


if __name__ == "__main__":
    main()