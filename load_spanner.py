from typing import List

from google.cloud import spanner
from tqdm import tqdm
import typer

DB_NAME = "reddit-pipeline"
TABLE_NAME = "Entities"

def load_entities(transaction, entities = List[List[str]]) -> None:
    """
    Bulk insert entities.
    """
    transaction.insert(
        TABLE_NAME,
        columns=["EntityName", "SubReddit"],
        values=entities
    )

# Main CLI entrypoint
def main(
    csv_file: str = typer.Argument(..., help="CSV file containing entities of interest"),
    instance_id: str = typer.Argument(..., help="Spanner instance id"),
    batch_size: int = typer.Argument(5000, help="Number of entities to upload to spanner per batch insert")
):
    """
    Reads CSV file and bulk inserts all the entities into spanner, batch_size entities at a time.
    """

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(DB_NAME)

    with open(csv_file, 'r') as f:
        lines = f.readlines()
    records = [line.strip().split(",") for line in lines[1:]]
    assert len(records) > 0 # Need at least one entity.

    for i in tqdm(range(len(records)//batch_size + 1), "Uploading Entity Batches"):
        database.run_in_transaction(
            load_entities,
            entities=records[i*batch_size:(i+1)*batch_size]
        )

if __name__ == "__main__":
    typer.run(main)
    