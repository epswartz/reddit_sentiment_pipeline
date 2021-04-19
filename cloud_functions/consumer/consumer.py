import base64
import json
from google.cloud import spanner
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

SPANNER_INSTANCE = "reddit-pipeline"
DB_NAME = "reddit_pipeline"
TABLE_NAME = "Comments"
COMMENTS_TABLE_COLUMNS = [
    "Id",
    "Created_UTC",
    "Author",
    "SubReddit",
    "EntityName",
    "Sentiment"
]

def comment_to_rows(comment, entities, compound_score):
    """
    Transforms a comment into a list of lists,
    the rows that needs inserting.

    The order and types of values returned must match COMMENTS_TABLE_COLUMNS.
    """
    rows = []
    for entity in entities:
        r = [
            comment["id"],
            comment["created_utc"],
            comment["author"],
            comment["subreddit"],
            entity,
            compound_score
        ]
        rows.append(r)
    return rows

def handle_publish(event, _):
    """
    Pub/Sub Entrypoint.

    1. Determine vader score
    2. Write to Spanner Comments table.

    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    comment,entities = json.loads(pubsub_message)

    vader = SentimentIntensityAnalyzer()
    compound_sentiment = vader.polarity_scores(comment["body"])['compound']

    comment_rows = comment_to_rows(comment, entities, compound_sentiment)

    spanner_client = spanner.Client()
    instance = spanner_client.instance(SPANNER_INSTANCE)
    database = instance.database(DB_NAME)
    database.run_in_transaction(
        insert_comment,
        comment_rows=comment_rows
    )

def insert_comment(transaction, comment_rows) -> None:
    """
    Insert comment into spanner.
    """
    transaction.insert(
        TABLE_NAME,
        columns = COMMENTS_TABLE_COLUMNS,
        values = comment_rows
    )
