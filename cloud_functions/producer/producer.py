from typing import List
import json
from random import choice
from time import sleep

import requests
from google.cloud import spanner, pubsub_v1

NUMBER_OF_COMMENTS = 2 # Number of comments to fetch per call.

# Spanner Config
SPANNER_INSTANCE = "reddit-pipeline"
DB_NAME = "reddit-pipeline"

# Pub/Sub Config
PROJECT_ID = "proj4-310202"
TOPIC_ID = "reddit-pipeline"

def extract_data(comment: dict) -> dict:
    """
    Extract wanted fields from a push shift API comment object.
    """
    result = {
      "created_utc": comment["created_utc"],
      "body": comment["body"],
      "author": comment["author"],
      "subreddit": comment["subreddit"],
    }
    return result


def fetch_comments(limit: int, subreddit: str) -> List[dict]:
    """
    Fetch {limit} recent comments from reddit using the push shift API.
    Inputs:
        limit: how many comments to get
        subreddit: which subreddit to get them from
    Returns:
        List of dict, each dict is a comment created by extract_data(comment).
    """

    url = f"https://api.pushshift.io/reddit/search/comment/?size={limit}&subreddit={subreddit}"
    resp = requests.get(url)
    comments = resp.json()['data']
    return [extract_data(comment) for comment in comments]


def get_unique_subreddits(database):
    """
    Given the entities DB, return all the subreddits.
    """
    get_all_reddits_sql = "SELECT DISTINCT SubReddit FROM Entities"
    subreddits = []
    with database.snapshot() as snapshot:
        result = snapshot.execute_sql(get_all_reddits_sql)
        for row in result:
            subreddits.append(row[0])
    return subreddits

def get_all_entities_for_subreddit(database, subreddit):
    """
    Get all entities for a given subreddit from spanner.
    """
    get_all_entities_for_reddit_sql = f'SELECT DISTINCT EntityName FROM Entities WHERE SubReddit = "{subreddit}"'
    entities = []
    with database.snapshot() as snapshot:
        result = snapshot.execute_sql(get_all_entities_for_reddit_sql)
        for row in result:
            entities.append(row[0])
    return entities


def entity_filter(comments, entities):
    """
    Given a list of comments and entity names, return comments which mention the entities.

    For now this is a simple lookup - room for much improvement here, anything from levenshtein to an RNN could work.
    """

    filtered = []
    for comment in comments:
        for entity in entities:
            if entity in comment["body"]:
                filtered.append(comment)
    return filtered


def handle_timer(_):
    """
    HTTP entrypoint for the GCP Cloud Function. Called by Cloud Scheduler.

    1. Query Spanner for entities and subreddits of interest
    2. Query pushshift for comments
    3. Filter to only comments which mention the entity
    4. Publish each comment to Pub/Sub
    """

    # Get from spanner to determine the desired subreddits
    spanner_client = spanner.Client()
    instance = spanner_client.instance(SPANNER_INSTANCE)
    database = instance.database(DB_NAME)
    all_subreddits = get_unique_subreddits(database)

    # Pick a subreddit at random
    # This is random because it will be uniform over time, but can still be stateless.
    subreddit = choice(all_subreddits)

    # Get comments from that subreddit
    comments = fetch_comments(NUMBER_OF_COMMENTS, subreddit)

    entities = get_all_entities_for_subreddit(database, subreddit)

    # Filter only for comments which mention an entity
    comments = entity_filter(comments, entities)

    # Publish relevant comments to Cloud Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    futures = dict()
    i = 0
    def create_callback(_, i):
        def callback(_):
            futures.pop(i)
        return callback

    for comment in comments:
        data = json.dumps(comment).encode("utf-8")
        future = publisher.publish(topic_path, data)
        futures[i] = future
        future.add_done_callback(create_callback(future, i))
        i += 1

    # Wait for all futures to finish
    while futures:
        sleep(1)


    # Stuffs everything into a dict so that it's a valid response.
    return {
        "published_count": NUMBER_OF_COMMENTS
    }


if __name__ == "__main__":
    print("Testing producer...")
    print(handle_timer(None))
