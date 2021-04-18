import requests
from typing import List
from google.cloud import spanner

NUMBER_OF_COMMENTS = 2 # Number of comments to fetch per call.
SPANNER_INSTANCE = "reddit-pipeline"
DB_NAME = "reddit-pipeline"

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

    url = f"https://api.pushshift.io/reddit/search/comment/?size={limit}?subreddit={subreddit}"
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
            subreddits.append(row)
    return subreddits


def handle_timer(_):
    """
    HTTP entrypoint for the GCP Cloud Function. Called by Cloud Scheduler.
    """

    # TODO get from spanner to determine the desired subreddits
    spanner_client = spanner.Client()
    instance = spanner_client.instance(SPANNER_INSTANCE)
    database = instance.database(DB_NAME)
    subreddits = get_unique_subreddits(database)
    print(subreddits)

    # TODO Pick a subreddit at random
    # TODO Get comments from that subreddit
    # TODO Get entities for that subreddit ( spanner query)
    # Using some levenshtein or something, determine whether the entity is mentioned.

    # Stuffs everything into a dict so that it's a valid response.
    return {
            "comments": fetch_comments(NUMBER_OF_COMMENTS)
    }

if __name__ == "__main__":
    print("Testing producer...")
    print(handle_timer(None))
