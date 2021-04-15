import requests
from typing import List

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


def fetch_comments(limit: int) -> List[dict]:
    """
    Fetch {limit} recent comments from reddit using the push shift API.
    """

    url = f"https://api.pushshift.io/reddit/search/comment/?size={limit}"
    resp = requests.get(url)
    comments = resp.json()['data']
    return [extract_data(comment) for comment in comments]


def handle_timer(request):
    """
    HTTP entrypoint for the GCP Cloud Function. Called by Cloud Scheduler.
    """

    # Stuffs everything into a dict so that it's a valid response.
    return {
            "comments": fetch_comments(2)
    }

if __name__ == "__main__":
    print("Testing producer...")
    print(handle_timer(None))