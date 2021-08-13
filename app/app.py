from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import requests
import warnings
import os
import logging
import time
import schedule
from datetime import datetime

warnings.filterwarnings("ignore")
warnings.simplefilter("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

ES_USERNAME = os.environ.get("ES_USERNAME")
#  logging.info(ES_USERNAME)
ES_PASSWORD = os.environ.get("ES_PASSWORD")
#  logging.info(ES_PASSWORD)
ES_ENDPOINT = os.environ.get("ES_ENDPOINT")
#  logging.info(ES_ENDPOINT)

JIRA_USERNAME = os.environ.get("JIRA_USERNAME")
#  logging.info(JIRA_USERNAME)
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD")
#  logging.info(JIRA_PASSWORD)
JIRA_ENDPOINT = os.environ.get("JIRA_ENDPOINT")
#  logging.info(JIRA_ENDPOINT)
JIRA_PROJECT = os.environ.get("JIRA_PROJECT")
#  logging.info(JIRA_PROJECT)

JIRA_PARAMS = {"jql-": f"project = {JIRA_PROJECT}", "maxResults": 100, "startAt": 0}

logging.info("connecting to elsasticsearch")
es = Elasticsearch(
    hosts=[ES_ENDPOINT],
    port=9200,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    scheme="https",
    verify_certs=False,
)
logging.info(es.info())


def get_jira_data(insert_type="current"):
    done = False
    issues = []
    current_date = datetime.now()

    while not done:
        logging.info("making a request to JIRA API")
        response = requests.get(
            url=JIRA_ENDPOINT, params=JIRA_PARAMS, auth=(JIRA_USERNAME, JIRA_PASSWORD)
        )
        issues_total = response.json().get("total")
        issues.extend(response.json().get("issues"))

        if len(issues) < issues_total:
            logging.info("need to fetch more issues")
            JIRA_PARAMS["startAt"] = len(issues)
        elif len(issues) == issues_total:
            logging.info(f"fetched {issues_total} issues from JIRA")
            done = True

    logging.info(f"got {len(issues)} to insert into elasticsearch")

    def gen_jira_issues():
        for issue in issues:
            doc = {"_index": "jira", "_id": issue.get("id"), "timestamp": current_date}
            yield {**issue, **doc}

    def gen_jira_issues_timeseries():
        for issue in issues:
            doc = {"_index": "jira-timeseries", "timestamp": current_date}
            yield {**issue, **doc}

    if insert_type == "current":
        es.indices.delete("jira")
        bulk(es, gen_jira_issues())

    if insert_type == "timeseries":
        bulk(es, gen_jira_issues_timeseries())


schedule.every().minute.at(":00").do(get_jira_data, insert_type="current")
schedule.every().day.at("00:00").do(get_jira_data, insert_type="timeseries")
while True:
    schedule.run_pending()
    time.sleep(1)
