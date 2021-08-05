from elasticsearch import Elasticsearch
import requests
import warnings
import os
import json
import logging
import time

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

while True:

    done = False
    issues = []

    while not done:
        logging.info("making a request to JIRA API")
        response = requests.get(
            JIRA_ENDPOINT, params=JIRA_PARAMS, auth=(JIRA_USERNAME, JIRA_PASSWORD)
        )
        maxResults = response.json().get("maxResults")
        issues_total = response.json().get("total")
        issues_received = len(response.json().get("issues"))

        issues.extend(response.json().get("issues"))

        if len(issues) < issues_total:
            logging.info("need to fetch more issues")
            JIRA_PARAMS["startAt"] = len(issues)
        elif len(issues) == issues_total:
            logging.info(f"fetched {issues_total} issues from JIRA")
            done = True

    for issue in issues:
        es.index(index="jira", body=issue, id=issue.get("id"))

    logging.info(f"inserted {len(issues)} documents into elasticsearch")

    logging.info("sleeping for 60 seconds")
    time.sleep(60)
