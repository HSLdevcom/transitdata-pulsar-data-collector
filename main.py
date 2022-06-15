import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

### SECRETS / ENV VARIABLES ###

TENANT_ID=os.getenv('TENANT_ID')
CLIENT_ID=os.getenv('CLIENT_ID')
CLIENT_SECRET=os.getenv('CLIENT_SECRET')
ADMIN_URL=os.getenv('ADMIN_URL')
NAMESPACE=os.getenv('NAMESPACE')
PULSAR_PROXY_RESOURCE_ID=os.getenv('PULSAR_PROXY_RESOURCE_ID')
ACCESS_TOKEN_PATH = os.getenv('ACCESS_TOKEN_PATH')

### SECRETS / ENV VARIABLES ###


METRIC_MSG_RATE_IN = "Msg Rate In"
METRIC_MSG_RATE_OUT = "Msg Rate Out"
METRIC_STORAGE_SIZE = "Storage Size"

TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN = [
    "hfp-mqtt-raw/v2",
    "hfp-mqtt-raw-deduplicated/v2",
    "hfp/v2",
    "gtfs-rt/feedmessage-vehicleposition",
    "metro-ats-mqtt-raw/metro-estimate",
    "metro-ats-mqtt-raw-deduplicated/metro-estimate",
    "source-metro-ats/metro-estimate",
    "source-pt-roi/arrival",
    "source-pt-roi/departure",
    "internal-messages/pubtrans-stop-estimate",
    "internal-messages/feedmessage-tripupdate",
    "gtfs-rt/feedmessage-tripupdate",
    "internal-messages/stop-cancellation"
]

TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT = [
    "gtfs-rt/feedmessage-vehicleposition",
    "gtfs-rt/feedmessage-tripupdate"
]

TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE = [
    "hfp/v2"
]


def main():
    topic_data_collection = {}
    # Merge all topic name lists as a single array
    collect_data_from_topics_list = list(set(TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN + TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT + TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE))

    for topic_name in collect_data_from_topics_list:
        topic_data = collect_data_from_topic(topic_name)
        if topic_data != None:
            topic_data_collection[topic_name] = topic_data

    if bool(topic_data_collection):
        send_metrics_into_azure(topic_data_collection)
    else:
        print(f'Not sending metrics, topic_data_collection was empty.')

def collect_data_from_topic(topic_name):
    pulsar_url = f'{ADMIN_URL}/admin/v2/persistent/{NAMESPACE}/{topic_name}/stats'
    try:
        r = requests.get(url=pulsar_url)
        topic_data = r.json()
        # print(f'Stats of topic {topic_data}:')
        # print(f'{topic_data["msgRateIn"]}')
        # print(f'{topic_data["msgRateOut"]}')
        # print(f'{topic_data["storageSize"]}')
        return topic_data
    except Exception as e:
        print(f'Failed to send a POST request to {pulsar_url}. Is pulsar running and accepting requests?')

def send_metrics_into_azure(topic_data_collection):
    send_pulsar_topic_metric_into_azure(METRIC_MSG_RATE_IN, "msgRateIn", topic_data_collection, TOPIC_NAMES_TO_COLLECT_MSG_RATE_IN)
    send_pulsar_topic_metric_into_azure(METRIC_MSG_RATE_OUT, "msgRateOut", topic_data_collection, TOPIC_NAMES_TO_COLLECT_MSG_RATE_OUT)
    send_pulsar_topic_metric_into_azure(METRIC_STORAGE_SIZE, "storageSize", topic_data_collection, TOPIC_NAMES_TO_COLLECT_STORAGE_SIZE)
    print(f'Pulsar metrics sent: {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}')

def send_pulsar_topic_metric_into_azure(
        log_analytics_metric_name,
        topic_data_metric_name,
        topic_data_collection,
        topic_names_to_collect
):
    """
    Send custom metrics into azure. Documentation for the required format can be found from here:
    https://docs.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-custom-overview

    # Subject: which Azure resource ID the custom metric is reported for.
    # Is included in the URL of the API call

    # Region: must be the same for the resource ID and for log analytics
    # Is included in the URL of the API call
    """

    # Azure wants time in UTC ISO 8601 format
    time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    series_array = get_series_array(topic_data_collection, topic_data_metric_name, topic_names_to_collect)

    custom_metric_object = {
        # Time (timestamp): Date and time at which the metric is measured or collected
        "time": time,
        "data": {
            "baseData": {
                # Metric (name): name of the metric
                "metric": log_analytics_metric_name,
                # Namespace: Categorize or group similar metrics together
                "namespace": "Pulsar",
                # Dimension (dimNames): Metric has a single dimension
                "dimNames": [
                  "Topic"
                ],
                # Series: data for each monitored topic
                "series": series_array
            }
        }
    }

    custom_metric_json = json.dumps(custom_metric_object)

    send_custom_metrics_request(custom_metric_json, 3)

def get_series_array(topic_data_collection, topic_data_metric_name, topic_names_to_collect):
    series_array = []
    for topic_name in topic_names_to_collect:
        dimValue = {
            "dimValues": [
                topic_name
            ],
            "sum": round(topic_data_collection[topic_name][topic_data_metric_name], 1),
            "count": 1
        }
        series_array.append(dimValue)
    return series_array

def send_custom_metrics_request(custom_metric_json, attempts_remaining):
    # Exit if number of attempts reaches zero.
    if attempts_remaining == 0:
        return
    attempts_remaining = attempts_remaining - 1

    make_sure_access_token_file_exists()
    # Create access_token.txt file, if it does not exist
    f = open(ACCESS_TOKEN_PATH, "r")
    existing_access_token = f.read()
    f.close()

    pulsar_proxy_resource_id = PULSAR_PROXY_RESOURCE_ID
    request_url = f'https://westeurope.monitoring.azure.com/{pulsar_proxy_resource_id}/metrics'
    headers = {'Content-type': 'application/json', 'Authorization': f'Bearer {existing_access_token}'}
    response = requests.post(request_url, data=custom_metric_json, headers=headers)

    # Return if response is successful
    if response.status_code == 200:
        return

    # Try catch because json.loads(response.text) might not be available
    try:
        response_dict = json.loads(response.text)
        if response_dict['Error']['Code'] == 'TokenExpired':
            print("Currently stored access token has expired, getting a new access token.")
            request_new_access_token_and_write_it_on_disk()
            send_custom_metrics_request(custom_metric_json, attempts_remaining)
        elif response_dict['Error']['Code'] == 'InvalidToken':
            print("Currently stored access token is invalid, getting a new access token.")
            request_new_access_token_and_write_it_on_disk()
            send_custom_metrics_request(custom_metric_json, attempts_remaining)
        else:
            print(f'Request failed for an unknown reason, response: {response_dict}.')
    except Exception as e:
        print(f'Request failed for an unknown reason, response: {response}.')

def make_sure_access_token_file_exists():
    try:
        f = open(ACCESS_TOKEN_PATH, "r")
        f.close()
    except Exception as e:
        # Create access_token.txt file, if it does not exist
        f = open(ACCESS_TOKEN_PATH, "x")
        f.close()

def request_new_access_token_and_write_it_on_disk():
    request_url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/token'

    request_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "resource": "https://monitoring.azure.com/"
    }

    response = requests.post(request_url, data=request_data)
    response_dict = json.loads(response.text)
    new_access_token = response_dict['access_token']

    print("Saving Access token on disk........")
    f = open(ACCESS_TOKEN_PATH, "w")
    f.write(new_access_token)
    f.close()
    print("........Access token saved on disk")

if __name__ == '__main__':
    main()
