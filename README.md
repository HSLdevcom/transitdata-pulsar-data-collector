# Transitdata Pulsar Data Collector

Collects Pulsar topic data and sends it to Azure Monitor so that alerts can monitor the data and alert when needed.

### How to run locally?

Have `.env` file at the project directory containing all of the secret values (you can get secrets from a pulsar-proxy VM from pulsar-dev resource group)

Run
```
python3 main.py
```

### How to test sending custom metrics manually to Azure Monitor?

If you need to send new custom metrics to Azure Monitor,
you can firstly test sending by editing
`custom_metric_example.json` and running:
```
curl -X POST https://<azureRegion>.monitoring.azure.com/<resourceId>/metrics -H "Content-Type: application/json" -H "Authorization: Bearer <AccessToken>" -d @custom_metric_example.json
```
Notes:
- Edit what you need in `custom_metric_example.json` (at least the timestamp)
- You need a fresh `access token` for this command, you can get it by running `main.py` locally (see access_token.txt file)
