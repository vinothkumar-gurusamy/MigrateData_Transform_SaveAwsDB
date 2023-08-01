import json
import requests

def lambda_handler(event, context):
    message = {'text': 'server down'}
    url = 'https://hooks.slack.com/services/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'
    try:
        requests.post(url, json=message)
        print("Alert Sent To Slack Successfully..")
    except:
        print("No Alert Sent To Slack..")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
