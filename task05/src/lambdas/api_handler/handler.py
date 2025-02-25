import json
import os
import uuid
import boto3
import logging
from datetime import datetime

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger(__name__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table_name = os.environ['target_table']
table = dynamodb.Table(table_name)

class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):

        try:
            body = json.loads(event["body"])
            principal_id = body.get('principalId')
            content = body.get('content')

            # Validation
            if not isinstance(principal_id, int) or not isinstance(content, dict):
                return {
                    "statusCode": 400,
                    "message": "Invalid input. 'principalId' must be an integer and 'content' must be a map."
                }

            # Generate event data
            event_id = str(uuid.uuid4())
            created_at = datetime.utcnow().isoformat() + 'Z'

            table.put_item(Item={
                "id": event_id,
                "principalId": principal_id,
                "createdAt": created_at,
                "body": content
            })

            return {
                "statusCode": 201,
                "event": {
                    "id": event_id,
                    "principalId": principal_id,
                    "createdAt": created_at,
                    "body": content
                }
            }

        except Exception as e:
            print(f"Error: {str(e)}")
            return {
                "statusCode": 500,
                "message": "Internal Server Error"
            }


HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
