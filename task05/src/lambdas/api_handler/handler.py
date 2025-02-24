import json
import uuid
import boto3
import logging
from datetime import datetime

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger(__name__)
# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Events")

class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        """
        Explain incoming event here
        """

        try:
            body = json.loads(event.get("body", "{}"))  # Ensure event body is parsed
            principal_id = body.get("principalId")
            content = body.get("content")

            # Validate input
            if not principal_id or not content:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Missing principalId or content"})
                }

            # Create event object
            new_event = {
                "id": str(uuid.uuid4()),
                "principalId": int(principal_id),
                "createdAt": datetime.utcnow().isoformat() + "Z",
                "body": content
            }

            # Save to DynamoDB
            table.put_item(Item=new_event)

            return {
                "statusCode": 201,
                "body": json.dumps({"event": new_event})
            }

        except Exception as e:
            print(f"Error: {str(e)}")  # Logs error to CloudWatch
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Internal Server Error"})
            }


HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
