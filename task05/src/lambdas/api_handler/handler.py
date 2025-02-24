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
            # Parse request body
            body = json.loads(event["body"])
            principal_id = body.get("principalId")
            content = body.get("content")

            # Validate request data
            if not isinstance(principal_id, int) or not isinstance(content, dict):
                return {
                    "statusCode": 400,
                    "body": json.dumps({
                        "statusCode": 400,
                        "error": "Invalid input: principalId must be int, content must be an object."})
                }

            # Generate unique event ID
            event_id = str(uuid.uuid4())
            created_at = datetime.utcnow().isoformat() + "Z"

            # Construct event object
            event_item = {
                "id": event_id,
                "principalId": principal_id,
                "createdAt": created_at,
                "body": content
            }

            # Save to DynamoDB
            table.put_item(Item=event_item)
            logger.info(f"Event saved: {event_item}")

            # Return response
            return {
                "statusCode": 201,
                "body": json.dumps({"statusCode": 201, "event": event_item})
            }

        except Exception as e:
            logger.error(f"Error saving event: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "statusCode": 500,
                    "error": "Internal Server Error"})
            }


HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
