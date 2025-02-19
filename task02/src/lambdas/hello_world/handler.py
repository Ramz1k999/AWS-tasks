import json

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger(__name__)


class HelloWorld(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        http_method = event.get("httpMethod", "UNKNOWN")
        path = event.get("path", "UNKNOWN")

        headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }

        if http_method == "GET" and path == "/hello":
            response_body = {"message": "Hello from Lambda"}
            status_code = 200
        else:
            response_body = {
                "error": "Bad Request",
                "message": f"Invalid request to {path} with method {http_method}"
            }
            status_code = 400

        return {
            "statusCode": status_code,
            "headers": headers,
            "body": json.dumps(response_body)
        }
    

HANDLER = HelloWorld()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
