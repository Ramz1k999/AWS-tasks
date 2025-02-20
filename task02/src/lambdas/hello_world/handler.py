import json

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger(__name__)


class HelloWorld(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        http_method = event.get("httpMethod", "")
        path = event.get("path", "")

        if http_method == "GET" and path == "/hello":
            status_code = 200
            response_body = {
                "statusCode": status_code,
                "message": "Hello from Lambda"
            }
        else:
            status_code = 400
            response_body = {
                "statusCode": status_code,
                "message": f"Bad Request.Invalid request to {path} with method {http_method}"
            }

        return {
            "statusCode": status_code,
            "body": json.dumps(response_body)
        }
    

HANDLER = HelloWorld()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
