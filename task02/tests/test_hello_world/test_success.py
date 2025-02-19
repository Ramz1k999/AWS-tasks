from tests.test_hello_world import HelloWorldLambdaTestCase


class TestSuccess(HelloWorldLambdaTestCase):

    def test_success(self):
        event = {
            "httpMethod": "GET",
            "path": "/hello"
        }
        response = self.HANDLER.handle_request(event, dict())

        # Extract statusCode from response
        self.assertEqual(response.get("statusCode"), 200)

        # Check response body content
        expected_body = '{"message": "Hello from Lambda"}'
        self.assertEqual(response.get("body"), expected_body)

