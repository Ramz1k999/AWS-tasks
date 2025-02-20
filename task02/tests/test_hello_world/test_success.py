from tests.test_hello_world import HelloWorldLambdaTestCase


class TestSuccess(HelloWorldLambdaTestCase):

    def test_success(self):
        response = self.HANDLER.handle_request({}, {})
        self.assertIn("statusCode", response)  # Just checks if statusCode exists

