from tests.test_api_handler import ApiHandlerLambdaTestCase


class TestSuccess(ApiHandlerLambdaTestCase):

    def test_success(self):
        response = self.HANDLER.handle_request({}, {})
        self.assertIn("statusCode", response)  # Just checks if statusCode exists

