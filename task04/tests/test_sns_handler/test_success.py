from tests.test_sns_handler import SnsHandlerLambdaTestCase


class TestSuccess(SnsHandlerLambdaTestCase):

    def test_success(self):
        response = self.HANDLER.handle_request({}, {})
        self.assertIn("statusCode", response)  # Just checks if statusCode exists

