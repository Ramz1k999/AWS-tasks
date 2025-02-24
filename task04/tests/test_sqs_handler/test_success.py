from tests.test_sqs_handler import SqsHandlerLambdaTestCase


class TestSuccess(SqsHandlerLambdaTestCase):

    def test_success(self):
        response = self.HANDLER.handle_request({}, {})
        self.assertIn("statusCode", response)  # Just checks if statusCode exists

