import aws_cdk as core
import aws_cdk.assertions as assertions

from analyze_sec_aws.analyze_sec_aws_stack import AnalyzeSecAwsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in analyze_sec_aws/analyze_sec_aws_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AnalyzeSecAwsStack(app, "analyze-sec-aws")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
