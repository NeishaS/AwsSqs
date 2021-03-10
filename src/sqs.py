import boto3
import json

QUEUE_NAME = 'MyTest-SQS-Queue'
QUEUE_NAME_URL = 'https://us-east-2.queue.amazonaws.com/585973949575/MyTest-SQS-Queue'
FIFO_QUEUE_NAME = 'MyTestQueue.fifo'
FIFO_QUEUE_NAME_URL = 'https://sqs.us-east-2.amazonaws.com/585973949575/MyTestQueue.fifo'
DEAD_LETTER_QUEUE_NAME = 'Dead-Letter-Queue-for-Main'
DEAD_LETTER_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/585973949575/Dead-Letter-Queue-for-Main'
MAIN_QUEUE = 'Main-Queue'
MAIN_QUEUE_URL = 'https://us-east-2.queue.amazonaws.com/585973949575/Main-Queue'


def sqs_client():
    sqs = boto3.client('sqs', region_name='us-east-2')
    """ :type: pyboto3.sqs """
    return sqs


def create_sqs_queue():
    return sqs_client().create_queue(
        QueueName=QUEUE_NAME
    )


def create_fifo_queue():
    return sqs_client().create_queue(
        QueueName=FIFO_QUEUE_NAME,
        Attributes=dict(FifoQueue='true')
    )


def create_queue_for_dead_letter():
    return sqs_client().create_queue(
        QueueName=DEAD_LETTER_QUEUE_NAME
    )


def create_main_queue_for_dead_letter():
    redrive_policy = {
        "deadLetterTargetArn": "arn:aws:sqs:us-east-2:585973949575:Dead-Letter-Queue-for-Main",
        "maxReceiveCount": 3
    }
    return sqs_client().create_queue(
        QueueName=MAIN_QUEUE,
        Attributes={"DelaySeconds": "0",
                    "MaximumMessageSize": "262144",
                    "VisibilityTimeout": "30",
                    "MessageRetentionPeriod": "345680",
                    "ReceiveMessageWaitTimeSeconds": "0",
                    "RedrivePolicy": json.dumps(redrive_policy)}
    )


def find_queue():
    return sqs_client().list_queues(
        QueueNamePrefix='MyTest'
    )


def list_all_queues():
    return sqs_client().list_queues()


def get_queue_attributes():
    return sqs_client().get_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        AttributeNames=['All']
    )


def update_queue_attributes():
    return sqs_client().set_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        Attributes={"MaximumMessageSize": "131072",
                    "VisibilityTimeout": "15"}
    )


def delete_queue():
    return sqs_client().delete_queue(QueueUrl=QUEUE_NAME_URL)


def send_message_to_queue():
    return sqs_client().send_message(
        QueueUrl=MAIN_QUEUE_URL,
        MessageAttributes={
            'Title': {
                'DataType': "String",
                "StringValue": "My Message Title"
            },
            "Author": {
                'DataType': "String",
                "StringValue": "Neisha"
            },
            "Time": {
                "DataType": "String",
                "StringValue": "6"
            }
        },
        MessageBody="This is my first SQS Message!"
    )


def send_batch_messages_to_queue():
    return sqs_client().send_message_batch(
        QueueUrl=MAIN_QUEUE_URL,
        Entries=[
            {"Id": "FirstMessage",
             "MessageBody": "This is the 1st message of batch"},
            {"Id": "SecondMessage",
             "MessageBody": "This is the 2nd message of batch"},
            {"Id": "thirdMessage",
             "MessageBody": "This is the 3rd message of batch"},
            {"Id": "fourthMessage",
             "MessageBody": "This is the 4th message of batch"},
        ]
    )


def poll_queue_for_messages() -> list:
    return sqs_client().receive_message(
        QueueUrl=MAIN_QUEUE_URL,
        MaxNumberOfMessages=10,
        # WaitTimeSeconds=20
    )


def process_message_from_queue():
    queued_messages = poll_queue_for_messages()
    if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
        for message in queued_messages['Messages']:
            print("Processing message " + message['MessageId'] + " with text " + message['Body'])
            delete_message_from_queue(message['ReceiptHandle'])
            # change_message_visibility_timeout(message['ReceiptHandle'])


def delete_message_from_queue(receipt_handle):
    sqs_client().delete_message(
        ReceiptHandle=receipt_handle,
        QueueUrl=MAIN_QUEUE_URL
    )


def change_message_visibility_timeout(recipt_handle):
    sqs_client().change_message_visibility(
        QueueUrl=MAIN_QUEUE_URL,
        ReceiptHandle=recipt_handle,
        VisibilityTimeout=5
    )


def purge_queue():
    sqs_client().purge_queue(
        QueueUrl=DEAD_LETTER_QUEUE_URL
    )


if __name__ == '__main__':
    # print(create_sqs_queue())
    # print(create_fifo_queue())
    # create_queue_for_dead_letter()
    # print(create_main_queue_for_dead_letter())
    # print(find_queue())
    # print(list_all_queues())
    # print(get_queue_attributes())
    # update_queue_attributes()
    # delete_queue()
    # send_message_to_queue()
    # send_batch_messages_to_queue()
    # print(poll_queue_for_messages())
    # process_message_from_queue()
    purge_queue()
