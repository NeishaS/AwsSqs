import boto3

TOPIC_NAME = 'SubscriptionTopic'
TOPIC_ARN = 'arn:aws:sns:us-east-2:585973949575:SubscriptionTopic'


def sns_client() -> object:
    sns = boto3.client('sns', region_name='us-east-2')
    """:type: pyboto3.sns """
    return sns


def create_topic():
    return sns_client().create_topic(Name=TOPIC_NAME)


def get_topics():
    return sns_client().list_topics()


def get_topic_attributes():
    return sns_client().get_topic_attributes(
        TopicArn=TOPIC_ARN
    )


def update_topic_attributes():
    return sns_client().set_topic_attributes(TopicArn=TOPIC_ARN,
                                             AttributeName='DisplayName',
                                             AttributeValue=TOPIC_NAME + '-Updated')


def delete_topic():
    sns_client().delete_topic(TopicArn=TOPIC_ARN)


def create_email_subscription(topic_arn, email_address):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=email_address
    )


def create_sms_subscription(topic_arn, phone_number):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sms',
        Endpoint=phone_number
    )


def create_sqs_queue_subscription(topic_arn, queue_arn):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )


def get_topic_subscriptions(topic_arn):
    return sns_client().list_subscriptions_by_topic(TopicArn=topic_arn)


def check_if_phone_number_opted_out(phone_number) -> object:
    return sns_client().check_if_phone_number_is_opted_out(phoneNumber=phone_number)


def list_opted_out_phone_numbers():
    return sns_client().list_phone_numbers_opted_out()


def opt_out_of_email_subscription(email_address):
    subscriptions = get_topic_subscriptions(TOPIC_ARN)
    for subscription in subscriptions['Subscriptions']:
        if subscription['Protocol'] == 'email' and subscription['Endpoint'] == email_address:
            print('Unsubscribing ' + subscription['Endpoint'])
            subscription_arn = subscription['SubscriptionArn']
            sns_client().unsubscribe(SubscriptionArn=subscription_arn)
            print('Unsubscribed from this topic')


def opt_out_of_sms_subscription(phone_number):
    subscriptions = get_topic_subscriptions(TOPIC_ARN)
    for subscription in subscriptions['Subscriptions']:
        if subscription['Protocol'] == 'sms' and subscription['Endpoint'] == phone_number:
            print('Unsubscribing ' + subscription['Endpoint'])
            subscription_arn = subscription['SubscriptionArn']
            sns_client().unsubscribe(SubscriptionArn=subscription_arn)
            print('Unsubscribed from this topic')


def opt_in_phone_number(phone_number):
    return sns_client().opt_in_phone_number(phoneNumber=phone_number)


def publish_message_to_subscribers(topic_arn):
    return sns_client().publish(
        TopicArn=topic_arn,
        Message='Hello, you are receiving this because you are subscribed!'
    )


if __name__ == '__main__':
    # create_topic()
    # print(get_topics())
    # print(get_topic_attributes())
    # update_topic_attributes()
    # delete_topic()
    # create_email_subscription(TOPIC_ARN, '')
    # create_sms_subscription(TOPIC_ARN, '')
    # create_sqs_queue_subscription(TOPIC_ARN, 'arn:aws:sqs:us-east-2:585973949575:Main-Queue')
    # print(get_topic_subscriptions(TOPIC_ARN))
    # print(check_if_phone_number_opted_out(''))
    # print(list_opted_out_phone_numbers())
    # opt_out_of_email_subscription('')
    # opt_out_of_sms_subscription('')
    # print(opt_in_phone_number(''))
    print(publish_message_to_subscribers(TOPIC_ARN))
