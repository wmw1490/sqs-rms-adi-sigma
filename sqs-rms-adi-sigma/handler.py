import sys
import boto3
sqs = boto3.client("sqs")
from os import environ

def handler(event, context):
    
    client = boto3.client('sqs')
    try:
        data = sqs.receive_message(
            QueueUrl="https://sqs.{}.amazonaws.com/{}/sqs-rms-adi-in".format(environ["AWS_REGION"], environ["SIGMA_AWS_ACC_ID"]),
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )
    except BaseException as e:
        print(e)
        raise(e)

    # get the body of the message
    body = data.get('Body')
    qsostring = data['Messages'][0]['Body']

    qsolocation, qsodatetime, qsobearing, qsocallsign, qsocmsbytes,    \
       qsoseconds, qsodistance, qsofreq, qsogridsquare,  \
       qsolastcommand, qsomode, qsomsgrcv, qsomsgsent, qsoradiobytes, \
       gwgridsq, gwcallsign, qsohash = qsostring.split(',')

    # Connect to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('QSO')
    # Attempt to insert into DynamoDB table
    try:
        table.put_item(Item={'QSOlocation': qsolocation, 'QSOdatetime': qsodatetime, \
            'QSObearing': qsobearing, 'QSOcallsign': qsocallsign, 'QSOcmsbytes': qsocmsbytes, \
            'QSOseconds': qsoseconds, 'QSOdistance': qsodistance, 'QSOfreq': qsofreq, \
            'QSOgridsquare': qsogridsquare, 'QSOlastcommand': qsolastcommand, \
            'QSOmode': qsomode, 'QSOmsgrcv': qsomsgrcv, 'QSOmsgsent': qsomsgsent, \
            'QSOradiobytes': qsoradiobytes, 'GWgridsq': gwgridsq, 'GWcallsign': gwcallsign, \
            'QSOhash': qsohash } )          

        try:
            # Delete message from sqs once added to dynamodb
            response = client.delete_message(
                QueueUrl='https://sqs.us-east-2.amazonaws.com/578839498373/sqs-rms-adi-in',
                ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
        except:
            print('**unable to delete message**')
    except:
        # do nothing
        print('Unable to write to DynamoDB')
        print(body)

    return {"message": "Successfully executed"}
