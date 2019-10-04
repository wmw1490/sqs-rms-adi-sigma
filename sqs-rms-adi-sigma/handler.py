from hamutils.adif import ADIReader
import sys
import boto3

def handler(event, context):
    
    a = "World"
    print("Hello ")
    print(a)
    return {"message": "Successfully executed"}
