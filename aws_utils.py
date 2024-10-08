import boto3
import json
from os import environ,path,popen
environ['AWS_DEFAULT_REGION'] = "us-west-2"
environ['AWS_PROFILE'] = "s3writeprofile"



class aws_utils():
    def __init__(self):
        self.session = boto3.Session(profile_name='s3writeprofile')
        self.sqs = boto3.client('sqs')
        self.s3 = boto3.resource("s3").Bucket("data.pooltool.io")
        self.dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        self.tipstable = self.dynamodb.Table(f'NewTips') # update to new table at same time as sendtips
        
    
    def s3_put_object(self,Key,Body,ACL='public-read',ContentType='image/png'):
        self.s3.put_object(Body=Body,ACL=ACL, ContentType=ContentType, Key=Key)

    def load_s3(self,f):
        return json.load(self.s3.Object(key=f).get()["Body"])
    def dump_s3(self,obj,f):
        self.s3.Object(key=f).put(Body=json.dumps(obj),ACL='public-read')
    def s3_upload_file(self, Key, file_path, ACL='public-read'):
        # Upload the file directly to S3
        self.s3.upload_file(Filename=file_path, Key=Key, ExtraArgs={'ACL': ACL, 'ContentType': 'text/csv'})

        print(f"File {file_path} uploaded to S3 as {Key}")

    def get_directory_listing_s3(self,dir):
        return [obj.key for obj in self.s3.objects.filter(Prefix=dir)]
    
    def load_s3_object(self, Key):
        # Retrieve the object from S3
        return self.s3.Object(key=Key).get()["Body"].read()

    def delete_s3_object(self,Key):
        self.s3.Object(key=Key).delete()
    
    def awsbroadcast(self,message):
        queue_url = 'https://sqs.us-west-2.amazonaws.com/637019325511/pooltoolevents.fifo'
        #print(message)
        # Send message to SQS queue
        response = self.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=(
                json.dumps(message)
            ),
            MessageGroupId="botgroup"
        )
        #print(response['MessageId'])

        queue_url = 'https://sqs.us-west-2.amazonaws.com/637019325511/ptb_twitter.fifo'
        # Send message to SQS queue
        response = self.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=(
                json.dumps(message)
            ),
            MessageGroupId="botgroup"
        )
        #print(response['MessageId'])