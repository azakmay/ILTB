import boto3

def read_in_file(bucket, path):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=path)
    html_str = s3_response_object['Body'].read()
    return html_str.decode('utf-8')


# list files of s3 bucket
def list_files(bucket, path):
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.list_objects(Bucket=bucket, Prefix=path)
    return s3_response_object['Contents']



class ILTBParser:
    def __init__(self, soup):
        self.soup = soup

    def parse(self):
        pass

    def process(self):
        pass
