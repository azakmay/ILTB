import json
import logging
import re
from collections import OrderedDict

import boto3
from bs4 import BeautifulSoup

from itlb_scraper import upload_html

logger = logging.getLogger(__name__)


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


def parse(soup):
    conversation = []
    names = []
    for item in soup.find_all('p'):
        if ':' in item.text and '[' in item.text:
            s = item.text.split(":")
            name = s[0]
            names.append(name)
        conversation.extend([item.text])
    interviewers = list(OrderedDict.fromkeys(names).keys())
    conversation = " ".join(conversation)
    return {'interviewers': interviewers, 'names': names, 'conversation': conversation}


def process(interviewers, names, conversation):
    if len(interviewers) != 2:
        return None
    string = f"(?:{interviewers[0]}|{interviewers[1]}):\s*\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*"
    paragraphs = re.split(string, conversation)
    transcript = []
    # Want to skip the introduction
    for n, p in zip(names[1:], paragraphs[2:]):
        d = {'name': n, 'response': p}
        transcript.append(d)

    return transcript


def transform_html_to_json():
    bucket = 'scrape-projects'
    for file in list_files(bucket, path='colossus-transcripts/html/'):
        file_name = file['Key'].split('/')[-1].replace('.html', '')
        html = read_in_file(bucket, file['Key'])
        soup = BeautifulSoup(html, 'html.parser')
        parsed = parse(soup)
        transcript = process(parsed['interviewers'], parsed['names'], parsed['conversation'])
        if transcript is None:
            logger.warning(f"Skipping {file_name}")
            continue
        upload_html(json.dumps(transcript), bucket=bucket, name=file_name, extension='json')


if __name__ == '__main__':
    transform_html_to_json()
