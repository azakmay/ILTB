import json
import logging
import re
from collections import OrderedDict

import boto3
from bs4 import BeautifulSoup

from itlb_scraper import upload_html, _check_exists

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
            # Some documents have timestamp before the name and others do not
            if item.text.find('[') > item.text.find(':'):
                s = item.text.split(":")
                name_first = True
            else:
                s = item.text.split(']')[1].split(':')
                name_first = False
            name = s[0].strip()
            names.append(name)
        conversation.extend([item.text])
    interviewers = list(OrderedDict.fromkeys(names).keys())
    conversation = " ".join(conversation)
    return name_first, {'interviewers': interviewers, 'names': names, 'conversation': conversation}


def process(interviewers, names, conversation, name_first=True):
    if name_first:
        string = f"(?:{interviewers[0]}|{interviewers[1]}):\s*\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*"
    else:
        string = f'\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*(?:{interviewers[0]}|{interviewers[1]}):\s*'
        # string = f"(?:\s*\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*:{interviewers[0]}|{interviewers[1]}):"

    paragraphs = re.split(string, conversation)
    transcript = []
    # Want to skip the introduction
    for n, p in zip(names[1:], paragraphs[2:]):
        d = {'name': n, 'response': p.strip()}
        transcript.append(d)

    return transcript


def transform_html_to_json():
    bucket = 'scrape-projects'
    for file in list_files(bucket, path='colossus-transcripts/html/'):
        file_name = file['Key'].split('/')[-1].replace('.html', '')
        logger.warning(f"processing {file_name}")
        if _check_exists(file_name, bucket=bucket, extension='json'):
            logger.warning(f"skipping {file_name}")
            continue
        html = read_in_file(bucket, file['Key'])
        soup = BeautifulSoup(html, 'html.parser')
        name_first, parsed = parse(soup)

        if len(parsed['interviewers']) != 2:
            logger.error(f"3 or more people in interview, skipping {file_name}")
            continue

        transcript = process(parsed['interviewers'], parsed['names'], parsed['conversation'], name_first=name_first)
        logger.warning(f"uploading {file_name}")
        upload_html(json.dumps(transcript), bucket=bucket, name=file_name, extension='json')


if __name__ == '__main__':
    transform_html_to_json()
