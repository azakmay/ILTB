import json
import logging
import re
from collections import OrderedDict
from typing import List

import boto3
from bs4 import BeautifulSoup

from itlb_scraper import upload_file, _check_exists

logger = logging.getLogger(__name__)


def read_in_file(bucket, path):
    """
    Read in file from s3 bucket
    :param bucket:
    :param path:
    :return: file
    """
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=path)
    html_str = s3_response_object['Body'].read()
    return html_str.decode('utf-8')


# list files of s3 bucket
def list_files(bucket, path):
    """
    List files in s3 bucket
    :param bucket: s3 bucket
    :param path: prefix
    :return: Contents of bucket
    """
    s3_client = boto3.client('s3')
    s3_response_object = s3_client.list_objects(Bucket=bucket, Prefix=path)
    return s3_response_object['Contents']


def _check_if_name_first(soup: BeautifulSoup) -> bool:
    """
    Check if the name is first in the text
    :param soup:
    :return: whether the name is first
    """
    item = soup.find('p')
    return item.text.find('[') > item.text.find(':')


def parse(soup: BeautifulSoup) -> dict:
    """
    Parse the html
    :param soup:
    :return:
    """
    conversation = []
    names = []
    for item in soup.find_all('p'):
        if ':' in item.text and '[' in item.text:
            # Some documents have timestamp before the name and others do not
            if item.text.find('[') > item.text.find(':'):
                s = item.text.split(":")
            else:
                s = item.text.split(']')[1].split(':')
            name = s[0].strip()
            names.append(name)
        conversation.extend([item.text])
    interviewers = list(OrderedDict.fromkeys(names).keys())
    conversation = " ".join(conversation)
    return {'interviewers': interviewers, 'names': names, 'conversation': conversation}


def process(interviewers, names, conversation, name_first=True) -> List[dict]:
    if name_first:
        string = f"(?:{interviewers[0]}|{interviewers[1]}):\s*\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*"
    else:
        string = f'\[\d{{2}}:\d{{2}}:\d{{2}}\]\s*(?:{interviewers[0]}|{interviewers[1]}):\s*'

    paragraphs = re.split(string, conversation)
    transcript = []

    # Want to skip the introduction
    for n, p in zip(names[1:], paragraphs[2:]):
        d = {'name': n, 'response': p.strip()}
        transcript.append(d)

    return transcript


def transform_html_to_json():
    """
    Transform html to json and upload json to s3
    """
    bucket = 'scrape-projects'
    for file in list_files(bucket, path='colossus-transcripts/html/'):
        file_name = file['Key'].split('/')[-1].replace('.html', '')
        logger.warning(f"processing {file_name}")
        if _check_exists(file_name, bucket=bucket, extension='json'):
            logger.warning(f"skipping {file_name}")
            continue

        html = read_in_file(bucket, file['Key'])
        soup = BeautifulSoup(html, 'html.parser')
        name_first = _check_if_name_first(soup)
        parsed = parse(soup)

        if len(parsed['interviewers']) != 2:
            logger.error(f"3 or more people in interview, skipping {file_name}")
            continue

        transcript = process(parsed['interviewers'], parsed['names'], parsed['conversation'], name_first=name_first)
        logger.warning(f"uploading {file_name}")
        upload_file(json.dumps(transcript), bucket=bucket, name=file_name, extension='json')


if __name__ == '__main__':
    transform_html_to_json()
