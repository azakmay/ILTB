import logging
import os
import tempfile
import time

import awswrangler as wr
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

load_dotenv()

BUCKET = os.getenv('DATA_BUCKET')
SLEEP_TIME = 5


def create_s3_path(bucket=None, name=None, extension='html'):
    if bucket is None:
        bucket = BUCKET
    bucket = f"s3://{bucket}/"
    path = bucket + f"colossus-transcripts/{extension}/{name}.{extension}"
    return path


def upload_file(content: str, bucket: str = None, name: str = None, extension: str = 'html'):
    path = create_s3_path(bucket, name, extension=extension)
    with tempfile.NamedTemporaryFile('wb', suffix="." + extension, delete=False) as temp:
        temp.write(content.encode())
        wr.s3.upload(temp.name, path)


class ILTBUrls:
    LOGIN_URL = "https://www.joincolossus.com/login"
    EPISODE_URLS = "https://www.joincolossus.com/episodes?prod-episode-release-desc%5BrefinementList%5D%5BpodcastName%5D%5B0%5D=Invest%20Like%20the%20Best&prod-episode-release-desc%5Bpage%5D={page}"


class ILTBSelectors:
    LOGIN_ID_NAME = "ignoreautocompleteemail"
    LOGIN_PW_NAME = "ignoreautocompletepassword"
    LOGIN_BTN_XPATH = '//*[@id="q-app"]/div/div[1]/main/div/div/div/div[4]/button'
    LOGOUT_BTN_XPATH = '/html/body/div[3]/div/div[7]/div[2]/div'
    EPISODE_DIV_XPATH = '//*[@id="q-app"]/div/div[1]/main/div/div/div/div/div[2]/div/div[1]/div[{div_number}]/div/div[1]/div[2]/div[2]'


def _check_exists(episode_title: str, bucket=None, extension='html') -> bool:
    """
    Check if the episode has already been scraped
    :param episode_title: The title of the episode
    :return: True if the episode has already been scraped, False otherwise
    """
    path = create_s3_path(name=episode_title, bucket=bucket, extension=extension)
    return wr.s3.does_object_exist(path)


class ILTBScraper:
    """
    Scraper for Invest Like the Best
    :param username: username for colossus
    :param password: password for colossus
    """

    def __init__(self, username: str = None, password: str = None):
        self.username = username or os.getenv('COLOSSUS_USER')
        self.password = password or os.getenv('COLOSSUS_PW')
        self.driver = webdriver.Chrome()
        self.driver.get(ILTBUrls.LOGIN_URL)

    def is_logged_in(self):
        try:
            el = self.find_element_by_xpath(ILTBSelectors.LOGOUT_BTN_XPATH)
            return el.text == 'Logout'
        except NoSuchElementException:
            return False

    def find_element_by_id(self, id: str):
        return self.driver.find_element(By.ID, id)

    def find_element_by_name(self, id: str):
        return self.driver.find_element(By.NAME, id)

    def find_element_by_class(self, id: str):
        return self.driver.find_element(By.CLASS_NAME, id)

    def find_element_by_xpath(self, id: str):
        return self.driver.find_element(By.XPATH, id)

    def login(self):
        self.find_element_by_name(ILTBSelectors.LOGIN_ID_NAME).send_keys(self.username)
        self.find_element_by_name(ILTBSelectors.LOGIN_PW_NAME).send_keys(self.password)
        self.find_element_by_xpath(ILTBSelectors.LOGIN_BTN_XPATH).click()

    def generate_pages_to_visit(self, start: int = 1, stop: int = 39):
        for i in range(start, stop):
            yield ILTBUrls.EPISODE_URLS.format(page=i)

    def get_episode_pages(self, page, start: int = 1, stop: int = 9):
        """
        Generates clickable elements for each episode on a page
        :param page: url of page to scrape
        :param start: first episode div to scrape
        :param stop: last episode div to scrape
        :return: elements to click that have the transcript
        """
        episode_link = []
        self.driver.get(page)
        time.sleep(SLEEP_TIME)
        for i in range(start, stop):
            element = self.find_element_by_xpath(ILTBSelectors.EPISODE_DIV_XPATH.format(div_number=i))
            episode_title = element.text.replace(' ', '-')
            if _check_exists(episode_title):
                logger.warning("Already scraped %s", episode_title)
                continue
            element.click()
            # Sleep and give the page a second to load before downloading
            time.sleep(SLEEP_TIME)
            upload_file(self.driver.page_source, name=episode_title)

            episode_link.append(self.driver.current_url)
            self.driver.get(page)
            time.sleep(SLEEP_TIME)

        return episode_link


def main(start=1):
    scraper = ILTBScraper()
    if not scraper.is_logged_in():
        scraper.login()
    pages = scraper.generate_pages_to_visit(start=start)
    for page in pages:
        scraper.get_episode_pages(page)


if __name__ == '__main__':
    main()
