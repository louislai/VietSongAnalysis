from lxml import html
import requests
import time
import re


class Song:
    def __init__(self, name, artist, lyric):
        self.name = name
        self.artist = artist
        self.lyric = lyric

    def fix_name(self, name):
        name = name.strip()
        name = re.sub('- REMIX$', '', name)
        return name.strip()

    def __str__(self):
        return (self.name.encode("UTF-8") +
                "\n" + self.artist.encode("UTF-8") +
                "\n" + self.lyric.encode("UTF-8") + "\n")


class SongCrawler:
    def __init__(self, page_url):
        self.page_url = page_url

    def get_page(self, link):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
        return requests.get(link, headers=headers)

    def start_crawl(self):
        links = self.get_links_from_page(self.page_url)

    def get_links_from_page(self, page_url):
        print("start get links from page ----------", page_url)

        page = self.get_page(page_url)
        document = html.fromstring(page.text)
        links = document.xpath('//h2[@class="name"]/a/@href')

        return links

    def get_lyric_from_link(self, link):
        print("...")


def main():
    print("starting:... ")
    crawler = SongCrawler("http://chonbaihat.com/karaoke-arirang/trang/797")
    crawler.start_crawl()
    print("finish:... ")


if __name__ == '__main__':
    main()
