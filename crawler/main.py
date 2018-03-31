from lxml import html
import requests
import time
import re


class Song:
    def __init__(self, name, artist, lyric):
        self.name = name.strip().lower()
        self.artist = artist.strip().lower()
        self.lyric = lyric.strip().lower()

    def fix_name(self, name):
        name = re.sub('- REMIX$', '', name)
        return name.strip()

    def __str__(self):
        return (self.name.encode("UTF-8") +
                "\n" + self.artist.encode("UTF-8") +
                "\n" + self.lyric.encode("UTF-8") + "\n")


class SongCrawler:
    def __init__(self, page_url, counter):
        self.page_url = page_url
        self.counter = counter

    def get_page(self, link):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
        return requests.get(link, headers=headers)

    def start_crawl(self):
        links = self.get_links_from_page(self.page_url)
        for link in links:
            song = self.get_song_from_link(link)
            if song is None:
                continue
            self.counter += 1
            print(self.counter)
            self.write_to_file(song, "../data/{}.txt".format(self.counter))

    def write_to_file(self, song, filename):
        file = open(filename, "w")
        file.write(str(song))
        file.close()

    def get_links_from_page(self, page_url):
        page = self.get_page(page_url)
        document = html.fromstring(page.text)
        links = document.xpath('//h2[@class="name"]/a/@href')

        return links

    def get_song_from_link(self, link):
        try:
            page = self.get_page(link)
            document = html.fromstring(page.text)
            for br in document.xpath("*//br"):
                br.tail = " " + br.tail if br.tail else " "
            name = document.xpath('//h2[@class="name"]/text()')[0]
            artist = document.xpath('//div[@class="artist"]/a/text()')[0]
            lyric = document.xpath('//div[@class="lyric"]')[0].text_content()
            lyric = re.sub(" +", " ", lyric).strip()
            return Song(name, artist, lyric)
        except Exception as e:
            print(e)
            return None


def main():
    print("Start")

    page_num = 1
    couter = 0

    for idx in range(1, page_num + 1):
        crawler = SongCrawler(
            "http://chonbaihat.com/karaoke-arirang/trang/{}".format(idx), couter)
        crawler.start_crawl()
        couter = crawler.counter

    print("Finish")


if __name__ == '__main__':
    main()
