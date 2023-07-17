from pathlib import Path
import scrapy
from scrapy.selector import Selector

URLS_TIMES_BRASILEIRAO_FILE = "urlsTimesDoBrasileirao.txt"

class UrlsDosTimes(scrapy.Spider):
    name = "urls_times_brasileirao"

    def start_requests(self):
        urls = [
        "https://www.espn.com.br/futebol/liga/_/nome/bra.1/brasileiro"
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        u = sel.xpath("//div//tr//td//a")
        _urls = ["https://www.espn.com.br"+i.attrib["href"] for i in u]

        print(30*"#")
        print(_urls)
        print(30*"#")

        filename = URLS_TIMES_BRASILEIRAO_FILE
        Path(filename).write_text("\n".join(_urls))
        # self.log(f"Saved file {filename}")