from pathlib import Path
import scrapy
import os as os


URLS_TIMES_BRASILEIRAO_FILE = "urlsTimesDoBrasileirao.txt"
filename = "jogos.txt"



class UrlsDosTimes(scrapy.Spider):
    name = "jogos_brasileirao"

    def start_requests(self):
        try:
            urls = Path(URLS_TIMES_BRASILEIRAO_FILE).read_text().split("\n")
        except FileNotFoundError:
            print("Arquivo não encontrado")
        with open(filename, "w", encoding="utf-8") as jogos_file:
            pass
        
        for url in urls:
            yield scrapy.Request(url=url.replace("_/","resultados/_/"),
                                 callback=self.parse)

    def parse(self, response):
        rows = response.xpath("//table//tr | //table//tr")

        with open(filename, "a", encoding="utf-8") as jogos_file:
           for row in rows:
                data = row.xpath(".//td//a/text() | .//td//div/text() |\
                                  .//td//span/text()").getall()
                print(data)
                if len(data) == 0:
                    continue
                yield ";".join(data) + "\n"
            #    jogos_file.write()

