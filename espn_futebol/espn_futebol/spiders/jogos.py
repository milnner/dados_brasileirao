from pathlib import Path
import scrapy
import os as os


URLS_TIMES_BRASILEIRAO_FILE = "urlsTimesDoBrasileirao.txt"


class Jogo(scrapy.Item):
    linha = scrapy.Field()


class UrlsDosTimes(scrapy.Spider):
    name = "jogos_brasileirao"

    def start_requests(self):
        try:
            urls = Path(URLS_TIMES_BRASILEIRAO_FILE).read_text().split("\n")
        except FileNotFoundError:
            print("Arquivo não encontrado")
            exit(0)
        
        for url in urls:
            yield scrapy.Request(url=url.replace("_/","resultados/_/"),
                                 callback=self.parse)

    def parse(self, response):
        tabelas = response.xpath("//div[@class='ResponsiveTable Table__results']")

        for tabela in tabelas:
            rows = tabela.xpath("//table//tr | //table//tr")
            titulo_da_tabela = tabela.xpath("//div[@class='Table__Title']/text()").get()
            for row in rows:
                data = row.xpath("  .//td//a/text() |       \
                                    .//td//div/text() |     \
                                    .//td//span/a/text() |  \
                                    .//td/span[not(@*)]/text()"
                                ).getall()
                if len(data) == 0:
                    continue
                # campeonato = row.xpath(".//td").getall()[-1].xpath(".//span/a/text()")
                # data.append(campeonato
                            # )
                data.append(titulo_da_tabela)
                item = Jogo()
                item["linha"] = data
                yield item
                

