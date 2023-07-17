# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

tabela_jogos = "jogos.csv"
tabela_times = "times.txt"

mes_to_num = {
    "jan":"1",
    "fev":"2",
    "mar":"3",
    "abr":"4",
    "mai":"5",
    "jun":"6",
    "jul":"7",
    "ago":"8",
    "set":"9",
    "out":"10",
    "nov":"11",
    "dez":"12"
}

class FormataDados:
    def open_spider(self, spider):
        with open(tabela_jogos, "w", encoding="utf-8") as f:
            pass
    def process_item(self, item, spyder):
        data =      \
            self    \
            .formata_data(              \
                item["linha"][0][6:-1], \
                item["linha"][-1])

        mandante,      \
        visitante,     \
        g_mandante,    \
        g_visitante =  \
            self       \
            .formata_mandante_gol_visitante_gol(\
                item["linha"][1],               \
                item["linha"][3],               \
                item["linha"][2])
        
        jogo_status = \
            item["linha"][4]
        campeonato = \
            item["linha"][5]

        with open(tabela_jogos, "a", encoding="utf-8") as f:
            f.write(f"{data};{mandante};{g_mandante};{visitante};{g_visitante};{jogo_status};{campeonato}\n")
    def close_spider(self, spider):

        pass

    
    def formata_mandante_gol_visitante_gol(self,mandante:str, visitante, gols:str):
        g_mandante, g_visitante = gols.split(" - ")
        return mandante, visitante, g_mandante, g_visitante

    def formata_data(self, slice_data:str, ultimo_item_linha:str):
        espaco_pos = slice_data.find(" ")
        
        if espaco_pos == -1:
            raise ValueError("Data fora do formato")
        
        dia = slice_data[0:espaco_pos]
        mes = mes_to_num[slice_data[espaco_pos+1:].strip().lower()]

        ano_texto_pos = str(ultimo_item_linha).find(" ") + 1
        ano = ultimo_item_linha[ano_texto_pos:]
        return f"{ano}-{mes}-{dia}"    
    

