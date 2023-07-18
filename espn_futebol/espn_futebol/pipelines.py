# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import subprocess
from itemadapter import ItemAdapter
import bisect
import pandas as pd

tabela_jogos = "jogos.csv"

tabela_times = "times.csv"
tabela_campeonato = "campeonato.csv"
tabela_jogo = "jogo.csv"
tabela_jogo_status = "status.csv"
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

        time_esquerda_placar,      \
        time_direita_placar,     \
        g_time_esquerda_placar,    \
        g_time_direita_placar =  \
            self       \
            .formata_time_esquerda_placar_gol_time_direita_placar_gol(\
                item["linha"][1],               \
                item["linha"][3],               \
                item["linha"][2])
        
        jogo_status = \
            item["linha"][4]
        campeonato = \
            item["linha"][5]

        with open(tabela_jogos, "a", encoding="utf-8") as f:
            f.write(f"{data};{time_esquerda_placar};{g_time_esquerda_placar};{time_direita_placar};{g_time_direita_placar};{jogo_status};{campeonato}\n")
    def close_spider(self, spider):
        times_list = []
        times_dict = {}
        campeonato_list = []
        campeonato_dict = {}
        jogo_status_list = []
        jogo_status_dict = {}

        with open(tabela_jogos, "r", encoding="utf-8") as f, \
            open(tabela_jogo, "w", encoding="utf-8") as f_jogo, \
            open(tabela_campeonato, "w", encoding="utf-8") as f_camp, \
            open(tabela_jogo_status, "w", encoding="utf-8") as f_status, \
            open(tabela_times, "w", encoding="utf-8") as f_times:
            id_time_count = max(times_dict.values()) + 1 if times_dict else 1
            id_campeonato_count = max(campeonato_dict.values()) + 1 if campeonato_dict else 1
            id_jogo_status_count = max(jogo_status_dict.values()) + 1 if jogo_status_dict else 1

            for linha in f:
                linha = linha.upper().strip()
                dados = linha.split(";")
                time_esquerda_placar, time_direita_placar, campeonato, jogo_status = dados[1], dados[3], dados[6], dados[5]

                if time_esquerda_placar not in times_dict:
                    times_dict[time_esquerda_placar] = id_time_count
                    bisect.insort(times_list, time_esquerda_placar)
                    f_times.write(f"{id_time_count};{time_esquerda_placar}\n")
                    id_time_count += 1

                if time_direita_placar not in times_dict:
                    times_dict[time_direita_placar] = id_time_count
                    bisect.insort(times_list, time_direita_placar)
                    f_times.write(f"{id_time_count};{time_direita_placar}\n")
                    id_time_count += 1

                if campeonato not in campeonato_dict:
                    campeonato_dict[campeonato] = id_campeonato_count
                    bisect.insort(campeonato_list, campeonato)
                    f_camp.write(f"{id_campeonato_count};{campeonato}\n")
                    id_campeonato_count += 1

                if jogo_status not in jogo_status_dict:
                    jogo_status_dict[jogo_status] = id_jogo_status_count
                    bisect.insort(jogo_status_list, jogo_status)
                    f_status.write(f"{id_jogo_status_count};{jogo_status}\n")
                    id_jogo_status_count += 1

                id_time_esquerda_placar = times_dict[time_esquerda_placar]
                id_time_direita_placar = times_dict[time_direita_placar]
                id_campeonato = campeonato_dict[campeonato]
                id_jogo_status = jogo_status_dict[jogo_status]

                f_jogo.write(f"{id_time_esquerda_placar};{id_time_direita_placar};{dados[0]};{id_jogo_status};{dados[2]};{dados[4]};{id_campeonato}\n")
        
        # remove as copias e adiciona indices
        df = pd.read_csv(tabela_jogo, encoding="utf-8", sep=";")
        df.drop_duplicates(inplace=True)
        df.insert(0, '', range(1, len(df) + 1))
        df.to_csv(tabela_jogo, encoding="utf-8", sep=";",header=False, index=False)

        subprocess.run(f"psql -U postgres -d dados_futebol -f load.sql", shell=True)
    
    def formata_time_esquerda_placar_gol_time_direita_placar_gol(self,time_esquerda_placar:str, time_direita_placar, gols:str):
        g_time_esquerda_placar, g_time_direita_placar = gols.split(" - ")
        return time_esquerda_placar, time_direita_placar, g_time_esquerda_placar, g_time_direita_placar

    def formata_data(self, slice_data:str, ultimo_item_linha:str):
        espaco_pos = slice_data.find(" ")
        
        if espaco_pos == -1:
            raise ValueError("Data fora do formato")
        
        dia = slice_data[0:espaco_pos]
        mes = mes_to_num[slice_data[espaco_pos+1:].strip().lower()]

        ano_texto_pos = str(ultimo_item_linha).find(" ") + 1
        ano = ultimo_item_linha[ano_texto_pos:]
        return f"{ano}-{mes}-{dia}"    
    
