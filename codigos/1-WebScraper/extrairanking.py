# Importando Pacotes
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time 
import pandas as pd
import os
from google.oauth2 import service_account
from pandas.io import gbq



# Função que extrai raking das seguradoras com mais reclamações

def run_ranking(rank):
    # Criando data dataframe
    empresas_ranking = pd.DataFrame(columns= ['posicao','empresa','link']) # cria um data frame
    # Carregando Conta de Serviço Big Quer
    credentials = service_account.Credentials.from_service_account_file(filename='/home/alex/apps/drivers/ContaServicoBigQuery.json', 
                                                                    scopes=["https://www.googleapis.com/auth/cloud-platform"])
    # Coletando dados das páginas com maiores reclamações
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    pagina ='https://www.reclameaqui.com.br/categoria/seguradoras/'
    driver.get(pagina)
    time.sleep(5)
    # Fazendo for para coletar top 5 seguradoras com maiores reclamações no reclame aqui
    for i in range(rank):
        empresa = driver.find_element_by_xpath(
        '//*[@id="category-rankings"]/div/div/div[3]/div/ul/li[' + str( i + 1 ) + ']/a[1]').get_attribute('title')
        link = driver.find_element_by_xpath(
        '//*[@id="category-rankings"]/div/div/div[3]/div/ul/li[' + str( i + 1 ) + ']/a[1]').get_attribute('href')
        empresas_ranking = pd.DataFrame([[i+1,empresa,link]],columns = empresas_ranking.columns).append(empresas_ranking)
    # Fechando drive
    driver.close()
    # Passando arquivo Big Query
    gbq.to_gbq(dataframe = empresas_ranking,credentials=credentials, 
                                 destination_table='ReclameAquiStage.empresas_ranking', 
                                 if_exists='replace',
                                 table_schema=[{'name': 'posicao', 'type': 'INTEGER'},
                                               {'name': 'empresa', 'type': 'STRING'},
                                               {'name': 'link', 'type': 'STRING'}])


