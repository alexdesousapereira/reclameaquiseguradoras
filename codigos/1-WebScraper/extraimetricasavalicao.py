############# Extraindo Métricas de Avaliação ################

# Importando Pacotes
from google.cloud import bigquery
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

# Coletando dados das páginas com maiores reclamações
def extrai_metricas():
    # Credenciais Google Cloud
    credentials = service_account.Credentials.from_service_account_file(filename='/home/alex/apps/drivers/ContaServicoBigQuery.json', 
                                                                        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials)
    # Query SQL
    sql = """
            SELECT
                A.link
            FROM `carbide-legend-331517.ReclameAquiStage.empresas_ranking` AS A
            ORDER BY posicao
        """
    # Executando Query
    link_lojas = client.query(sql).to_dataframe()
    link_lojas = link_lojas['link'].values.tolist()
    empresas = pd.read_csv("dados/empresasranking.csv",sep=',')
    empresas = empresas.sort_values(by='posicao')
    link_lojas = empresas['link'].values

    # Criando dataframe para armazenar informações da página inicial
    info_avaliadas = pd.DataFrame(columns= ['idempresa','empresa','reclamacoes','respondidas','percent_resp','percent_voltariam','indice_solucao','nota','avaliadas'])     
    #Constante para gerar id
    id = 1
    # Estrutura de repetição para coletar Metricas Gerais das Empresas
    for link_loja in link_lojas :
        # Conectando ao Chorme
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        s = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=s)
        driver.get(link_loja)
        driver.maximize_window()
        time.sleep(10)
        # Busca o nome da empresa
        empresa =  driver.find_element_by_xpath('//*[@id="hero"]/div[2]/div/div[2]/div[1]/h1').get_attribute('title')
        time.sleep(5)
        # Move para Dados Gerais
        driver.find_element_by_xpath('//*[@id="reputation-tab-5"]').click()
        # Total Reclamações
        reclamacoes = driver.find_element_by_xpath('//*[@id="reputation"]/div[1]/div[2]/a[1]/div/div').text
        # Total respondidas
        respondidas = driver.find_element_by_xpath('//*[@id="reputation"]/div[1]/div[2]/a[2]/div/div').text 
        # Busca o % de reclamações respondidas pela empresa
        percent_resp = driver.find_element_by_xpath('//*[@id="reputation"]/div[2]/div[1]/div[1]').get_attribute('title')
        # Busca o % de clientes que voltariam a fazer negácio com a empresa
        percent_voltariam = driver.find_element_by_xpath('//*[@id="reputation"]/div[2]/div[1]/div[2]').get_attribute('title')
        # Busca o índice de reclamações solucionadas pela empresa
        indice_solucao = driver.find_element_by_xpath('//*[@id="reputation"]/div[2]/div[1]/div[3]').get_attribute('title')
        # Busca a nota do consumidor para a empresa
        nota = driver.find_element_by_xpath('//*[@id="reputation"]/div[2]/div[1]/div[4]').get_attribute('title')
        # Avaliadas
        avaliadas = driver.find_element_by_xpath('//*[@id="reputation"]/div[2]/div[2]/div[2]/a/div/b').text
        # Inserindo dados no dataframe
        info_avaliadas = pd.DataFrame([[id,empresa,reclamacoes,respondidas,percent_resp,percent_voltariam,indice_solucao,nota,avaliadas]],columns = info_avaliadas.columns).append(info_avaliadas)
        # adicionando constante
        id = id + 1
    # fechando driver
    driver.close()
    # Colocando dados em um arquivo csv
    os.makedirs('dados', exist_ok=True)  
    info_avaliadas.to_csv('dados/info_avaliadas.csv',index=False)
    # Passando arquivo Big Query
    gbq.to_gbq(dataframe = info_avaliadas,credentials=credentials, 
                                 destination_table='ReclameAquiStage.info_avaliadas', 
                                 if_exists='replace',
                                 table_schema=[{'name': 'idempresa', 'type': 'INTEGER'},
                                               {'name': 'empresa', 'type': 'STRING'},
                                               {'name': 'reclamacoes', 'type': 'STRING'},
                                               {'name': 'percent_resp', 'type': 'STRING'},
                                               {'name': 'percent_voltariam', 'type': 'STRING'},
                                               {'name': 'indice_solucao', 'type': 'STRING'},
                                               {'name': 'nota', 'type': 'STRING'},
                                               {'name': 'avaliadas', 'type': 'STRING'}])
try: 
    extrai_metricas()
except NoSuchElementException:
   extrai_metricas()



