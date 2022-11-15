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

print("Iniciando Extração ... \n")

# carregando links das empresas 
empresas = pd.read_csv("/home/alex/Documentos/Unifal/ReclameAquiTCC/dados/empresasranking.csv",sep=',')
empresas = empresas.sort_values(by='posicao')
link_lojas = empresas['link'].values

# Criando link das reclamações
link_reclamacoes = link_lojas + str("/lista-reclamacoes/")
print("link das reclamações: \n",link_reclamacoes)

# Credenciais Google Cloud
# credentials = service_account.Credentials.from_service_account_file(filename='/home/alex/apps/drivers/ContaServicoBigQuery.json', 
#                                                     scopes=["https://www.googleapis.com/auth/cloud-platform"])
# client = bigquery.Client(credentials=credentials)
# # Query SQL
# sql = """
# SELECT
#     CONCAT(A.link,"/lista-reclamacoes/") AS link
# FROM `carbide-legend-331517.ReclameAquiStage.empresas_ranking` AS A
# ORDER BY posicao
# """
# # Executando Query
# link_lojas = client.query(sql).to_dataframe()
# link_lojas = link_lojas['link'].values.tolist()
# # Criando link das reclamações
# link_reclamacoes = link_lojas[0] + str("/lista-reclamacoes/")
# print(link_reclamacoes)

# Criando dataframe para armazenar informações das reclamacoes
info_reclamacoes = pd.DataFrame(columns = ['idempresa','idreclamacao','empresa','local','data_inicial','data_final','status','vfn','nota']) 
# Carregando driver
# driver = webdriver.Chrome('/home/alex/Documentos/Unifal/ReclameAquiTCC/codigos/drive/chromedriver')
options = webdriver.ChromeOptions()
options.add_argument("--disable-logging")
options.add_argument("--log-level=3")

s = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=s)

# Valor para fazer id_empresa
idempresa = 0

# Contador para id_reclamacao
idreclamacao = 0

# Contador de erros
erros = 0

## Estrutura de repetição para inserir dados das reclamações
for link_reclamacao in link_reclamacoes:
    idempresa = idempresa + 1
    for reclamacao in range(1,2):
        try:
            idreclamacao = idreclamacao + 1
            # driver.get(link_reclamacao + str('?pagina=1/') + str('&status=EVALUATED'))
            driver.get(str(link_reclamacao) + '?pagina=1' + '&status=EVALUATED')
            time.sleep(10)
            if idreclamacao == 1 and idempresa == 1:
                driver.maximize_window()
                driver.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]').click()        
            driver.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]').click()        
            time.sleep(2)
            driver.delete_all_cookies()
            # Busca empresa da reclamação
            empresa = driver.find_element_by_xpath('//*[@id="hero"]/div[2]/div/div[2]/div[1]/h2').get_attribute('title')                                               
            time.sleep(5)
            print(empresa)
            # Entra na reclamação avaliada
            driver.find_element_by_xpath('//*[@id="__next"]/div[1]/div[1]/div[4]/main/section[2]/div[2]/div[2]/div[' + str(reclamacao) + ']/a/h4').click()        
            time.sleep(10)
            # Titulo reclamacao
            titulo = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/h1').text 
            # Local da reclamação
            local = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/div[1]/div[1]/div[1]/span').text
            # Data  incialreclamacao
            data_inicial = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/div[1]/div[1]/div[2]/span').text
            # Data de fechamento da reclamacao
            data_final = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[1]/div[1]/div[2]/div[2]/div[1]/div/span').text  
            # Status reclamacao
            status = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[2]/div/span').text
            # Voltaria fazer negócio
            vfn = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[2]/div[2]/div[2]/div[2]/div/div').text
            # # Nota
            nota = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[2]/div[2]/div[2]/div[3]/div/div').text
            # # Inserindo dados no dataframe
            info_reclamacoes = pd.DataFrame([[idempresa,idreclamacao,empresa,local,data_inicial,data_final,status,vfn,nota]],columns = info_reclamacoes.columns).append(info_reclamacoes)
        except NoSuchElementException:
            erros = erros +1
        pass              
# fechando drive
driver.close()
# colocando dados
print(info_reclamacoes)   
# Colocando dados em um arquivo csv
os.makedirs('dados', exist_ok=True)  
info_reclamacoes.to_csv('dados/info_avaliadas.csv',index=False) 
