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
from datetime import datetime


print("Iniciando Extração ... \n")

def extrai_reclamacoes():
    # Credenciais Google Cloud
    credentials = service_account.Credentials.from_service_account_file(filename='/home/alex/apps/drivers/ContaServicoBigQuery.json', 
                                                        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials)
    # Query SQL
    sql = """
    SELECT
        CONCAT(A.link,"/lista-reclamacoes/") AS link
    FROM `carbide-legend-331517.ReclameAquiStage.empresas_ranking` AS A
    ORDER BY posicao
    """
    # Executando Query
    link_lojas = client.query(sql).to_dataframe()
    link_lojas = link_lojas['link'].values.tolist()
    # Criando link das reclamações
    link_reclamacoes = link_lojas[0] + str("/lista-reclamacoes/")
    # Criando dataframe para armazenar informações das reclamacoes
    info_reclamacoes = pd.DataFrame(columns = ['id_empresa','idreclamacao','empresa','link','titulo','local','data','status','vfn','nota'])
    # Criando data frame para armazenar erros
    info_erros = pd.DataFrame(columns =['iderro','id_empresa','idreclamacao','empresa','link_pag','link'])
    # Carregando driver
    # driver = webdriver.Chrome('/home/alex/Documentos/Unifal/ReclameAquiTCC/codigos/drive/chromedriver')
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s)
    # Valor para fazer id_empresa
    id_empresa = 0
    # Contador para id_reclamacao
    idreclamacao = 0
    # Contador de erros
    erros = 0
    print(link_reclamacoes)
    # Estrutura de repetição para inserir dados das reclamações
    for link_reclamacao in link_reclamacoes:
        # Chave identificadora da empresa
        id_empresa = id_empresa + 1
        # Data de limite para coleta dados
        data_atual = time.strftime( "%d/%m/%Y")
        data_reclamacao = time.strptime(data_atual, "%d/%m/%Y")
        dtObj = datetime.strptime(data_atual, "%d/%m/%Y")
        # Add -3 months to a given datetime object
        n = -3
        data_limite = dtObj + pd.DateOffset(months=n)
        data_limite = data_limite.strftime("%d/%m/%Y")
        data_limite = time.strptime(data_limite, "%d/%m/%Y")
        # data_limite = "15/06/2022"
        # data_limite = time.strptime(data_limite, "%d/%m/%Y")
        # Contador de páginas acessadas
        p = 0
        # Estutura de repetição para ir mudando as páginas
        while data_reclamacao >= data_limite:
            p = p +1
            for reclamacao in range(1,2):
                try:
                    idreclamacao = idreclamacao + 1
                    # driver.get(link_reclamacao + str('?pagina=1/') + str('&status=EVALUATED'))
                    link_pag = str(link_reclamacao) + '?pagina=' + str(p) + '&status=EVALUATED'
                    driver.get(link_pag)
                    time.sleep(5)
                    if idreclamacao == 1 and id_empresa == 1:
                        driver.maximize_window()
                        driver.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]').click()        
                    driver.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]').click()        
                    # time.sleep(2)
                    driver.delete_all_cookies()
                    # Busca empresa da reclamação
                    empresa = driver.find_element_by_xpath('//*[@id="hero"]/div[2]/div/div[2]/div[1]/h2').get_attribute('title')                                               
                    time.sleep(5)
                    # Armaezena link da reclamacao
                    link = driver.find_element_by_xpath('//*[@id="__next"]/div[1]/div[1]/div[4]/main/section[2]/div[2]/div[2]/div[' + str(reclamacao) + ']/a').get_attribute('href')                             
                    # Entra na reclamação avaliada
                    driver.find_element_by_xpath('//*[@id="__next"]/div[1]/div[1]/div[4]/main/section[2]/div[2]/div[2]/div[' + str(reclamacao) + ']/a/h4').click()        
                    time.sleep(5)
                    # Titulo reclamacao
                    titulo = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/h1').text 
                    # Local da reclamação
                    local = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/div[1]/div[1]/div[1]/span').text
                    # Data reclamacao
                    data = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[3]/div[1]/div[1]/div[2]/span').text
                    # Status reclamacao
                    status = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[1]/div[2]/div/span').text
                    # Voltaria fazer negócio
                    vfn = driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[2]/div[2]/div[2]/div[2]/div/div').text
                    # Nota
                    nota = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/div[1]/div[4]/main/div/div[2]/div[1]/div[2]/div[2]/div[2]/div[3]/div/div').text
                    # Convertendo data para validação
                    data = data[:10]
                    data_reclamacao = time.strptime(data, "%d/%m/%Y")
                    # Clausúla para continuar o loop
                    if data_reclamacao < data_limite:
                        break                
                    # Inserindo dados no dataframe
                    info_reclamacoes = pd.DataFrame([[id_empresa,idreclamacao,empresa,link,titulo,local,data,status,vfn,nota]],columns = info_reclamacoes.columns).append(info_reclamacoes)
                except:
                    iderro = erros +1
                    # info_erros = pd.DataFrame([[iderro,id_empresa,idreclamacao,empresa,link_pag,link]],columns = info_erros.columns).append(info_erros)
                pass                 
    # fechando drive
    driver.close()
    # print('total de erros: ', iderro)
    # # Colocando dados das reclamcaoes em um arquivo csv
    # os.makedirs('dados', exist_ok=True)  
    # info_reclamacoes.to_csv('dados/info_reclamacoes.csv',index=False)
    # # Colocando dados dos erros em um arquivo csv
    # os.makedirs('dados', exist_ok=True)  
    # info_erros.to_csv('dados/info_erros.csv',index=False)
    # # Passando arquivo Big Query
    # gbq.to_gbq(dataframe = info_reclamacoes,credentials=credentials, 
    #                                 destination_table='ReclameAquiStage.info_reclamacoes', 
    #                                 if_exists='replace',
    #                                 table_schema=[{'name': 'id_empresa', 'type': 'INTEGER'},
    #                                             {'name': 'idreclamacao', 'type': 'INTEGER'},
    #                                             {'name': 'empresa', 'type': 'STRING'},
    #                                             {'name': 'link', 'type': 'STRING'},
    #                                             {'name': 'titulo', 'type': 'STRING'},
    #                                             {'name': 'local', 'type': 'STRING'},
    #                                             {'name': 'data', 'type': 'STRING'},
    #                                             {'name': 'status', 'type': 'STRING'},
    #                                             {'name': 'vfn', 'type': 'STRING'},
    #                                             {'name': 'nota', 'type': 'STRING'}])
    # # Passando arquivo Big Query
    # gbq.to_gbq(dataframe = info_erros,credentials=credentials, 
    #                                 destination_table='ReclameAquiStage.info_erros', 
    #                                 if_exists='replace',
    #                                 table_schema=[{'name': 'iderro', 'type': 'INTEGER'},
    #                                             {'name': 'id_empresa', 'type': 'INTEGER'},
    #                                             {'name': 'idreclamacao', 'type': 'INTEGER'},
    #                                             {'name': 'empresa', 'type': 'STRING'},
    #                                             {'name': 'link_pag', 'type': 'STRING'},
    #                                             {'name': 'link', 'type': 'STRING'}])

# Executando
extrai_reclamacoes()
