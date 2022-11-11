from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator, PythonVirtualenvOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'airflow',
}


with DAG(dag_id='cnpj_caio_2',
        start_date=datetime.now(),
        schedule_interval="0 */12 * * *",
        default_args=args,
        catchup=False) as dag:

    def get_links():
        import requests
        from bs4 import BeautifulSoup

        URL = "http://200.152.38.155/CNPJ/"
        page = requests.get(URL)
        url_link = []
        soup = BeautifulSoup(page.content, "html.parser")
        links = soup.find_all("a")[5:-1]

        for link in links:
            link_download = 'http://200.152.38.155/CNPJ/'+link["href"]
            url_link.append(link_download)
        return url_link
    
    
    def down_links(url_link):
        import wget
        
        link_download = url_link
        localfile = f'downloads/{link_download[27:]}'
        print(localfile)
        wget.download(link_download, localfile)
    

    def get_date():
        import requests
        from bs4 import BeautifulSoup
    
        URL = "https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        date = soup.find_all(class_="value")[1].text[0:10]

        try:
            with open('/opt/airflow/downloads/data.txt', 'r') as data:
                store_data = data.read()
                if date == store_data:

                    return 'no_new_data'
                else:
                    with open('/opt/airflow/downloads/data.txt', 'w') as data_write:
                        data_write.write(date)

                        return 'new_data'

        except FileNotFoundError:            
            with open('/opt/airflow/downloads/data.txt', 'w') as data:
                data.write(date)

                return 'new_data'
            
    def paralel():
        from joblib import Parallel, delayed

        def down_links(url_link):
            import wget
            
            link_download = url_link
            localfile = f'downloads/{link_download[27:]}'
            print(localfile)
            wget.download(link_download, localfile)

        def get_links():
            import requests
            from bs4 import BeautifulSoup

            URL = "http://200.152.38.155/CNPJ/"
            page = requests.get(URL)
            url_link = []
            soup = BeautifulSoup(page.content, "html.parser")
            links = soup.find_all("a")[5:-1]

            for link in links:
                link_download = 'http://200.152.38.155/CNPJ/'+link["href"]
                url_link.append(link_download)
            return url_link

        Parallel(n_jobs=4)(delayed(down_links)(link) for link in get_links())
        
    def unzip():
        import os
        import zipfile

        dir_name = '/opt/airflow/downloads/'
        extension = ".zip"

        os.chdir(dir_name) # change directory from working dir to dir with files

        for item in os.listdir(dir_name): # loop through items in dir
            if item.endswith(extension): # check for ".zip" extension
                file_name = os.path.abspath(item) # get full path of files
                zip_ref = zipfile.ZipFile(file_name) # create zipfile object
                zip_ref.extractall(dir_name) # extract file to dir
                zip_ref.close() # close file
                os.remove(file_name) # delete zipped file

    def to_sql():
        from datetime import date
        import pandas as pd
        import os
        from sqlalchemy import create_engine

        files = os.listdir('/opt/airflow/downloads/')
        today = date.today()
        month_year = today.strftime('%m%y')
        engine = create_engine('postgresql://datlo:S1g4#1331@quicksight.cbisoemmvdsb.us-east-2.rds.amazonaws.com:5432/trusted')

        for file in files:
            if 'SIMPLES' in file:
                table = 'table_cnpj_simples'
                header = ['cnpj_simples', 'opc_simples', 'dt_opc_simples', 'dt_exc_simples', 'opc_mei', 'dt_opc_mei', 'dt_exc_mei']

                df = pd.read_csv(f'/opt/airflow/downloads/{file}', dtype = str, delimiter = ';', encoding = 'latin1', header = 0, names = header)
                print(table + '_' + month_year)
                df.to_sql(table + '_' + month_year, engine, if_exists='replace', chunksize=10000,index=False)

            if 'SOCIO' in file:
                table = 'table_cnpj_socios'
                header = ['cnpj','id_socio','nome_socio', 'cnpj_cpf', 'qualificacao_socio', 'dt_entrada', 'pais', 'representante_legal', 'no_representante', 'quali_representante', 'faixa_etaria']

                df = pd.read_csv(f'/opt/airflow/downloads/{file}', dtype = str, delimiter = ';', encoding = 'latin1', header = 0, names = header)
                print(table + '_' + month_year)
                df.to_sql(table + '_' + month_year, engine, if_exists='append', chunksize=10000,index=False)

            if 'ESTAB' in file:
                table = 'table_cnpj_estab'
                header = ['cnpj','cnpj_ordem','cnpj_dv','matriz','nome_fantasia','situacao_cadastral','dt_sit_cadastral','motivo_sit_cadastral','no_cidade_exterior','co_pais','dt_ini_atividade','cnae_fiscal_principal','cnae_fiscal_secundaria','tipo_logradouro','logradouro','numero','complemento','bairro','cep','uf','co_mun','ddd_1','tel_1','ddd_2','tel_2','ddd_fax','tel_fax','email','situacao_especial','data_sit_especial']

                df = pd.read_csv(f'/opt/airflow/downloads/{file}', dtype=str, delimiter=';', encoding='latin1', header=0, names=header)
                print(table + '_' + month_year)
                df.to_sql(table + '_' + month_year, engine, if_exists='append', chunksize=10000,index=False)

            if 'EMP' in file:
                table = 'table_cnpj_emp'
                header = ['cnpj','razao_social','natureza_juridica','qualificacao_resp','capital_socia','porte','ente_federativo_resp']

                df = pd.read_csv(f'/opt/airflow/downloads/{file}', dtype = str, delimiter = ';', encoding = 'latin1', header = 0, names = header)
                print(table + '_' + month_year)
                df.to_sql(table + '_' + month_year, engine, if_exists='append', chunksize=10000,index=False)
            
            
    # [START DAGS]

    get_data = BranchPythonOperator(
        task_id="get_date",
        python_callable=get_date
    )

    new_data = DummyOperator(
        task_id="new_data",
    )

    no_new_data = DummyOperator(
        task_id="no_new_data",
    )

    download_data = PythonVirtualenvOperator(
            task_id="download_data",
            python_callable=paralel,
            requirements=["wget","joblib","requests","bs4"], #pip show requests
            system_site_packages=False,
    )
    
    unziped = PythonOperator(
            task_id="unzip",
            python_callable=unzip
        )

    to_psql = PythonVirtualenvOperator(
        task_id = 'to_psql',
        python_callable=to_sql,
        requirements=["datetime","pandas","sqlalchemy","psycopg2-binary"], #pip show requests
        system_site_packages=False,
    )

    get_data >> new_data >> download_data >> unziped >> to_psql
    get_data >> no_new_data
    