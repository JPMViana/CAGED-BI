import ftplib
import os
import py7zr
import pandas as pd
import numpy as np

# CONNECT TO FTP
ftp_host = 'ftp.mtps.gov.br'
ftp = ftplib.FTP(ftp_host)
ftp.login()
ftp.cwd('pdet/microdados/CAGED/')

# DECLARE A FUNCTION TO DOWNLOAD FILES FROM FTP SERVER 
# AND EXTRACT(.7z) THEM TO A TEMPORARY FOLDER
def downloadAndExtract(ftp, filename, path):
    tempfile_path = path + "/tmp/" + filename
    extracted_path = path + "/tmp/extracted"
    # print(tempfile_path, "\n", extracted_path)
    with open(tempfile_path, 'wb') as f:
        ftp.retrbinary(f'RETR {filename}', f.write)
        print(f"Downloaded {filename}")
    py7zr.SevenZipFile(tempfile_path, mode='r').extractall(path=extracted_path)
    print(f"Extracted {filename}")

# CREATE A TEMPORARY FOLDER
if not os.path.exists('tmp'):
    os.makedirs('tmp')

# FILTER FOLDERS BY YEAR AND CALL DOWNALOADANDEXTRACT FUNCTION
filter = ['2019', '2018']
path = os.getcwd()
for dir in filter:
    ftp.cwd(dir)
    files = []
    ftp.retrlines('LIST', files.append)
    for file in files:
        print(f'Tryng to download {file}')
        downloadAndExtract(ftp, file.split()[3], path)
        # print(file.split()[3])
    ftp.cwd('..')
with open(path + "/tmp/CAGEDEST_layout_Atualizado.xls", 'wb') as f:
        ftp.retrbinary('RETR CAGEDEST_layout_Atualizado.xls', f.write)
        print("Downloaded CAGEDEST_layout_Atualizado.xls")

# CREATE A DATAFRAME FROM THE Municipio TABLE
df_mun = pd.read_excel('./tmp/CAGEDEST_layout_Atualizado.xls', sheet_name= 'municipio')

# SPLIT THE COLUM MUNICIPIO INTO THREE COLUMNS:'Município', 'MunicípioStr' AND 'UFStr' BY THE ":" AND "-" SEPARATORS
df_mun[['Município', 'MunicípioStr']] = df_mun['Município'].str.split(':', expand=True, n=1)
df_mun[['UFStr', 'MunicípioStr']] = df_mun['MunicípioStr'].str.split('-', expand=True, n=1)

# CHANGING DATA TYPES
df_mun['Município'] = df_mun['Município'].astype('int64')
df_mun['UFStr'] = df_mun['UFStr'].astype('str')
df_mun['MunicípioStr'] = df_mun['MunicípioStr'].astype('str')

# DEFINE CAPITALS CITY NAMES
capitals = ['Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza',
            'Brasília', 'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande',
            'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina',
            'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista',
            'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas']

# READ .TXT FILES AS .CSV
path = os.getcwd() + "/tmp/extracted/"
dataFrames=[]
for root, dirs, files in os.walk(path):
    for file in files:
        dfTmp= pd.read_csv(path + file, encoding='latin-1', sep=';')

        # DROP NOT USABLE COLUMNS
        dfTmp.drop(columns=[
            'IBGE Subsetor',
            'UF',
            'Bairros SP',
            'Bairros Fortaleza',
            'Bairros RJ',
            'Distritos SP',
            'Regiões Adm DF',
            'Mesorregião',
            'Microrregião',
            'Região Adm RJ',
            'Região Adm SP',
            'Região Corede',
            'Região Corede 04',
            'Região Gov SP',
            'Região Senac PR',
            'Região Senai PR',
            'Região Senai SP',
            'Sub-Região Senai PR'            
        ], inplace=True)

        # FILTER THE DATAFRAME TO INCLUDE ONLY THE CAPITALS
        dfTmp = pd.merge(dfTmp, df_mun, left_on='Município', right_on='Município', how='left')
        dfTmp = dfTmp.loc[dfTmp['MunicípioStr'].isin(capitals),:]
        
        #CONCATENATE ALL INTO A SINGLE PANDAS DATAFRAME
        dataFrames.append(dfTmp)
df = pd.concat(dataFrames, ignore_index=True)
dataFrames.clear()

# CLEANING AND FORMATING COLUMNS THAT HAD DATA TYPE 'Object'
df.loc[df['Grau Instrução']=='{ñ','Grau Instrução'] = 7
df['Salário Mensal'] = df['Salário Mensal'].str.replace(',','.')
df['Tempo Emprego'] = df['Tempo Emprego'].str.replace(',','.')

# CHANGING DATA TYPES
df['Grau Instrução'] = df['Grau Instrução'].astype('int64')
df['Salário Mensal'] = df['Salário Mensal'].astype('float64')
df['Tempo Emprego'] = df['Tempo Emprego'].astype('float64').astype('int64')

# SAVE DATAFRAME (.CSV AND .PARQUET)
df.to_parquet('caged.parquet')
df.to_csv('caged.csv')