import os, re, zipfile
from matplotlib import collections
from pandas   import DataFrame, read_csv
import pymongo



class Reader_CVM_dados():
  def __init__(self):
    self.config      = self.set_config()
    self.companies   = self.split_tables_per_company()

  @staticmethod
  def set_config():
    """Inicializa as configurcoes da classe 
    Returns:
        dict: dicionario com a chave e o valor do caminho-base para o reader.
    """
    return {'path': 'CVM/Dados/'}

  @staticmethod
  def _add_tables_to_cias(cias, key, groups):
    """Atribui tabelas as companhias
  Params: 
      cias:dict: dicionario de companhias que virá vazio pela primeira vez e sera preenchido neste metodo
      key:str a chave do item no dicionario, que é o nome da tabela .csv retirado o ano
      groups: 

  Returns:
      None: nao retorna nada, mas o dict "cias" é preenchido com a chave o CNPJ da empresa, e o valor é outro 
      dicionário, cuja chave é o nome da tabela tirada do .csv e o valor são os dataframes de varios anos correspondentes a esse .csv. 
      Dessa forma, cada CNPJ leva a um dicionario onde a chave é o nome da tabela na CVM e o valor são os dataframes, propriamente. Assim, é possível acessar a todos os dataframes correspondentes a um único CNPJ de acordo com a tabela CVM onde esse dataframe estava
      .csv
    """
    for g in groups:
      cnpj    = re.sub('\W','',g[0])
      if cnpj not in cias.keys():
        cias[cnpj] = dict()
        cias[cnpj][key] = g[1]
      else:
        cias[cnpj][key] = g[1]

  @staticmethod
  def _append_dataframe(
    dictionary:dict, zip_file:zipfile.ZipFile, file_name:str) -> None:
    """Adiciona um DataFrame ao dicionario de dados, cuja chave é o nome do .csv sem o ano, e o valor
    é um conjunto de dataframes tirado dos arquivos .csv de mesmo nome.
    Args:
        dictionary: dicionario de dados
        zip_file: arquivo zip
        file_name:  nome do arquivo .csv dentro do zip
    """
    df  = read_csv(
      zip_file.open(file_name, 'r'), encoding="latin", sep=";",on_bad_lines="skip")
    key = re.sub('.csv', '', file_name)
    if file_name not in dictionary.keys():
      dictionary[key] = DataFrame()
      dictionary[key] = dictionary[key].append(df)
    else:
      dictionary[key] = dictionary[key].append(df)

  def _get_zip_paths(self, path:str):
    """Produz uma lista de caminhos para os arquivos .zip

    Returns:
        List: uma lista de strings com nomes dos arquivos zip em que os arquivos .csv estão
    """
    return [os.path.join(path, i) for i in os.listdir(self.config['path']) if i.endswith('.zip')]

  def _unify_tables(self) -> dict:
    """Une todos os dataframes em um dicionario, onde a chave eh o nome do arquivo.csv sem o ano, 
    e o valor são os dataframes daquela tabela especifica em todos os anos 

    Returns:
        dict: "d" um dicionario de empresas e tabelas
    """
    d = {}
    paths = [zipfile.ZipFile(i) for i in self._get_zip_paths(self.config['path'])]
    # limitando o tamanho para trabalhar
    # paths2 = paths[:3] #não mais
    for z in paths:
      for i in z.namelist():
        if i.endswith('.csv'):
          self._append_dataframe(d, z, i)
    return d

  def split_tables_per_company(self) -> dict:
    """Separar as tabelas por empresa apos unificar todas as tabelas por ano.

    Returns:
        dict: dicionario de empresas e tabelas
    """
    d = self._unify_tables()
    cias = dict()
    for k,v in d.items():
      try:
        groups = v.groupby('CNPJ_Companhia')
      except:
        groups = v.groupby('CNPJ_CIA')
      self._add_tables_to_cias(cias, k, groups)
    return cias


def insert_all_in_mongo_db( companies:tuple, client):
  """Insere os dados de todas as companhias no servidor local de MongoDB
  Params: 
      companies:dict: dicionario de companhias que virá preenchido por CNPJ

  Returns:
      None: nada, mas realiza a inserção completa dos dados no servidor MongoDB
  """
  for c in companies.items(): 
    db_name = f'cnpj_{(c[0])}'
    db = client[db_name]
    dict_dfs = c[1] 
    exsiting_collections = db.list_collection_names()
    for i in dict_dfs: 
      if i not in exsiting_collections:
        insert_single_df_in_mongo_db(dict_dfs, db, i)


def insert_single_df_in_mongo_db( dict_dfs:dict, db:pymongo.database.Database, i:str):   
    collection = db[i]
    df = dict_dfs[i]
    try:
      db[collection.name].insert_many(df.to_dict('records'))
    except Exception:
      print(Exception)  
 

instance = Reader_CVM_dados()
mongoClient = pymongo.MongoClient('localhost', 27017)
insert_all_in_mongo_db(instance.companies, mongoClient)






