from utils import query,files
import os 
from datetime import datetime


# A seguir o código ia consultar uma quey para criar uma tabela analítica do questionário
#  desenvolvi uma função para conseguir rodar a mesmo query em tabelas diferentes (as tabelas precisam ser da mesma estrutura)
print('\n ---Running querys----\n')
retornoAP08 = query.Postgres.consultaPNAD(tblName='pnad_covid_082020',apelido='pd08')
print(f'pnad_covid_082020 gerada com sucesso : {datetime.now()}')

retornoAP09 = query.Postgres.consultaPNAD(tblName='pnad_covid_092020',apelido='pd09')
print(f'pnad_covid_09202 gerada com sucesso : {datetime.now()}')

retornoAP10 = query.Postgres.consultaPNAD(tblName='pnad_covid_102020',apelido='pd10')
print(f'pnad_covid_102020 gerada com sucesso : {datetime.now()} \n')


#Após rodar as querys salvo o data frame de retorno em um arquivo CSV 
print('\n ---saving csv files---- \n')
files.Manipulation.saveCSVDataFrame(df=retornoAP08,path=os.path.join(files.path.pathDBInput,'returnAnalisePNAD082020.csv'),sep=';')
files.Manipulation.saveCSVDataFrame(df=retornoAP09,path=os.path.join(files.path.pathDBInput,'returnAnalisePNAD092020.csv'),sep=';')
files.Manipulation.saveCSVDataFrame(df=retornoAP10,path=os.path.join(files.path.pathDBInput,'returnAnalisePNAD102020.csv'),sep=';')

#executo a função do bulk insert para inserir os arquivos CSV criados anteriormente 
print('\n ---importing csv files to Postgres--- \n')
query.Postgres.runCopyCSVCommand(filepath=os.path.join(files.path.pathDBInput,'returnAnalisePNAD082020.csv'),tableName='retorno_analise_pnad',sep=';',columns=list(retornoAP08.columns))
query.Postgres.runCopyCSVCommand(filepath=os.path.join(files.path.pathDBInput,'returnAnalisePNAD092020.csv'),tableName='retorno_analise_pnad',sep=';',columns=list(retornoAP09.columns))
query.Postgres.runCopyCSVCommand(filepath=os.path.join(files.path.pathDBInput,'returnAnalisePNAD102020.csv'),tableName='retorno_analise_pnad',sep=';',columns=list(retornoAP10.columns))

print('finalizdo')