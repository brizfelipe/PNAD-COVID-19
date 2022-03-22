#biblioteca para rodar comandos do MS-DOS 
import os 
#objetos estanciados para auxiliar no código
from utils import files,query

#Parte do código exclusiva para importar os arquivos CSV para suas respectivas tabelas
#  escolhi usar o método de importação bulk insert, por acreditar ser um dos métodos mais rápido para grande volumes de dados


#For para pegar cada nome dentro de um diretório
for name in os.listdir(files.path.pathDBInput):

    #nome do arquivo, pego do nome do arquico antes o '.' exemplo, se o arquivo chama 'saida.CSV' ele apena pegara o 'saida'
    tblName = name.split('.')[0]

    #indentificador de nome de arquivo para achar o arquivo da pesquisa
    #caso o arquivo dentro do looping seja o arquivo da pesquisa, ele entrava dentro do if
    if name.split('_')[0] == 'PNAD':

        #noma das colunas do arquivo da pesquisa
        columnsName = 'ano,uf,capital,rm_ride,v1008,v1012,v1013,v1016,estrato,upa,v1022,v1023,v1030,v1031,v1032,posest,a001,a001a,a001b1,a001b2,a001b3,a002,a003,a004,a005,a006,a007,a008,a009,b0011,b0012,b0013,b0014,b0015,b0016,b0017,b0018,b0019,b00110,b00111,b00112,b00113,b002,b0031,b0032,b0033,b0034,b0035,b0036,b0037,b0041,b0042,b0043,b0044,b0045,b0046,b005,b006,b007,b008,b009a,b009b,b009c,b009d,b009e,b009f,b0101,b0102,b0103,b0104,b0105,b0106,b011,c001,c002,c003,c004,c005,c0051,c0052,c0053,c006,c007,c007a,c007b,c007c,c007d,c007e,c007e1,c007e2,c007f,c008,c009,c009a,c010,c0101,c01011,c01012,c0102,c01021,c01022,c0103,c0104,c011a,c011a1,c011a11,c011a12,c011a2,c011a21,c011a22,c012,c013,c014,c015,c016,c017a,d0011,d0013,d0021,d0023,d0031,d0033,d0041,d0043,d0051,d0053,d0061,d0063,d0071,d0073,e001,e0021,e0022,e0023,e0024,f001,f0021,f0022,f002a1,f002a2,f002a3,f002a4,f002a5,f0061,f006'
        
        #função para inserir arquivos CSV no banco de dados SQL
        query.Postgres.runCopyCSVCommand(filepath=os.path.join(files.path.pathDBInput,name),tableName=tblName,sep=',',columns=columnsName.split(','))    
   
    #caso contrario, para as demais tabelas, ele usara os seguintes nomes de coluna padrão ['numero','descrição']
    else:
        columnsName = ['numero','descricao']
        
        #função para inserir arquivos CSV no banco de dados SQL
        query.Postgres.runCopyCSVCommand(filepath=os.path.join(files.path.pathDBInput,name),tableName=tblName,sep=';',columns=columnsName)    