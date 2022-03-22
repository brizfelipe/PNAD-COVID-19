import psycopg2
from . import connection
import pandas as pd 
from datetime import datetime
class Postgres:

    def runCopyCSVCommand(filepath, tableName, sep, columns=False):

        # cria a conexão com o banco de dados apartir das credenciais do banco
        conn = psycopg2.connect(dbname=connection.AcessPostgres.dbName
                                ,user=connection.AcessPostgres.user
                                ,password=connection.AcessPostgres.password
                                ,host=connection.AcessPostgres.host
                                ,port=connection.AcessPostgres.port)
        
        #desligo o auto commit, para tentar rodar a operação com sucesso, caso contrario, não ia alterar nada do banco        
        conn.autocommit = False
        
        #criando o cursor para rodar a query
        cursor = conn.cursor()
        columnsCorrigido:list = []

        #abrindo o arquivo CSV
        with open(filepath) as f:
            try:
                #caso nao declare nehum nome de coluna a varial ficara vazia
                if not columns or len(columns) == 0:
                    columns = ''
                #caso os nomes de coluna sejam declarado, eles serão organizado e feito um join separando os nomes por virgula
                else:
                    for column in columns:
                        columnsCorrigido.append('"'+column+'"')
                        
                    columns = '(' + ','.join(columnsCorrigido) + ')'

                #cursor que axecuta a função SQL
                cursor.copy_expert('copy public.' + tableName + ' ' + columns + ' from stdin with csv delimiter as ' + "\'" + sep + "\'" + " encoding \'UTF-8\' ", f)

            #caso aconteça algum erro na execução da função, ele retornara um print com o erro ocorrido
            except psycopg2.DatabaseError as err:
                conn.rollback()
                return print('Base import error : '+str(err))
        #se tudo der certo, faço commit no SQL
        conn.commit()
        #fecho o cursor
        cursor.close()
        # e religo o auto commit
        conn.autocommit = True
        #retorno uma mensagem de sucesso com o nome da tabela e o tempo que demorou para rodar a query
        return  print(f'successfully imported base: {tableName} time: {datetime.now()}')
    

    def consultaPNAD(tblName,apelido):

        conn = psycopg2.connect(dbname=connection.AcessPostgres.dbName,user=connection.AcessPostgres.user,password=connection.AcessPostgres.password,host=connection.AcessPostgres.host,port=connection.AcessPostgres.port)
        cursor = conn.cursor()

        query = f"""
            SELECT 
                --localidade
                ano as "Ano Referencia", 
                uf.descricao as uf, 
                c.descricao as capital, 
                rm_ride,

                case when v1012 = 1 then 'Urbana' when v1012 = 2 then 'Rural' when v1012 = 3 then 'Capital'
                end as  "Situacao Domicilio",

                a001b1 as "Dia Nascimento",
                a001b2 as "Mes Nascimento",
                a001b3 as "Ano Nascimento",
                a002 as "Idade do Morador",
                
                case when a003 = '1' then 'Homen' when a003 = '2' then 'Mulher'
                end AS "Sexo do morador",
                
                case
                    when a004 = '1' then 'Branca'
                    when a004 = '2' then 'Preta'
                    when a004 = '3' then 'Amarela'
                    when a004 = '4' then 'Parda'
                    when a004 = '5' then 'indigena'
                    when a004 = '9' then 'Ignorado'
                end as "Cor ou Raca",
                
                case
                    when a005 = '1' then 'Sem instrucao'
                    when a005 = '2' then 'Fundamental Incompleto'
                    when a005 = '3' then 'Fundamental Completo'
                    when a005 = '4' then 'Medio Inconpleto'
                    when a005 = '5' then 'Medio Completo'
                    when a005 = '6' then 'Superior Incompleto'
                    when a005 = '7' then 'Superior Completo'
                    when a005 = '8' then 'Pos Graduacao, Mestre ou Doutor'
                end as "Escolaridade",

                case when a006 = '1' then 'Sim' when a006 = '2' then 'Nao'
                end as "Frequenta a escola",
                --sintomas
                case when b0011 = '1' then 'Sim' when b0011 = '2' then 'Nao' when b0011 = '3' then 'Nao Sabe'
                end as "Febre",
                
                case when b0012 = '1' then 'Sim' when b0012 = '2' then 'Nao' when b0012 = '3' then 'Nao Sabe'
                end as "Tosse",

                case when b0013 = '1' then 'Sim' when b0013 = '2' then 'Nao' when b0013 = '3' then 'Nao Sabe'
                end as "Dor de garganta",
                
                case when b0014 = '1' then 'Sim' when b0014 = '2' then 'Nao' when b0014 = '3' then 'Nao Sabe'
                end as "Dificuldade para respirar",
                
                case when b0015 = '1' then 'Sim' when b0015 = '2' then 'Nao' when b0015 = '3' then 'Nao Sabe'
                end as "Dor de cabeca",

                case when b0016 = '1' then 'Sim' when b0016 = '2' then 'Nao' when b0016 = '3' then 'Nao Sabe'
                end as "Dor no peito",
                
                case when b0017 = '1' then 'Sim' when b0017 = '2' then 'Nao' when b0017 = '3' then 'Nao Sabe'
                end as "Nausea",

                case when b0018 = '1' then 'Sim' when b0018 = '2' then 'Nao' when b0018 = '3' then 'Nao Sabe'
                end as "Nariz entupido ou escorrendo",

                case when b0019 = '1' then 'Sim' when b0019 = '2' then 'Nao' when b0019 = '3' then 'Nao Sabe'
                end as "Fatiga",

                case when b00110 = '1' then 'Sim' when b00110 = '2' then 'Nao' when b00110 = '3' then 'Nao Sabe'
                end as "Dor nos olhos",

                case when b00111 = '1' then 'Sim' when b00111 = '2' then 'Nao' when b00111 = '3' then 'Nao Sabe'
                end as "Perda de cheiro ou sabor",

                case when b00112 = '1' then 'Sim' when b00112 = '2' then 'Nao' when b00112 = '3' then 'Nao Sabe'
                end as "Dor muscular",

                case when b009a = '1' then 'Sim' when b009a = '1' then 'Nao'
                end as "exameCotonete",

                case 
                    when b009b = '1' then 'Positivo' 
                    when b009b = '2' then 'Negativo' 
                    when b009b = '3' then 'Inconclusivo' 
                    when b009b = '4' then 'Ainda nao recebeu o resultado' 
                    when b009b = '5' then 'Nao aplicado'
                 end as "resultadoExameCotonete",
                
                case when b009c = '1' then 'Sim' when b009c = '1' then 'Nao'
                end as "exameSangueDedo",

                case 
                    when b009d = '1' then 'Positivo' 
                    when b009d = '2' then 'Negativo' 
                    when b009d = '3' then 'Imconclusivo' 
                    when b009d = '4' then 'Ainda nao recebeu o resultado' 
                    when b009d = '5' then 'Nao aplicado'
                 end as "resultadoExameSangueDedo"
        
        FROM public.{tblName} AS {apelido}
            left join public.uf as uf on {apelido}.uf = uf.numero
            left join public.capital as c on {apelido}.capital = c.numero
        """

        #executa a query anterior
        cursor.execute(query)
        #método de transformar a query rodada em uma lista
        retornoQuery:list = cursor.fetchall()
        #pega o cabeçalho da query rodada
        colnames = [desc[0] for desc in cursor.description]
        #transformar a lista da query em um data frame usando a biblioteca pandas
        df  = pd.DataFrame(data=retornoQuery,columns=colnames)
        #o pandas trabalha com os valores NULL utilizando o tipo de dados Nan, aqui eu substituo os valores Nan para vazio 
        retorno = df.fillna('')
        #crio uma nova coluna do data frame criado com o mês base que esta no nome da tabela, apenas separo ela pelos '_' e pego a terceiro  estrutura da string
        retorno['database']= tblName.split('_')[2]

        #a função retorna o data frame
        return retorno

    view = """
     create view "v_Positivos" as 
        SELECT 
            "Ano Referencia",
            "uf",
            "capital",
            "rm_ride",
            "Situacao Domicilio",
            "Dia Nascimento",
            "Mes Nascimento",
            "Ano Nascimento",
            "Idade do Morador",
            "Sexo do morador",
            "Cor ou Raca",
            "Escolaridade",
            "Frequenta a escola",
            "Febre",
            "Tosse",
            "Dor de garganta",
            "Dificuldade para respirar",
            "Dor de cabeca",
            "Dor no peito",
            "Nausea",
            "Nariz entupido ou escorrendo",
            "Fatiga",
            "Dor nos olhos",
            "Perda de cheiro ou sabor",
            "Dor muscular",
            "exameCotonete",
            "resultadoExameCotonete",
            "exameSangueDedo",
            "resultadoExameSangueDedo",
            "database",
                CASE
                    WHEN "resultadoExameSangueDedo" = 'Positivo' THEN 'Sangue'
                    WHEN "resultadoExameCotonete" = 'Positivo' THEN 'Cotonete'
                    ELSE NULL
                END AS "TipoExame",
                CASE
                    WHEN "Febre" = 'Sim' THEN 'Febre'
                    WHEN "Dor de cabeca" = 'Sim' THEN 'Dor de cabeça'
                    WHEN "Fatiga" = 'Sim' THEN 'Fatiga'
                    WHEN "Dor muscular" = 'Sim' THEN 'Dor no musculo'
                    WHEN "Tosse" = 'Sim' THEN 'Tosse'
                    WHEN "Nausea" = 'Sim' THEN 'Nausea'
                    WHEN "Dificuldade para respirar" = 'Sim' THEN 'Dificuldade para respirar'
                    WHEN "Nariz entupido ou escorrendo" = 'Sim' THEN 'Nariz entupido ou escorrendo'
                    WHEN "Dor de garganta" = 'Sim' THEN 'Dor de garganta'
                    WHEN "Dor no peito" = 'Sim' THEN 'Dor no peito'
                    WHEN "Nariz entupido ou escorrendo" = 'Sim' THEN 'Nariz entupido ou escorrendo'
                    WHEN "Dor nos olhos" = 'Sim' THEN 'Dor nos olhos'
                    WHEN "Perda de cheiro ou sabor" = 'Sim' THEN 'Perda de cheiro ou sabor'
                    ELSE 'Sem sintomas'
                END AS "Sintomas"
        FROM retorno_analise_pnad
        WHERE retorno_analise_pnad."resultadoExameSangueDedo" = 'Positivo' OR retorno_analise_pnad."resultadoExameCotonete" = 'Positivo';
    """