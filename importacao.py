import  json, os,  re, sqlalchemy as s, pandas as p
from urllib.parse import quote_plus
from datetime import datetime

def extrairParametro(json_config:any, chave:str):
    #funcao caso tenha tratativa
    try: 
        if json_config[chave] == "": return None
        else: return json_config[chave]
    except: return None

def listarArquivos(regex_arquivo:str):
    if regex_arquivo == None: return os.listdir()
    else:
        lista = os.listdir()
        r = re.compile(rf'{regex_arquivo}')
        lista = list(filter(r.match, lista))
    return lista

with open(f'.\\configuracao.json') as dados_arquivo:
    dados_json = json.load(dados_arquivo)
    servidor = extrairParametro(dados_json, 'servidor')
    usuario = extrairParametro(dados_json, 'usuario')
    senha = extrairParametro(dados_json, 'senha')
    regex_arquivo = extrairParametro(dados_json, 'regex_arquivo')
    banco_dados = extrairParametro(dados_json, 'banco_dados')
    separador = extrairParametro(dados_json, 'separador')
    tabela = extrairParametro(dados_json, 'tabela')
    del dados_json
s_conexao = f'mssql+pyodbc://{usuario}:{quote_plus(senha)}@{servidor}/{banco_dados}?driver=ODBC+Driver+17+for+SQL+Server'
print(f"String de conexao gerada: \n{s_conexao}")
engine = s.create_engine(s_conexao) 
tabela_criada = False
lista_arquivos_erro = []
lista_erros = []
dict_falhas = {}
with  engine.connect() as conn:
    
        lista_arquivos = listarArquivos(regex_arquivo)
        for arquivo in lista_arquivos:
            try:
                if arquivo[-4:] != '.csv':
                    print(f"{arquivo} nao sera importado, pois nao e CSV...")
                    continue
                df = p.read_csv(arquivo, sep=separador)
                print(f"Comecando importacao de {arquivo}")
                if tabela_criada == False:
                    dtype = {}
                    for coluna in df.columns:
                        dtype.update({coluna:s.types.Text})
                    tabela_criada = True
                    print(f'Metadados da tabela absorvidos de {arquivo}')
                df.to_sql(tabela, conn, if_exists='append', dtype=dtype, index=False)   
                print(f'Arquivo {arquivo} importado com sucesso.') 
            except Exception as e:
                print(f'{arquivo} nao foi importado... \nPulando arquivo {arquivo}...')
                lista_arquivos_erro.append(arquivo)
                lista_erros.append(repr(e).replace(';', '')[:200])
                dict_falhas.update({'arquivo':lista_arquivos_erro, 'erro':lista_erros})
                continue

print("Arquivos importados com sucesso. Fechando conexao...")
if len(lista_erros) > 0:
    print(f"Houveram falhas durante a importacao, um log foi gerado...")
    df = p.DataFrame.from_dict(dict_falhas)
    log = f'log_falhas_{datetime.now().strftime("%d%m%Y_%H%M%S")}.csv'
    df.to_csv(log, index=False, sep=';')
    print(f'{log} \nPressione Enter para finalizar o importador...')
    input()