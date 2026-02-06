# Imports necessários
import random
from pyspark.sql import SparkSession
from faker import Faker 
import os
import shutil
import glob
fake = Faker('pt_BR')

spark = SparkSession.builder.appName("DictToCSV").getOrCreate()

# Gerar dados de clientes

def generate_client_data(num_clients):
    clients = []
    for _ in range(num_clients):
        cliente_id = random.randint(1000, 9999)
        nome = fake.name()
        cpf = fake.cpf().replace('.', '').replace('-', '')
        data_nascimento = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
        sexo = random.choice(['M', 'F', 'O'])
        renda_mensal = round(random.uniform(1000.00, 20000.00), 2)
        score_credito = random.randint(300, 850)
        data_cadastro = fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')

        client = {
            'cliente_id': cliente_id,
            'nome': nome,
            'cpf': cpf,
            'data_nascimento': data_nascimento,
            'sexo': sexo,
            'renda_mensal': renda_mensal,
            'score_credito': score_credito,
            'data_cadastro': data_cadastro
        }
        clients.append(client)
    return clients


# Gerar dados de endereços

def generate_address_data(num_addresses):
    addresses = []
    for _ in range(num_addresses):
        endereco_id = random.randint(501, 892)
        cliente_id = random.randint(1000, 1999)
        logradouro = fake.street_address()
        cidade = fake.city()
        estado = fake.state_abbr()
        cep = fake.postcode()

        address = {
            'endereco_id': endereco_id,
            'cliente_id': cliente_id,
            'logradouro': logradouro,
            'cidade': cidade,
            'estado': estado,
            'cep': cep
        }
        addresses.append(address)
    return addresses

# Gerar contas

def generate_account_data(num_accounts):
    accounts = []
    for _ in range(num_accounts):
        conta_id = random.randint(10000, 99999)
        cliente_id = random.randint(1000, 1999)
        tipo_conta = random.choice(['Corrente', 'Poupança', 'Salario'])
        agencia = random.randint(1000, 9999)
        saldo_atual = round(random.uniform(0.00, 100000.00), 2)
        status = random.choice(['Ativa', 'Inativa', 'Encerrada'])
        data_abertura = fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')

        account = {
            'conta_id': conta_id,
            'cliente_id': cliente_id,
            'tipo_conta': tipo_conta,
            'agencia': agencia,
            'saldo_atual': saldo_atual,
            'data_abertura': data_abertura
        }
        accounts.append(account)
    return accounts


# Gerar cartões, foi adicionado algumas condições para os cartões de crédito, como limite total e limite disponível, 
# para os cartões de débito esses campos ficam nulos, pros valores ficarem mais realistas.

def generate_card_data(num_cards):
    cards = []
    for _ in range(num_cards):
        cartao_id = random.randint(100000, 999999)
        cliente_id = random.randint(1000, 1999)
        tipo_cartao = random.choice(['Crédito', 'Débito'])
        if tipo_cartao == 'Crédito':
            limite_total = round(random.uniform(1000.00, 20000.00), 2)
            limite_disponivel = round(random.uniform(0.00, limite_total), 2)
        else:
            limite_total = None
            limite_disponivel = None

        card = {
            'cartao_id': cartao_id,
            'cliente_id': cliente_id,
            'tipo_cartao': tipo_cartao,
            'limite_total': limite_total,
            'limite_disponivel': limite_disponivel,
        }
        cards.append(card)
    return cards

# Gerar transações 

def generate_transaction_data(num_transactions):
    transactions = []
    for _ in range(num_transactions):
        transacao_id = random.randint(1000000, 9999999)
        conta_id = random.randint(10000, 99999)
        valor = round(random.uniform(10.00, 5000.00), 2)
        tipo_transacao = random.choice(['Débito', 'Crédito'])
        data_transacao = fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        canal = random.choice(['Internet Banking', 'Mobile Banking', 'Caixa Eletrônico', 'Agência'])
        status = random.choice(['Aprovada', 'Recusada', 'Pendente'])

        transaction = {
            'transacao_id': transacao_id,
            'conta_id': conta_id,
            'valor': valor,
            'tipo_transacao': tipo_transacao,
            'data_transacao': data_transacao,
            'canal': canal,
            'status': status
        }
        transactions.append(transaction)
    return transactions

# Gerar contratos de credito, para os contratos de crédito, o valor total é definido com base no tipo de crédito, 
# para deixar os dados mais realistas,

def generate_credit_contract_data(num_contracts):
    contracts = []
    for _ in range(num_contracts):
        contrato_id = random.randint(10000000, 99999999)
        cliente_id = random.randint(1000, 1999)
        tipo_credito = random.choice(['Pessoal', 'Consignado', 'Imobiliário', 'Veicular'])

        if tipo_credito == 'Pessoal':
            valor_total = round(random.uniform(5000.00, 50000.00), 2)
        elif tipo_credito == 'Consignado':
            valor_total = round(random.uniform(10000.00, 100000.00), 2)
        elif tipo_credito == 'Imobiliário':
            valor_total = round(random.uniform(20000.00, 500000.00), 2)
        else: 
            valor_total = round(random.uniform(5000.00, 100000.00), 2)

        taxa_juros = round(random.uniform(4.0, 10.0), 2)
        prazo_meses = random.choice([12, 24, 36, 48, 60])
        status = random.choice(['Ativo', 'Quitado', 'Inadimplente'])
        data_inicio = fake.date_time_between(start_date='-5y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
        
        contract = {
            'contrato_id': contrato_id,
            'cliente_id': cliente_id,
            'tipo_credito': tipo_credito,
            'valor_total': valor_total,
            'taxa_juros': taxa_juros,
            'prazo_meses': prazo_meses,
            'status': status,
            'data_inicio': data_inicio,
            
        }
        contracts.append(contract)
    return contracts

'''
Função para salvar DataFrame em CSV

    Para fins de estudos foi utilizado o método coalesce(1) para garantir que o DataFrame 
seja salvo em um único arquivo CSV, pra facilitar o processo de renomeação e organização dos arquivos.
    Obs: Tenho consciência de que esse método pode não ser eficiente para grandes volumes de dados, 
pois pode causar sobrecarga de memória e reduzir o desempenho, fiz dessa forma pra efeitos de estudo e 
para garantir a organização dos arquivos, mas em um cenário real, seria recomendado utilizar uma abordagem mais escalável,
como salvar os dados em múltiplos arquivos CSV ou utilizar um formato de arquivo mais eficiente, como Parquet.
'''

def save_df_to_csv(df, output_path):
   df = spark.createDataFrame(df)
   df_to_csv = df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

   return df_to_csv

'''
Função para renomear os arquivos CSV gerados pelo Spark

    O Spark salva os arquivos com um nome padrão como part-00000-xxxx.csv, pra manipular os arquivos e 
    renomeá-los de forma mais organizada, foi criado essa função que percorre os arquivos gerados e renomeia 
    cada arquivo para o nome do csv correspondente, e depois remove os arquivos temporários gerados pelo Spark
    pra deixar a estrutura de arquivos mais limpa e organizada.
'''

def rename_csvs(base_tmp_path, final_path):

    os.makedirs(final_path, exist_ok=True)

    for file in os.listdir(base_tmp_path):
        csv_path = os.path.join(base_tmp_path, file)

        csv_file = glob.glob(f"{csv_path}/*.csv")[0]
        shutil.move(
            csv_file,
            os.path.join(final_path, f"{file}.csv")
        )
        shutil.rmtree(csv_path)