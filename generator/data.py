# Imports necessários
import random
from pyspark.sql import SparkSession
from faker import Faker 
fake = Faker('pt_BR')
Faker.seed(1)

spark = SparkSession.builder.appName("DictToCSV").getOrCreate()

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