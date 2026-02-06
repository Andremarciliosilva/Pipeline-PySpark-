import data

RAW_PATH = "data/raw"
NUM_REGISTROS = 1000

'''
 Função principal para gerar os dados e salvar em CSVs, a função main é o ponto de entrada 
do script, onde são chamadas as funções de geração de dados para cada tabela, 
depois os dados são salvos em arquivos CSV usando a função save_df_to_csv. 
Após salvar os arquivos, a função rename_csvs é chamada para organizar os arquivos gerados pelo Spark, 
renomeando-os de forma mais clara e removendo os arquivos temporários.
'''

def main():
    print("Gerando dados bancários fake...")

    data.save_df_to_csv(
        data.generate_client_data(NUM_REGISTROS),
        f"{RAW_PATH}/clientes"
    )

    data.save_df_to_csv(
        data.generate_address_data(NUM_REGISTROS),
        f"{RAW_PATH}/enderecos"
    )

    data.save_df_to_csv(
        data.generate_account_data(NUM_REGISTROS),
        f"{RAW_PATH}/contas"
    )

    data.save_df_to_csv(
        data.generate_card_data(NUM_REGISTROS),
        f"{RAW_PATH}/cartoes"
    )

    data.save_df_to_csv(
        data.generate_transaction_data(NUM_REGISTROS),
        f"{RAW_PATH}/transacoes"
    )

    data.save_df_to_csv(
        data.generate_credit_contract_data(NUM_REGISTROS),
        f"{RAW_PATH}/contratos_credito"
    )

    data.rename_csvs(
        base_tmp_path=RAW_PATH,
        final_path=RAW_PATH
    )

    print("Dados gerados com sucesso!")


# “Uso if __name__ == "__main__" para garantir que o script execute apenas 
# quando chamado diretamente, evitando efeitos colaterais ao importar módulos.”

if __name__ == "__main__":
    main()