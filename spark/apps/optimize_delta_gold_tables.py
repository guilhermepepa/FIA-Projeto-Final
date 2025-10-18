from pyspark.sql import SparkSession
from delta.tables import *
from datetime import datetime

def log_info(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f',{now.microsecond // 1000:03d}'
    print(f"\n>>> [DELTA_MAINTENANCE_LOG]: {message}\n")

def main():
    spark = SparkSession.builder \
        .appName("Delta Lake Maintenance Job") \
        .getOrCreate()

    log_info("Sessão Spark iniciada para a tarefa de manutenção.")

    # --- Lista de tabelas Gold a serem otimizadas ---
    gold_tables = {
        "fato_posicao_onibus_atual": "prefixo_onibus",
        "fato_operacao_linhas_hora": "id_tempo, id_linha",
        "fato_velocidade_linha": "id_tempo, id_linha",
        "fato_onibus_parados_linha": "id_tempo, id_linha"
    }

    for table_name, zorder_cols_str in gold_tables.items():
        table_path = f"s3a://gold/{table_name}"
        log_info(f"Iniciando otimização para a tabela: {table_name} em {table_path}")

        try:
            if DeltaTable.isDeltaTable(spark, table_path):
                delta_table = DeltaTable.forPath(spark, table_path)
                
                # --- CORREÇÃO AQUI ---
                # 1. Converte a string de colunas em uma lista
                zorder_cols_list = [col.strip() for col in zorder_cols_str.split(',')]
                
                log_info(f"Executando OPTIMIZE e ZORDER BY ({zorder_cols_str})...")
                
                # 2. Usa o operador * para passar os itens da lista como argumentos separados
                delta_table.optimize().executeZOrderBy(*zorder_cols_list)
                
                log_info(f"Executando VACUUM na tabela {table_name}...")
                delta_table.vacuum() # Limpa versões antigas dos arquivos
                
                log_info(f"Otimização da tabela {table_name} concluída com sucesso.")
            else:
                log_info(f"A tabela {table_name} não foi encontrada ou não é uma tabela Delta. Pulando.")
        except Exception as e:
            log_info(f"Erro ao otimizar a tabela {table_name}: {e}")

    log_info("Trabalho de manutenção do Delta Lake concluído.")
    spark.stop()

if __name__ == "__main__":
    main()