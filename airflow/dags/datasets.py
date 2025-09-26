from airflow.datasets import Dataset

# Este é o "sinal" que representa os dados na camada Silver.
# A URI é um identificador único, não precisa ser um caminho real que o Airflow acessa.
silver_sptrans_posicoes = Dataset("s3a://silver/posicoes_onibus")
