from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

# --- Configurações de Conexão com o Banco de Dados ---
# (Lê as variáveis de ambiente, com valores padrão para o nosso docker-compose)
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "sptrans_dw")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "projetofinal")

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    return conn

app = FastAPI(
    title="Raio-X da Frota SPTrans API",
    description="Uma API que fornece insights operacionais e em tempo real sobre a frota de ônibus de São Paulo.",
    version="1.1.0"
)

# --- Modelos de Resposta (Pydantic) ---

class KpiResponse(BaseModel):
    valor: int

class KpiFloatResponse(BaseModel):
    valor: float

class LinhaRankingQuantidade(BaseModel):
    linha: str
    quantidade: int

class LinhaRankingVelocidade(BaseModel):
    linha: str
    velocidade_media_kph: float

class PosicaoOnibus(BaseModel):
    prefixo_onibus: int
    letreiro_linha: str
    latitude: float
    longitude: float
    horario_local_captura: str

# --- Endpoints da API ---

@app.get("/kpis/frota-ativa", response_model=KpiResponse, tags=["KPIs (Near Real Time)"])
def get_frota_ativa(conn=Depends(get_db_connection)):
    """Retorna o número total de ônibus em operação (com sinal nos últimos 5 minutos)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            WITH latest_timestamp AS (
                SELECT MAX(timestamp_captura) as max_ts_utc FROM nrt_posicao_onibus_atual
            )
            SELECT COUNT(prefixo_onibus) AS valor
            FROM nrt_posicao_onibus_atual
            WHERE timestamp_captura >= ((SELECT max_ts_utc FROM latest_timestamp) - INTERVAL '4 minutes');
        """
        cur.execute(query)
        result = cur.fetchone()
        if not result:
            return {"valor": 0}
        return result

@app.get("/kpis/frota-congestionada", response_model=KpiResponse, tags=["KPIs (Near Real Time)"])
def get_frota_congestionada(conn=Depends(get_db_connection)):
    """Retorna o número total de ônibus considerados parados/congestionados no último snapshot."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT COALESCE(SUM(quantidade_onibus_parados), 0) AS valor
            FROM nrt_onibus_parados_linha;
        """
        cur.execute(query)
        result = cur.fetchone()
        # Se o resultado for nulo (ex: tabela vazia), retorna 0
        return result if result and result['valor'] is not None else {"valor": 0}

@app.get("/kpis/velocidade-media-frota", response_model=KpiFloatResponse, tags=["KPIs (Near Real Time)"])
def get_velocidade_media(conn=Depends(get_db_connection)):
    """Retorna a velocidade média PONDERADA da frota, com base no último snapshot."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            WITH onibus_ativos_por_linha AS (
              SELECT
                dl.id_linha,
                COUNT(fpoa.prefixo_onibus) AS quantidade_onibus
              FROM
                nrt_posicao_onibus_atual fpoa
                JOIN dim_linha dl ON fpoa.letreiro_linha = dl.letreiro_linha
              WHERE
                fpoa.timestamp_captura >= ((SELECT MAX(timestamp_captura) FROM nrt_posicao_onibus_atual) - INTERVAL '4 minutes')
              GROUP BY
                dl.id_linha
            )
            SELECT
              COALESCE(
                ROUND(
                  (SUM(fvl.velocidade_media_kph * oa.quantidade_onibus)
                  / NULLIF(SUM(oa.quantidade_onibus), 0))::numeric
                , 2)
                , 0.0
              ) AS valor
            FROM
              nrt_velocidade_linha AS fvl
              JOIN onibus_ativos_por_linha AS oa ON fvl.id_linha = oa.id_linha
            WHERE
              fvl.velocidade_media_kph > 0
              AND oa.quantidade_onibus > 0;
        """
        cur.execute(query)
        result = cur.fetchone()
        return result if result and result['valor'] is not None else {"valor": 0.0}

@app.get("/rankings/linhas-mais-operantes", response_model=List[LinhaRankingQuantidade], tags=["Rankings Históricos (Batch)"])
def get_top_linhas_operantes(conn=Depends(get_db_connection)):
    """Retorna o ranking das 10 linhas com mais ônibus na última hora processada pelo pipeline de lote."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            WITH latest_time AS (
              SELECT MAX(id_tempo) AS latest_id_tempo FROM fato_operacao_linhas_hora
            )
            SELECT
              (dl.letreiro_linha || ' - ' || dl.nome_linha) AS linha,
              f.quantidade_onibus AS quantidade
            FROM fato_operacao_linhas_hora f
            JOIN dim_linha dl ON f.id_linha = dl.id_linha
            WHERE f.id_tempo = (SELECT latest_id_tempo FROM latest_time)
            ORDER BY f.quantidade_onibus DESC
            LIMIT 10;
        """
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Dados históricos de operação ainda não encontrados.")
        return result


@app.get("/rankings/linhas-mais-lentas", response_model=List[LinhaRankingVelocidade], tags=["Rankings (Near Real Time)"])
def get_top_linhas_lentas(conn=Depends(get_db_connection)):
    """Retorna o ranking das 10 linhas mais lentas, com base na velocidade operacional."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT
              (dl.letreiro_linha || ' - ' || dl.nome_linha) AS linha,
              ROUND(fvl.velocidade_media_kph::numeric, 2) AS velocidade_media_kph
            FROM
              nrt_velocidade_linha AS fvl
              JOIN dim_linha AS dl ON fvl.id_linha = dl.id_linha
            WHERE fvl.velocidade_media_kph > 0
            ORDER BY
              fvl.velocidade_media_kph ASC
            LIMIT 10;
        """
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Dados de velocidade ainda não encontrados.")
        return result

@app.get("/posicoes/ativas", response_model=List[PosicaoOnibus], tags=["Posições (Near Real Time)"])
def get_posicoes_ativas(conn=Depends(get_db_connection)):
    """Retorna a posição de todos os ônibus ativos nos últimos 5 minutos."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            WITH latest_timestamp AS (
                SELECT MAX(timestamp_captura) as max_ts_utc FROM nrt_posicao_onibus_atual
            )
            SELECT
              prefixo_onibus,
              letreiro_linha,
              latitude,
              longitude,
              to_char((timestamp_captura AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD HH24:MI:SS') AS horario_local_captura
            FROM nrt_posicao_onibus_atual
            WHERE timestamp_captura >= ((SELECT max_ts_utc FROM latest_timestamp) - INTERVAL '4 minutes');
        """
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Nenhum ônibus ativo encontrado.")
        return result

@app.get("/posicoes/por-linha", response_model=List[PosicaoOnibus], tags=["Posições (Near Real Time)"])
def get_posicoes_por_linha(letreiro_linha: str, conn=Depends(get_db_connection)):
    """Retorna a posição de todos os ônibus ativos de uma linha específica nos últimos 5 minutos."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            WITH latest_timestamp AS (
                SELECT MAX(timestamp_captura) as max_ts_utc FROM nrt_posicao_onibus_atual
            )
            SELECT
              prefixo_onibus,
              letreiro_linha,
              latitude,
              longitude,
              to_char((timestamp_captura AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'), 'YYYY-MM-DD HH24:MI:SS') AS horario_local_captura
            FROM nrt_posicao_onibus_atual
            WHERE timestamp_captura >= ((SELECT max_ts_utc FROM latest_timestamp) - INTERVAL '5 minutes')
              AND letreiro_linha = %s;
        """
        cur.execute(query, (letreiro_linha,))
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail=f"Nenhum ônibus ativo encontrado para a linha {letreiro_linha}.")
        return result