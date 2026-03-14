import json
import os
import re
import shutil
import requests
import pandas as pd
import pytz
# editar datas
BASE_URL = "https://pcmbm.zamp.com.br/api/1.1/obj/"
TABELA   = "tabela_orc"
MES_INICIO = 3
ANO_INICIO = 2026
MES_FIM    = 3
ANO_FIM    = 2026
PASTA_DOWNLOADS  = r"C:\Users\giovanne.silva\Downloads"
PASTA_SHAREPOINT = r"C:\Users\giovanne.silva\OneDrive - BK Brasil\Gerencia Manutenção BU BK - 03.Março"
MAPA_REGIONAL = {
    "RCL": "CENTRO LESTE",
    "RCO": "CENTRO OESTE",
    "RNE": "NE",
    "RRJ": "RJ",
    "SCN": "SP CENTRO NORTE",
    "SIN": "SP INTERIOR NORTE",
    "SIS": "SP INTERIOR SUL",
    "SPL": "SP LESTE",
    "SPS": "SP SUL",
    "SUL": "SUL",
}
COLUNAS_FIXAS = [
    "_id", "month", "loja", "regional", "empresa", "fornecedor",
    "chamado", "status-txt", "status_number", "valor_total",
    "valor_mo", "valor_ma", "Created Date", "Modified Date","classe","ordem_text",
]
COLUNAS_DATA_IDX = [12, 13, 28, 35]
STATUS_PERMITIDOS = ["Validado", "Aprovado", "Aguardando Validação"]
FUSO_SP = pytz.timezone("America/Sao_Paulo")
ILLEGAL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')
def limpar_valor(val):
    if isinstance(val, str):
        return ILLEGAL_CHARS.sub(' ', val)
    return val
def gerar_periodos(mes_ini, ano_ini, mes_fim, ano_fim):
    periodos = []
    mes, ano = mes_ini, ano_ini
    while (ano, mes) <= (ano_fim, mes_fim):
        periodos.append((mes, ano))
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return periodos
def converter_fuso(series, fmt="%d/%m/%Y %H:%M:%S"):
    series = pd.to_datetime(series, errors="coerce")
    if series.dt.tz is not None:
        series = series.dt.tz_convert(FUSO_SP).dt.tz_localize(None)
    else:
        series = (series.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
                        .dt.tz_convert(FUSO_SP)
                        .dt.tz_localize(None))
    return series, series.dt.strftime(fmt)
def extrair_dados(periodos):
    todos = []
    for mes, ano in periodos:
        month = f"{mes}-{ano}"
        cursor = 0
        print(f"── Extraindo {month} ──")
        while True:
            params = {
                "cursor": cursor,
                "constraints": json.dumps([{
                    "key": "month",
                    "constraint_type": "equals",
                    "value": month,
                }]),
            }
            resp = requests.get(f"{BASE_URL}/{TABELA}", params=params, timeout=30)
            if resp.status_code != 200:
                print(f"  Erro {resp.status_code}: {resp.text}")
                break
            data      = resp.json().get("response", {})
            results   = data.get("results", [])
            remaining = data.get("remaining", 0)
            print(f"  cursor={cursor:>6} | retornados={len(results)} | restantes={remaining}")
            if not results:
                break
            todos.extend(results)
            cursor += 100
            if remaining == 0:
                break
    return todos
def main():
    periodos = gerar_periodos(MES_INICIO, ANO_INICIO, MES_FIM, ANO_FIM)
    print(f"Períodos: {[f'{m}-{a}' for m, a in periodos]}\n")
    todos = extrair_dados(periodos)
    if not todos:
        print("Nenhum dado encontrado.")
        return
    df = pd.DataFrame(todos).map(limpar_valor)
    outras = [c for c in df.columns if c not in COLUNAS_FIXAS]
    ordem  = [c for c in COLUNAS_FIXAS if c in df.columns] + outras
    df     = df[ordem]
    if "regional" in df.columns:
        df = df[~df["regional"].astype(str).str.contains("PLK", case=False, na=False)]
    if "status-txt" in df.columns:
        df = df[df["status-txt"].astype(str).isin(STATUS_PERMITIDOS)]
    for idx in COLUNAS_DATA_IDX:
        if idx < len(df.columns):
            col = df.columns[idx]
            _, df[col] = converter_fuso(df[col])
    if "date_aproval" in df.columns:
        df["date_aproval"], _ = converter_fuso(df["date_aproval"])
        df = df[df["date_aproval"].dt.year == 2026]
        df["date_aproval"] = df["date_aproval"].dt.strftime("%d/%m/%Y")
    if "regional" in df.columns:
        df["regional_corrigida"] = df["regional"].map(MAPA_REGIONAL).fillna(df["regional"])
    nome_arquivo = f"Dados_{TABELA}_{MES_INICIO}_{ANO_INICIO}_a_{MES_FIM}_{ANO_FIM}.xlsx"
    arquivo = os.path.join(PASTA_DOWNLOADS, nome_arquivo)
    df.to_excel(arquivo, index=False, engine="openpyxl")
    print(f"\nTotal: {len(df)} registros")
    print(f"Arquivo: {arquivo}")
    os.makedirs(PASTA_SHAREPOINT, exist_ok=True)
    destino = os.path.join(PASTA_SHAREPOINT, nome_arquivo)
    shutil.copy2(arquivo, destino)
    print(f"Copiado: {destino}")
if __name__ == "__main__":
    main()
