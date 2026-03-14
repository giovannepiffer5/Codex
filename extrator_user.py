import json
import os
import re
import requests
import pandas as pd

# ── Configuração ──
BASE_URL = "https://pcmbm.zamp.com.br/api/1.1/obj/"
TABELA = "user"
PASTA_DOWNLOADS = r"C:\Users\giovanne.silva\Downloads"

ILLEGAL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')


def limpar_valor(val):
    if isinstance(val, str):
        return ILLEGAL_CHARS.sub(' ', val)
    return val


def achatar_dict(d, prefixo=""):
    """Achata dicionários aninhados em colunas separadas por '.'."""
    itens = {}
    for k, v in d.items():
        chave = f"{prefixo}.{k}" if prefixo else k
        if isinstance(v, dict):
            itens.update(achatar_dict(v, chave))
        elif isinstance(v, list):
            itens[chave] = json.dumps(v, ensure_ascii=False)
        else:
            itens[chave] = v
    return itens


def extrair_dados():
    todos = []
    cursor = 0
    print(f"── Extraindo tabela: {TABELA} ──\n")

    while True:
        params = {"cursor": cursor}
        resp = requests.get(f"{BASE_URL}{TABELA}", params=params, timeout=30)

        if resp.status_code != 200:
            print(f"  Erro {resp.status_code}: {resp.text}")
            break

        data = resp.json().get("response", {})
        results = data.get("results", [])
        remaining = data.get("remaining", 0)

        print(f"  cursor={cursor:>6} | retornados={len(results)} | restantes={remaining}")

        if not results:
            break

        for registro in results:
            todos.append(achatar_dict(registro))

        cursor += 100
        if remaining == 0:
            break

    return todos


def main():
    todos = extrair_dados()

    if not todos:
        print("Nenhum dado encontrado.")
        return

    df = pd.DataFrame(todos).map(limpar_valor)

    nome_arquivo = f"Dados_{TABELA}.xlsx"
    arquivo = os.path.join(PASTA_DOWNLOADS, nome_arquivo)
    df.to_excel(arquivo, index=False, engine="openpyxl")

    print(f"\nTotal: {len(df)} registros")
    print(f"Arquivo: {arquivo}")


if __name__ == "__main__":
    main()
