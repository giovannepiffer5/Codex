import os
import re
import requests
import pandas as pd
import win32com.client

# ─────────────────────────────────────────────
# CONFIGURAÇÕES — edite aqui
# ─────────────────────────────────────────────
BASE_URL = "https://pcmbm.zamp.com.br/api/1.1/obj"  # sem barra final
TABELA   = "tabela_orc"

MES_INICIO = 4
ANO_INICIO = 2026

MES_FIM    = 4
ANO_FIM    = 2026

PASTA_DOWNLOADS = r"C:\Users\giovanne.silva\Downloads"
# ─────────────────────────────────────────────

ILLEGAL_CHARS = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')


def limpar_valor(val):
    if isinstance(val, str):
        return ILLEGAL_CHARS.sub('  ', val)
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


todos = []
periodos = gerar_periodos(MES_INICIO, ANO_INICIO, MES_FIM, ANO_FIM)
print(f"Períodos a extrair: {[f'{m}-{a}' for m, a in periodos]}\n")

for MES, ANO in periodos:
    month  = f"{MES}-{ANO}"
    cursor = 0
    print(f"── Extraindo {month} ──")

    while True:
        params = {
            "cursor": cursor,
            "constraints": f'[{{"key":"month","constraint_type":"equals","value":"{month}"}}]',
        }

        resp = requests.get(f"{BASE_URL}/{TABELA}", params=params, timeout=30)

        if resp.status_code != 200:
            print(f"  Erro {resp.status_code}: {resp.text}")
            break

        data      = resp.json().get("response", {})
        results   = data.get("results", [])
        remaining = data.get("remaining", 0)
        count     = data.get("count", 0)

        print(f"  cursor={cursor:>6} | retornados={len(results)} | restantes={remaining} | total={count}")

        if not results:
            print("  Sem resultados.")
            break

        todos.extend(results)
        cursor += 100

        if remaining == 0:
            print(f"  ✅ {len(results)} registros coletados.")
            break

if todos:
    df = pd.DataFrame(todos)
    df = df.map(limpar_valor)

    # ── Filtro 1: remover regional PLK ──
    if "regional" in df.columns:
        antes = len(df)
        df = df[~df["regional"].astype(str).str.contains("PLK", case=False, na=False)]
        print(f"\nLinhas removidas (regional PLK): {antes - len(df)}")

    # ── Filtro 2: coluna "auditado" — manter apenas VERDADEIRO e FALSO ──
    if "auditado" in df.columns:
        antes = len(df)
        df["auditado"] = df["auditado"].astype(str).str.strip()
        df = df[df["auditado"].str.upper().isin(["VERDADEIRO", "FALSO", "TRUE", "FALSE"])]
        print(f"Linhas removidas (auditado em branco): {antes - len(df)}")

    # ── Salvar Excel ──
    arquivo = os.path.join(
        PASTA_DOWNLOADS,
        f"Dados_{TABELA}_{MES_INICIO}_{ANO_INICIO}_a_{MES_FIM}_{ANO_FIM}.xlsx"
    )
    tentativa = 0
    while os.path.exists(arquivo):
        try:
            with open(arquivo, 'a'):
                break
        except PermissionError:
            tentativa += 1
            arquivo = os.path.join(
                PASTA_DOWNLOADS,
                f"Dados_{TABELA}_{MES_INICIO}_{ANO_INICIO}_a_{MES_FIM}_{ANO_FIM}_v{tentativa}.xlsx"
            )
            print(f"  Arquivo em uso, salvando como: {arquivo}")

    df.to_excel(arquivo, index=False, engine="openpyxl")
    print(f"\nTotal de registros: {len(df)}")
    print(f"Arquivo gerado: {arquivo}")

    # ══════════════════════════════════════════════
    # ANÁLISE + RASCUNHO OUTLOOK
    # ══════════════════════════════════════════════

    total_registros = len(df)
    periodo_txt = f"{MES_INICIO}/{ANO_INICIO} a {MES_FIM}/{ANO_FIM}"

    # Normalizar auditado
    df["auditado_norm"] = df["auditado"].str.upper().map({
        "VERDADEIRO": "Auditado", "TRUE": "Auditado",
        "FALSO": "Invalidado", "FALSE": "Invalidado"
    })

    # Converter valor_total para numérico
    df["valor_total_num"] = pd.to_numeric(df["valor_total"], errors="coerce").fillna(0)

    # ── 1. Contagem de auditados x invalidados ──
    qtd_auditado   = len(df[df["auditado_norm"] == "Auditado"])
    qtd_invalidado = len(df[df["auditado_norm"] == "Invalidado"])
    perc_auditado   = (qtd_auditado   / total_registros * 100) if total_registros else 0
    perc_invalidado = (qtd_invalidado / total_registros * 100) if total_registros else 0

    # ── 2. Cruzamento: status x auditado ──
    # index.name=None remove a linha vazia; columns.name vira o rótulo de cabeçalho
    tabela_status = pd.crosstab(
        df["status-txt"], df["auditado_norm"], margins=True, margins_name="Total"
    )
    tabela_status.index.name   = None
    tabela_status.columns.name = "Status"
    html_tabela_status = tabela_status.to_html(border=0, classes="tabela")

    # ── 3. Auditoria por mês ──
    tabela_mes = pd.crosstab(df["month"], df["auditado_norm"], margins=True, margins_name="Total")
    tabela_mes.index.name   = None
    tabela_mes.columns.name = "Mês"
    html_tabela_mes = tabela_mes.to_html(border=0, classes="tabela")

    # ── 4. Auditoria por regional ──
    tabela_regional = pd.crosstab(df["regional"], df["auditado_norm"], margins=True, margins_name="Total")
    tabela_regional.index.name   = None
    tabela_regional.columns.name = "Regional"
    html_tabela_regional = tabela_regional.to_html(border=0, classes="tabela")

    # ── 5. Valor total por status e auditoria ──
    valor_por_status = (
        df.groupby(["status-txt", "auditado_norm"])["valor_total_num"]
        .sum()
        .unstack(fill_value=0)
    )
    valor_por_status["Total"] = valor_por_status.sum(axis=1)
    valor_por_status.loc["Total"] = valor_por_status.sum()
    valor_por_status.index.name   = None
    valor_por_status.columns.name = "Status"
    valor_por_status = valor_por_status.map(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    html_tabela_valor = valor_por_status.to_html(border=0, classes="tabela")

    # ── 6. Todas as regionais com invalidados ──
    inv_por_regional = (
        df[df["auditado_norm"] == "Invalidado"]
        .groupby("regional")
        .size()
        .sort_values(ascending=False)
    )
    html_inv_regional = ""
    for regional, qtd in inv_por_regional.items():
        html_inv_regional += f"<li><b>{regional}</b>: {qtd} chamados invalidados</li>"

    # ── 7. Top 5 fornecedores por volume (usando nome da empresa) ──
    top_fornecedores = df.groupby("empresa_text").agg(
        qtd=("empresa_text", "size"),
        valor=("valor_total_num", "sum")
    ).sort_values("valor", ascending=False).head(5)
    html_top_forn = ""
    for forn, row in top_fornecedores.iterrows():
        valor_fmt = f"R$ {row['valor']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        html_top_forn += f"<li><b>{forn}</b>: {int(row['qtd'])} chamados — {valor_fmt}</li>"

    # ── 8. Taxa de invalidação por status ──
    taxa_inv = df.groupby("status-txt")["auditado_norm"].apply(
        lambda x: f"{(x == 'Invalidado').sum() / len(x) * 100:.1f}%"
    )
    html_taxa = ""
    for status_nome, taxa in taxa_inv.items():
        html_taxa += f"<li><b>{status_nome}</b>: {taxa} de invalidação</li>"

    # ── Montar corpo do e-mail ──
    corpo_html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Calibri, Arial, sans-serif; font-size: 11pt; color: #333; }}
        h2 {{ color: #1F4E79; }}
        h3 {{ color: #2E75B6; margin-top: 24px; }}
        .tabela {{
            border-collapse: collapse;
            font-size: 10pt;
            margin: 8px 0 16px 0;
            box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            border-radius: 6px;
            overflow: hidden;
        }}
        .tabela thead tr:first-child th {{
            background: linear-gradient(135deg, #1F4E79 0%, #2E75B6 100%);
            color: #ffffff;
            font-size: 11pt;
            font-weight: bold;
            padding: 9px 16px;
            text-align: center;
            letter-spacing: 0.4px;
            border: none;
        }}
        .tabela thead tr:nth-child(2) th {{
            background-color: #2E75B6;
            color: #ffffff;
            padding: 7px 14px;
            text-align: center;
            border: none;
        }}
        .tabela td {{
            padding: 6px 14px;
            border: 1px solid #d0d8e4;
            text-align: center;
        }}
        .tabela tbody tr:nth-child(even) {{ background-color: #eef3f9; }}
        .tabela tbody tr:hover {{ background-color: #dce8f5; }}
        .tabela tbody tr:last-child td {{
            font-weight: bold;
            background-color: #d6e4f0;
            border-top: 2px solid #2E75B6;
        }}
        .destaque {{ font-size: 13pt; font-weight: bold; color: #1F4E79; }}
        .bloco {{
            background-color: #f0f5fb;
            border-left: 4px solid #2E75B6;
            padding: 10px 15px;
            margin: 10px 0;
            border-radius: 0 4px 4px 0;
        }}
        ul {{ line-height: 1.8; }}
    </style>
    </head>
    <body>
        <h2>Relatório de Auditoria — Custos - PCMBM</h2>
        <p>Período analisado: <b>{periodo_txt}</b> | Total de chamados analisados: <b>{total_registros}</b></p>

        <h3>1. Resumo Geral de Auditoria</h3>
        <div class="bloco">
            <p>Chamados auditados: <span class="destaque">{qtd_auditado}</span> ({perc_auditado:.1f}% do total)</p>
            <p>Chamados invalidados: <span class="destaque">{qtd_invalidado}</span> ({perc_invalidado:.1f}% do total)</p>
        </div>

        <h3>2. Distribuição por Status do Chamado</h3>
        <p>A tabela abaixo apresenta o cruzamento entre o <b>status atual do chamado</b> e a <b>situação de auditoria</b> (Auditado ou Invalidado):</p>
        {html_tabela_status}

        <h3>3. Taxa de Invalidação por Status</h3>
        <p>Percentual de chamados invalidados dentro de cada status:</p>
        <ul>{html_taxa}</ul>

        <h3>4. Evolução Mensal</h3>
        <p>A tabela abaixo mostra a <b>quantidade de chamados auditados e invalidados por mês</b>, permitindo acompanhar a evolução ao longo do período:</p>
        {html_tabela_mes}

        <h3>5. Distribuição por Regional</h3>
        <p>Detalhamento de chamados <b>auditados e invalidados por regional</b>:</p>
        {html_tabela_regional}

        <h3>6. Invalidações por Regional</h3>
        <p>Volume de chamados invalidados em cada regional, ordenado do maior para o menor:</p>
        <ul>{html_inv_regional}</ul>

        <h3>7. Valor Total por Status e Situação de Auditoria</h3>
        <p>A tabela abaixo apresenta o <b>valor financeiro (R$)</b> dos chamados, separado por status e situação de auditoria:</p>
        {html_tabela_valor}

        <h3>8. Top 5 Fornecedores por Valor</h3>
        <p>Fornecedores com maior volume financeiro no período:</p>
        <ul>{html_top_forn}</ul>
    </body>
    </html>
    """

    # ── Criar rascunho no Outlook ──
    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.Subject = f"Relatório de Auditoria — Chamados PCM — {periodo_txt}"
    mail.HTMLBody = corpo_html
    mail.Save()

    print("\n✅ Rascunho criado no Outlook com sucesso!")
    print("   Abra a pasta Rascunhos no Outlook para revisar e enviar.")

else:
    print("Nenhum dado encontrado para exportar.")
