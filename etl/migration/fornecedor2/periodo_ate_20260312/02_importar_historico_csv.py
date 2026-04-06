"""
importar_historico.py  —  PASSO 2 de 2
=======================================
Lê o CSV normalizado gerado pelo PASSO 1 (normalizar_historico.py) e
insere os dados no banco em LOTES (bulk insert), reduzindo drasticamente
o número de round-trips ao servidor remoto.

Estratégia por lote de BATCH_SIZE linhas:
  1. INSERT IGNORE INTO clientes  ... VALUES (…),(…),…   [1 query]
  2. SELECT id, cpf FROM clientes WHERE cpf IN (…)       [1 query → dict cpf→id]
  3. INSERT IGNORE INTO cliente_uc ... VALUES (…),(…),…  [1 query]
  4. SELECT id, cliente_id, uc FROM cliente_uc WHERE …   [1 query → dict]
  5. INSERT INTO tabela_macros … VALUES (…),(…),…        [1 query]
  6. COMMIT + salva progresso JSON

Arquivo de progresso (JSON):
  Salvo automaticamente a cada lote em dados/importacao_progresso.json.
  Ao rodar novamente retoma automaticamente de onde parou.
  Use --reset para ignorar o progresso e recomeçar do zero.

Uso:
    python scripts/importar_historico.py                          # importa (ou retoma)
    python scripts/importar_historico.py --file dados/outro.csv   # CSV customizado
    python scripts/importar_historico.py --dry-run                # simula sem gravar
    python scripts/importar_historico.py --force                  # inclui linhas com observacao
    python scripts/importar_historico.py --reset                  # recomeça do zero
"""

import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
DB_CONFIG = db_destino(autocommit=False)

BASE_DIR       = Path(__file__).resolve().parent.parent.parent  # raiz do projeto
PERIODO        = "migration_periodo_ate_20260312"
CSV_PADRAO     = BASE_DIR / "dados" / PERIODO / "processed" / "historico_normalizado_para_importar.csv"
PROGRESSO_FILE = BASE_DIR / "dados" / PERIODO / "state" / "importacao_progresso.json"
BATCH_SIZE          = 500   # linhas por lote
VALID_RESPOSTA_IDS  = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}  # ids presentes na tabela respostas


# ---------------------------------------------------------------------------
# Progresso JSON
# ---------------------------------------------------------------------------

def carregar_progresso(csv_path: Path) -> dict:
    """Carrega progresso salvo. Retorna dict vazio se não existir ou for de outro CSV."""
    if not PROGRESSO_FILE.exists():
        return {}
    try:
        with open(PROGRESSO_FILE, "r", encoding="utf-8") as f:
            dados = json.load(f)
        # Só retoma se for o mesmo CSV
        if dados.get("csv_file") != csv_path.name:
            print(f"[INFO] Progresso salvo é de outro CSV ('{dados.get('csv_file')}') — ignorando.")
            return {}
        return dados
    except Exception:
        return {}


def salvar_progresso(csv_path: Path, ultimo_indice: int, ok: int,
                     skipped: int, erros: int, total: int,
                     iniciado_em: str, erros_detalhe: list,
                     status: str = ""):
    """Salva o estado atual no arquivo JSON de progresso."""
    if not status:
        status = "em_andamento" if (ok + skipped + erros) < total else "concluido"
    dados = {
        "csv_file":        csv_path.name,
        "total":           total,
        "ultimo_indice":   ultimo_indice,
        "ok":              ok,
        "skipped":         skipped,
        "erros":           erros,
        "status":          status,
        "iniciado_em":     iniciado_em,
        "atualizado_em":   datetime.now().isoformat(timespec="seconds"),
        "erros_detalhe":   erros_detalhe[-200:],
    }
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Bulk helpers
# ---------------------------------------------------------------------------

def bulk_upsert_clientes(cursor, registros: list) -> dict:
    """
    Insere clientes em lote e retorna dict {cpf: id}.
    registros: lista de (cpf, data_criacao)
    """
    if not registros:
        return {}

    placeholders = ", ".join(["(%s, %s, %s)"] * len(registros))
    valores = []
    for cpf, data_criacao in registros:
        valores.extend([cpf, data_criacao, data_criacao])

    cursor.execute(
        f"INSERT IGNORE INTO clientes (cpf, data_criacao, data_update) VALUES {placeholders}",
        valores,
    )

    cpfs = [r[0] for r in registros]
    fmt  = ", ".join(["%s"] * len(cpfs))
    cursor.execute(f"SELECT id, cpf FROM clientes WHERE cpf IN ({fmt})", cpfs)
    return {row[1]: row[0] for row in cursor.fetchall()}


def bulk_upsert_cliente_uc(cursor, registros: list) -> dict:
    """
    Insere cliente_uc em lote e retorna dict {(cliente_id, uc): id}.
    registros: lista de (cliente_id, uc, distribuidora_id, data_criacao)
    """
    if not registros:
        return {}

    placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(registros))
    valores = []
    for cliente_id, uc, distribuidora_id, data_criacao in registros:
        valores.extend([cliente_id, uc, distribuidora_id, data_criacao])

    cursor.execute(
        f"INSERT IGNORE INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao) VALUES {placeholders}",
        valores,
    )

    condicoes = " OR ".join(["(cliente_id = %s AND uc = %s)"] * len(registros))
    params_sel = []
    for cliente_id, uc, _, _ in registros:
        params_sel.extend([cliente_id, uc])
    cursor.execute(
        f"SELECT id, cliente_id, uc FROM cliente_uc WHERE {condicoes}",
        params_sel,
    )
    return {(row[1], row[2]): row[0] for row in cursor.fetchall()}


def bulk_inserir_macros(cursor, registros: list):
    """
    Insere tabela_macros em lote.
    registros: lista de (cliente_id, distribuidora_id, resposta_id, data_update, data_criacao, status)
    """
    if not registros:
        return

    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, 1)"] * len(registros))
    valores = []
    for cliente_id, distribuidora_id, resposta_id, data_update, data_criacao, status in registros:
        valores.extend([cliente_id, distribuidora_id, resposta_id,
                        data_update, data_criacao, status])

    cursor.execute(
        f"""INSERT INTO tabela_macros
            (cliente_id, distribuidora_id, resposta_id,
             data_update, data_criacao, status, extraido)
            VALUES {placeholders}""",
        valores,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PASSO 2: Importa CSV normalizado para o banco (bulk).")
    parser.add_argument("--file", type=str, default=str(CSV_PADRAO),
                        help=f"CSV de entrada (default: {CSV_PADRAO.name})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula a importacao sem gravar no banco")
    parser.add_argument("--force", action="store_true",
                        help="Importa tambem linhas com 'observacao' preenchida")
    parser.add_argument("--reset", action="store_true",
                        help="Ignora progresso salvo e recomeça do zero")
    args = parser.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        print(f"[ERRO] Arquivo '{csv_path}' nao encontrado.")
        print(f"       Execute primeiro: python scripts/normalizar_historico.py")
        sys.exit(1)

    # Ler CSV
    try:
        df = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8-sig")
    except Exception as e:
        print(f"[ERRO] Falha ao ler CSV: {e}")
        sys.exit(1)

    df = df.fillna("")

    total_csv = len(df)
    com_obs   = df["observacao"].ne("").sum()
    sem_obs   = total_csv - com_obs

    print(f"[INFO] CSV: {csv_path.name}")
    print(f"[INFO] Total de linhas no CSV : {total_csv}")
    print(f"[INFO] Linhas validas (sem obs): {sem_obs}")
    print(f"[INFO] Linhas com observacao   : {com_obs}  "
          f"{'(serao importadas por --force)' if args.force else '(serao ignoradas)'}")

    if args.dry_run:
        print("[INFO] Modo DRY-RUN — nada sera gravado.\n")

    # Filtrar
    df_proc = df.copy() if args.force else df[df["observacao"] == ""].copy()
    df_proc = df_proc.reset_index(drop=True)

    if df_proc.empty:
        print("\n[AVISO] Nenhuma linha para importar.")
        sys.exit(0)

    total = len(df_proc)

    # Progresso
    if args.reset and PROGRESSO_FILE.exists():
        PROGRESSO_FILE.unlink()
        print("[INFO] Progresso anterior removido — recomeçando do zero.")

    progresso = {} if args.dry_run else carregar_progresso(csv_path)
    retomando = bool(progresso) and progresso.get("status") == "em_andamento"

    if retomando:
        inicio_de     = progresso["ultimo_indice"] + 1
        ok            = progresso["ok"]
        skipped       = progresso["skipped"]
        erros         = progresso["erros"]
        iniciado_em   = progresso["iniciado_em"]
        erros_detalhe = progresso.get("erros_detalhe", [])
        print(f"[INFO] Retomando a partir da linha {inicio_de} "
              f"(ja processadas: {ok} ok / {skipped} skip / {erros} erros)")
    elif progresso.get("status") == "concluido":
        print(f"[INFO] Importacao ja concluida anteriormente "
              f"({progresso['ok']} inseridos). Use --reset para reimportar.")
        sys.exit(0)
    else:
        inicio_de = 0
        ok = skipped = erros = 0
        iniciado_em   = datetime.now().isoformat(timespec="seconds")
        erros_detalhe = []

    restante = total - inicio_de
    print(f"[INFO] Processando {restante} linha(s) em lotes de {BATCH_SIZE} "
          f"{'(dry-run)' if args.dry_run else ''}\n")

    # Conectar
    conn = cursor = None
    if not args.dry_run:
        try:
            conn   = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            print("[INFO] Conectado ao banco.\n")
        except Exception as e:
            print(f"[ERRO] Falha ao conectar: {e}")
            sys.exit(1)

    # -----------------------------------------------------------------------
    # Loop por lotes de BATCH_SIZE
    # -----------------------------------------------------------------------
    pos = inicio_de
    while pos < total:
        lote_fim = min(pos + BATCH_SIZE, total)
        lote     = df_proc.iloc[pos:lote_fim]

        # Validar e coletar linhas do lote
        clientes_batch  = []   # (cpf, data_criacao)  — únicos
        uc_pendente     = []   # (cpf, uc, dist_id, data_criacao)
        macros_pendente = []   # (cpf, uc, dist_id, resposta_id, status, data_update, data_criacao)

        for _, row in lote.iterrows():
            linha_excel      = row.get("linha_excel", "?")
            aba_excel        = row.get("aba_excel", "")
            arquivo          = row.get("arquivo_origem", "")
            ref              = f"L{linha_excel}|{aba_excel}|{arquivo}" if aba_excel else f"L{linha_excel}|{arquivo}"

            cpf              = str(row["cpf"]).strip()
            uc               = str(row["uc"]).strip()
            distribuidora_id = str(row["distribuidora_id"]).strip()
            resposta_raw     = str(row["resposta_id"]).strip()
            status           = str(row.get("status", "pendente")).strip() or "pendente"
            data_update      = str(row["data_update"]).strip()
            data_criacao     = str(row["data_criacao"]).strip()

            if not cpf or len(cpf) != 11:
                print(f"  [SKIP {ref}] CPF invalido: '{cpf}'")
                skipped += 1
                continue
            if not distribuidora_id or not distribuidora_id.isdigit():
                print(f"  [SKIP {ref}] distribuidora_id invalida: '{distribuidora_id}'")
                skipped += 1
                continue
            if not data_update:
                print(f"  [SKIP {ref}] data_update ausente")
                skipped += 1
                continue

            _rid = int(resposta_raw) if resposta_raw.isdigit() else None
            resposta_id = _rid if _rid in VALID_RESPOSTA_IDS else None
            if _rid is not None and resposta_id is None:
                print(f"  [WARN {ref}] resposta_id={_rid} nao existe em respostas — será NULL")
            dist_id     = int(distribuidora_id)

            clientes_batch.append((cpf, data_criacao))
            if uc:
                uc_pendente.append((cpf, uc, dist_id, data_criacao))
            macros_pendente.append((cpf, uc, dist_id, resposta_id, status, data_update, data_criacao))

        if args.dry_run:
            ok += len(macros_pendente)
            pct = round(lote_fim / total * 100, 1)
            print(f"  [DRY ] lote {pos + 1}–{lote_fim}: {len(macros_pendente)} linhas | {pct}% ({lote_fim}/{total})")
            pos = lote_fim
            continue

        if not macros_pendente:
            pos = lote_fim
            continue

        # --- Inserções em lote ---
        try:
            # 1. Clientes (deduplicar CPFs no lote)
            clientes_unicos = list({cpf: dc for cpf, dc in clientes_batch}.items())
            cpf_to_id = bulk_upsert_clientes(cursor, clientes_unicos)

            # 2. cliente_uc
            uc_regs = []
            for cpf, uc, dist_id, data_criacao in uc_pendente:
                cid = cpf_to_id.get(cpf)
                if cid:
                    uc_regs.append((cid, uc, dist_id, data_criacao))

            bulk_upsert_cliente_uc(cursor, uc_regs) if uc_regs else None

            # 3. tabela_macros
            macro_regs = []
            for cpf, uc, dist_id, resposta_id, status, data_update, data_criacao in macros_pendente:
                cid = cpf_to_id.get(cpf)
                if not cid:
                    erros += 1
                    erros_detalhe.append({"cpf": cpf, "erro": "cliente_id nao encontrado apos insert"})
                    continue
                macro_regs.append((cid, dist_id, resposta_id, data_update, data_criacao, status))

            bulk_inserir_macros(cursor, macro_regs)
            conn.commit()

            ok += len(macro_regs)
            salvar_progresso(csv_path, lote_fim - 1, ok, skipped, erros,
                             total, iniciado_em, erros_detalhe)

            pct = round(lote_fim / total * 100, 1)
            print(f"  [LOTE] {pos + 1}–{lote_fim} | ok: {ok} | {pct}% ({lote_fim}/{total})")

        except Exception as e:
            conn.rollback()
            erros += len(macros_pendente)
            erros_detalhe.append({
                "lote_ini": pos, "lote_fim": lote_fim,
                "erro": str(e),
                "momento": datetime.now().isoformat(timespec="seconds"),
            })
            print(f"  [ERRO] Lote {pos + 1}–{lote_fim} falhou: {e}")
            salvar_progresso(csv_path, pos - 1, ok, skipped, erros,
                             total, iniciado_em, erros_detalhe)

        pos = lote_fim

    # Fechar conexão
    if cursor:
        cursor.close()
    if conn:
        conn.close()

    # Marcar concluído
    if not args.dry_run:
        salvar_progresso(csv_path, total - 1, ok, skipped, erros,
                         total, iniciado_em, erros_detalhe, status="concluido")
        print(f"\n[INFO] Progresso salvo em: {PROGRESSO_FILE}")

    # Relatório final
    print("\n" + "=" * 60)
    print("Resultado final:")
    print(f"  Inseridos com sucesso : {ok}")
    print(f"  Ignorados (skip)      : {skipped}")
    print(f"  Erros                 : {erros}")
    if args.dry_run:
        print("  DRY-RUN — nenhuma alteracao foi feita.")
    elif erros:
        print(f"\n  Detalhes dos erros em: {PROGRESSO_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
