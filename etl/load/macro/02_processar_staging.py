"""
02_processar_staging.py
=======================
Passo 2 do pipeline operacional.

Lê staging_imports com status='completed' que ainda tenham linhas não processadas
e insere nas tabelas de produção (com data_criacao = hoje):

  clientes → cliente_uc → tabela_macros → telefones → enderecos

Idempotente: pode ser re-executado sem duplicar registros.

Uso:
    python etl/load/02_processar_staging.py                   # todos os pendentes
    python etl/load/02_processar_staging.py --staging-id 3    # ID específico
    python etl/load/02_processar_staging.py --dry-run
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino(autocommit=False, read_timeout=600, write_timeout=600)

DISTRIBUIDORA_MAP = {
    "celp":                3,
    "celpe":               3,
    "neoenergia celpe":    3,
    "coelba":              1,
    "neoenergia coelba":   1,
    "cosern":              2,
    "neoenergia cosern":   2,
    "brasilia":            4,
    "neoenergia brasilia": 4,
}

# resposta_id 6 = 'Aguardando processamento' / status 'pendente'
RESPOSTA_PENDENTE = 6
BATCH = 2000
SEP = "=" * 70


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------

def norm_cpf(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip()).zfill(11)
    return s if len(s) == 11 else None


def norm_uc(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    return s.zfill(10) if s else None


def norm_str(val, maxlen: int = 255) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s[:maxlen] if s else None


def norm_uf(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"[^A-Za-z]", "", str(val).strip())
    return s[:2].upper() if len(s) >= 2 else None


def norm_telefone(val):
    """Retorna (int_tel, tipo) ou (None, None). Suporta com e sem DDD."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    if not s or len(s) < 8 or len(s) > 13:
        return None, None
    # Determina tipo pelo comprimento da parte numérica sem DDD
    parte_num = s[-9:] if len(s) >= 9 and s[-9] in "9" else s[-8:]
    tipo = "celular" if len(parte_num) == 9 else "fixo"
    try:
        return int(s), tipo
    except ValueError:
        return None, None


def parsear_endereco(raw: str) -> dict:
    """Separa endereço completo em logradouro, numero, bairro."""
    res = {"logradouro": None, "numero": None, "bairro": None}
    if not raw:
        return res
    partes = [p.strip() for p in str(raw).split(",")]
    if partes:
        m = re.match(r"^(.*?)\s+(\d[\w\-]*)$", partes[0])
        if m:
            res["logradouro"] = m.group(1).strip()[:255]
            res["numero"] = m.group(2).strip()[:50]
        else:
            res["logradouro"] = partes[0][:255]
    if len(partes) >= 2:
        res["bairro"] = ", ".join(
            partes[1:-1] if len(partes) > 3 else [partes[1]]
        )[:100]
    return res


# ---------------------------------------------------------------------------
# Leitura do arquivo (idêntica ao passo 1)
# ---------------------------------------------------------------------------

def ler_arquivo(filepath: Path) -> pd.DataFrame:
    ext = filepath.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str, encoding="utf-8",
                         sep=None, engine="python")
    else:
        df = pd.read_excel(filepath, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    if "cpf_consultado" in df.columns and "cpf" not in df.columns:
        df = df.rename(columns={"cpf_consultado": "cpf"})
    if "estado" in df.columns and "uf" not in df.columns:
        df = df.rename(columns={"estado": "uf"})
    return df


def detectar_distribuidora(nome_arquivo: str, df: pd.DataFrame) -> int | None:
    for col in ("companhia", "operadora_energia"):
        if col in df.columns:
            v = df[col].dropna()
            if not v.empty:
                chave = str(v.iloc[0]).strip().lower()
                if chave in DISTRIBUIDORA_MAP:
                    return DISTRIBUIDORA_MAP[chave]
    nome = nome_arquivo.lower()
    for chave, did in DISTRIBUIDORA_MAP.items():
        if chave in nome:
            return did
    return None


def colunas_telefone(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if re.match(r"telefone\D*\d+$", c)]


# ---------------------------------------------------------------------------
# Carregamento de maps de deduplicação
# ---------------------------------------------------------------------------

def conectar():
    return pymysql.connect(**DB_CONFIG)


def carregar_maps(cur) -> tuple[dict, dict, set, set, set]:
    print("  [INFO] Carregando maps de deduplicação...")

    cur.execute("SELECT cpf, id FROM clientes")
    cpf_map = {r[0]: r[1] for r in cur.fetchall()}

    # Inclui distribuidora_id na chave (migration 20260406)
    cur.execute("SELECT cliente_id, uc, distribuidora_id, id FROM cliente_uc")
    uc_map = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}

    cur.execute("SELECT cliente_id, distribuidora_id FROM tabela_macros "
                "WHERE status='pendente' AND data_criacao_data=CURDATE()")
    macros_hoje = {(r[0], r[1]) for r in cur.fetchall()}

    cur.execute("SELECT cliente_id, telefone FROM telefones WHERE telefone IS NOT NULL")
    tel_set = {(r[0], int(r[1])) for r in cur.fetchall()}

    cur.execute("SELECT cliente_uc_id, COALESCE(cep,'') FROM enderecos")
    end_set = {(r[0], str(r[1]).strip()) for r in cur.fetchall()}

    print(f"  [INFO] {len(cpf_map):,} clientes  {len(uc_map):,} ucs  "
          f"{len(macros_hoje):,} macros_hoje  {len(tel_set):,} tels  "
          f"{len(end_set):,} enderecos")
    return cpf_map, uc_map, macros_hoje, tel_set, end_set


# ---------------------------------------------------------------------------
# Processamento de um staging_imports
# ---------------------------------------------------------------------------

def processar_staging(conn, staging_id: int, dry_run: bool) -> dict:
    cur = conn.cursor()

    # Metadados do staging
    cur.execute(
        "SELECT filename, distribuidora_nome FROM staging_imports WHERE id=%s",
        (staging_id,),
    )
    row = cur.fetchone()
    if not row:
        print(f"  [ERRO] staging_id={staging_id} não encontrado.")
        cur.close()
        return {}

    filepath = Path(row[0])
    distrib_id_meta = int(row[1]) if row[1] else None

    # Índices válidos ainda não processados → {row_idx: normalized_cpf}
    cur.execute(
        "SELECT row_idx, normalized_cpf FROM staging_import_rows "
        "WHERE staging_id=%s AND validation_status='valid' AND processed_at IS NULL",
        (staging_id,),
    )
    valid_rows: dict[int, str] = {r[0]: r[1] for r in cur.fetchall()}

    if not valid_rows:
        print(f"  [INFO] staging_id={staging_id}: nenhuma linha válida pendente.")
        cur.close()
        return {"staging_id": staging_id, "processadas": 0}

    df = ler_arquivo(filepath)
    distrib_id = detectar_distribuidora(filepath.name, df) or distrib_id_meta
    tel_cols = colunas_telefone(df)
    tem_campos_separados = "numero" in df.columns  # CSV vs Excel

    print(f"\n  staging_id={staging_id}  |  {filepath.name}")
    print(f"  Distrib={distrib_id}  |  linhas_válidas={len(valid_rows):,}")

    cpf_map, uc_map, macros_hoje, tel_set, end_set = carregar_maps(cur)
    cur.close()  # leitura inicial concluída

    data_criacao = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    stats = {
        "clientes_novos": 0,
        "uc_novas": 0,
        "macros_novas": 0,
        "telefones": 0,
        "enderecos": 0,
        "processadas": 0,
        "erros": 0,
    }

    # Conexão de escrita única — sem reconexão por batch (timeout já é 600s)
    conn_w = conectar()
    cur_w  = conn_w.cursor()

    # Filtra apenas as linhas válidas
    df_validas = df[df.index.isin(valid_rows.keys())]
    all_idxs   = list(df_validas.index)

    for chunk_start in range(0, len(all_idxs), BATCH):
        chunk_idxs = all_idxs[chunk_start : chunk_start + BATCH]
        chunk_df   = df_validas.loc[chunk_idxs]

        # ── FASE 0: parsear o chunk ──────────────────────────────────────
        parsed = []
        for idx, row in chunk_df.iterrows():
            norm_c = valid_rows.get(idx)
            uc     = norm_uc(row.get("uc"))
            if not norm_c or not uc:
                stats["erros"] += 1
                continue

            # endereço
            cep    = norm_str(row.get("cep"), 20)
            cidade = norm_str(row.get("cidade"), 100)
            uf     = norm_uf(row.get("uf"))
            if tem_campos_separados:
                logradouro = norm_str(row.get("endereco"), 255)
                numero     = norm_str(row.get("numero"), 50)
                bairro     = norm_str(row.get("bairro"), 100)
            else:
                p = parsear_endereco(row.get("endereco"))
                logradouro = p["logradouro"]
                numero     = p["numero"]
                bairro     = p["bairro"]

            # telefones
            tels = []
            for col in tel_cols:
                tel_val, tipo = norm_telefone(row.get(col))
                if tel_val:
                    tels.append((tel_val, tipo))

            parsed.append({
                "idx": idx,
                "cpf": norm_c,
                "uc": uc,
                "nome": norm_str(row.get("nome"), 255),
                "tels": tels,
                "logradouro": logradouro, "numero": numero, "bairro": bairro,
                "cep": cep, "cidade": cidade, "uf": uf,
            })

        if not parsed:
            continue

        # ── FASE 1: clientes — executemany INSERT IGNORE + bulk SELECT ────
        cpfs_chunk = {d["cpf"] for d in parsed}
        novos_cpfs = cpfs_chunk - cpf_map.keys()

        if not dry_run:
            if novos_cpfs:
                nome_por_cpf = {d["cpf"]: d["nome"] for d in parsed}
                cur_w.executemany(
                    "INSERT IGNORE INTO clientes (cpf, nome, data_criacao)"
                    " VALUES (%s, %s, %s)",
                    [(c, nome_por_cpf[c], data_criacao) for c in novos_cpfs],
                )
                stats["clientes_novos"] += cur_w.rowcount

            ph = ",".join(["%s"] * len(cpfs_chunk))
            cur_w.execute(
                f"SELECT id, cpf FROM clientes WHERE cpf IN ({ph})",
                list(cpfs_chunk),
            )
            cpf_map.update({r[1]: r[0] for r in cur_w.fetchall()})

            # cliente_origem para os recém inseridos
            rows_orig = [
                (cpf_map[c], "fornecedor2", "operacional", data_criacao)
                for c in novos_cpfs if c in cpf_map
            ]
            if rows_orig:
                cur_w.executemany(
                    "INSERT IGNORE INTO cliente_origem"
                    " (cliente_id, fornecedor, campanha, data_import)"
                    " VALUES (%s, %s, %s, %s)",
                    rows_orig,
                )
        else:
            for i, d in enumerate(parsed):
                if d["cpf"] not in cpf_map:
                    cpf_map[d["cpf"]] = -(chunk_start + i + 1)
                    stats["clientes_novos"] += 1

        # ── FASE 2: cliente_uc — executemany INSERT IGNORE + bulk SELECT ──
        chaves_uc_chunk = set()
        for d in parsed:
            cid = cpf_map.get(d["cpf"])
            if cid:
                chaves_uc_chunk.add((cid if cid > 0 else 0, d["uc"], distrib_id))

        novas_ucs = chaves_uc_chunk - uc_map.keys()

        if not dry_run:
            if novas_ucs:
                cur_w.executemany(
                    "INSERT IGNORE INTO cliente_uc"
                    " (cliente_id, uc, distribuidora_id, data_criacao)"
                    " VALUES (%s, %s, %s, %s)",
                    [(cid, u, did, data_criacao) for cid, u, did in novas_ucs],
                )
                stats["uc_novas"] += cur_w.rowcount

            if chaves_uc_chunk:
                cids_chunk = {tpl[0] for tpl in chaves_uc_chunk}
                ph = ",".join(["%s"] * len(cids_chunk))
                cur_w.execute(
                    f"SELECT id, cliente_id, uc, distribuidora_id FROM cliente_uc"
                    f" WHERE cliente_id IN ({ph}) AND distribuidora_id=%s",
                    list(cids_chunk) + [distrib_id],
                )
                for r in cur_w.fetchall():
                    k = (r[1], r[2], r[3])
                    if k in chaves_uc_chunk:
                        uc_map[k] = r[0]
        else:
            for i, chave in enumerate(novas_ucs):
                uc_map[chave] = -(chunk_start + i + 1)
                stats["uc_novas"] += 1

        # ── FASE 3: tabela_macros — executemany INSERT ────────────────────
        rows_macros = []
        for d in parsed:
            cid = cpf_map.get(d["cpf"])
            if not cid:
                continue
            chave_macro = (cid if cid > 0 else 0, distrib_id)
            if chave_macro not in macros_hoje:
                rows_macros.append((cid, distrib_id, RESPOSTA_PENDENTE, data_criacao))
                macros_hoje.add(chave_macro)
                stats["macros_novas"] += 1

        if rows_macros and not dry_run:
            cur_w.executemany(
                "INSERT INTO tabela_macros"
                " (cliente_id, distribuidora_id, resposta_id, status, data_criacao)"
                " VALUES (%s, %s, %s, 'pendente', %s)",
                rows_macros,
            )

        # ── FASE 4: telefones — executemany INSERT ────────────────────────
        rows_tels = []
        for d in parsed:
            cid = cpf_map.get(d["cpf"])
            if not cid:
                continue
            cid_key = cid if cid > 0 else 0
            for tel_val, tipo in d["tels"]:
                chave_tel = (cid_key, tel_val)
                if chave_tel not in tel_set:
                    rows_tels.append((cid, tel_val, tipo, data_criacao))
                    tel_set.add(chave_tel)
                    stats["telefones"] += 1

        if rows_tels and not dry_run:
            cur_w.executemany(
                "INSERT INTO telefones (cliente_id, telefone, tipo, data_criacao)"
                " VALUES (%s, %s, %s, %s)",
                rows_tels,
            )

        # ── FASE 5: enderecos — executemany INSERT ────────────────────────
        rows_ends = []
        for d in parsed:
            cid = cpf_map.get(d["cpf"])
            if not cid or not d["logradouro"]:
                continue
            cid_key = cid if cid > 0 else 0
            chave_uc = (cid_key, d["uc"], distrib_id)
            uc_id    = uc_map.get(chave_uc)
            if not uc_id:
                continue
            uc_id_real = uc_id if not dry_run else 1
            cep_key    = d["cep"] or ""
            chave_end  = (uc_id_real, cep_key)
            if chave_end not in end_set:
                rows_ends.append((
                    cid, uc_id_real, distrib_id,
                    d["logradouro"], d["numero"], d["bairro"],
                    d["cidade"], d["uf"], d["cep"], data_criacao,
                ))
                end_set.add(chave_end)
                stats["enderecos"] += 1

        if rows_ends and not dry_run:
            cur_w.executemany(
                "INSERT INTO enderecos"
                " (cliente_id, cliente_uc_id, distribuidora_id,"
                "  endereco, numero, bairro, cidade, uf, cep, data_criacao)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                rows_ends,
            )

        # ── FASE 6: checkpoint + commit ───────────────────────────────────
        chunk_processed = [d["idx"] for d in parsed]
        stats["processadas"] += len(chunk_processed)

        if not dry_run:
            ph = ",".join(["%s"] * len(chunk_processed))
            cur_w.execute(
                f"UPDATE staging_import_rows SET processed_at=NOW()"
                f" WHERE staging_id=%s AND row_idx IN ({ph})",
                [staging_id] + chunk_processed,
            )
            conn_w.commit()

        pct = stats["processadas"] / len(valid_rows) * 100
        print(
            f"    {stats['processadas']:>7,}/{len(valid_rows):,} ({pct:.0f}%)"
            f"  clientes={stats['clientes_novos']}"
            f"  uc={stats['uc_novas']}"
            f"  macros={stats['macros_novas']}"
            f"  tel={stats['telefones']}"
            f"  end={stats['enderecos']}"
        )

    # Atualiza contagem final no staging_imports
    if not dry_run:
        cur_w.execute(
            "UPDATE staging_imports SET rows_success=%s WHERE id=%s",
            (stats["processadas"], staging_id),
        )
        conn_w.commit()

    cur_w.close()
    conn_w.close()
    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--staging-id", type=int, default=None,
        help="Processa apenas este staging_id",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    if args.staging_id:
        ids = [args.staging_id]
    else:
        cur.execute(
            """SELECT si.id
               FROM staging_imports si
               WHERE si.status = 'completed'
                 AND EXISTS (
                     SELECT 1 FROM staging_import_rows sir
                     WHERE sir.staging_id = si.id
                       AND sir.validation_status = 'valid'
                       AND sir.processed_at IS NULL
                 )
               ORDER BY si.id"""
        )
        ids = [r[0] for r in cur.fetchall()]

    cur.close()

    if not ids:
        print("[INFO] Nenhum staging pendente para processar.")
        conn.close()
        return

    print(SEP)
    print(f"PROCESSAR STAGING -> PRODUÇÃO  —  {len(ids)} arquivo(s)")
    print(SEP)

    if args.dry_run:
        print("[INFO] DRY-RUN — nada será gravado.\n")

    totais = {
        "clientes_novos": 0,
        "uc_novas": 0,
        "macros_novas": 0,
        "telefones": 0,
        "enderecos": 0,
        "processadas": 0,
        "erros": 0,
    }

    for sid in ids:
        stats = processar_staging(conn, sid, args.dry_run)
        for k in totais:
            totais[k] += stats.get(k, 0)

    conn.close()

    print(f"\n{SEP}")
    print("RESULTADO FINAL — PRODUÇÃO")
    print(SEP)
    labels = {
        "processadas":    "Linhas processadas",
        "clientes_novos": "Clientes novos     ",
        "uc_novas":       "UCs novas          ",
        "macros_novas":   "Macros inseridas   ",
        "telefones":      "Telefones inseridos",
        "enderecos":      "Endereços inseridos",
        "erros":          "Erros              ",
    }
    for k, label in labels.items():
        print(f"  {label} : {totais[k]:>10,}")
    print(SEP)


if __name__ == "__main__":
    main()
