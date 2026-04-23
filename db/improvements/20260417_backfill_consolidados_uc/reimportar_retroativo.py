"""
reimportar_retroativo.py
========================
Melhoria 20260417 — Limpeza + reimportação retroativa completa.

Regras de negócio:
  1. Registros sem UC (cliente_uc_id IS NULL) em tabela_macros são irrelevantes
     — são apagados.
  2. Todos os arquivos de entrada são relidos. Cada linha COM UC válida gera
     uma entrada em staging (staging_imports + staging_import_rows), mesmo que
     o CPF+UC já exista — para rastrear quantos arquivos repetiram a mesma combo.
  3. Para tabela_macros: combinações CPF+UC+distribuidora que JÁ existem no banco
     COM cliente_uc_id preenchido NÃO são duplicadas. Apenas combinações que
     não existem são inseridas como 'pendente' (resposta_id=6).

Fontes processadas (em ordem):
  A) dados/fornecedor2/migration_periodo_ate_20260312/processed/historico_normalizado_para_importar.csv
  B) dados/fornecedor2/migration_periodo_pos_20260312/processed/historico_normalizado_para_importar.csv
  C) dados/fornecedor2/operacional/*/  (todos os CSVs e XLSXs)

Uso:
  python db/improvements/20260417_backfill_consolidados_uc/reimportar_retroativo.py --dry-run
  python db/improvements/20260417_backfill_consolidados_uc/reimportar_retroativo.py
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70
BATCH = 2000

PROGRESS_FILE = Path(__file__).resolve().parent / "progress.json"

DISTRIBUIDORA_MAP = {
    "celp": 3, "celpe": 3, "neoenergia celpe": 3,
    "coelba": 1, "neoenergia coelba": 1,
    "cosern": 2, "neoenergia cosern": 2,
    "brasilia": 4, "neoenergia brasilia": 4,
}

RESPOSTA_PENDENTE = 6

NOMES_DISTRIBUIDORA = {
    "coelba", "cosern", "celpe", "celp",
    "neoenergia coelba", "neoenergia cosern", "neoenergia celpe",
    "brasilia", "neoenergia brasilia",
}


def nome_eh_distribuidora(nome: str | None) -> bool:
    """Retorna True se o nome armazenado é na verdade o nome de uma distribuidora."""
    if not nome:
        return False
    return nome.strip().lower() in NOMES_DISTRIBUIDORA


def log(msg: str = ""):
    print(msg, flush=True)


def load_progress() -> dict:
    """Carrega progresso salvo ou retorna dict vazio."""
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_progress(data: dict):
    """Salva progresso no arquivo JSON."""
    PROGRESS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                             encoding="utf-8")


def step_done(progress: dict, step: str) -> bool:
    """Verifica se uma etapa já foi concluída."""
    return progress.get("steps", {}).get(step, {}).get("done", False)


def mark_step(progress: dict, step: str, stats: dict | None = None):
    """Marca etapa como concluída e salva."""
    if "steps" not in progress:
        progress["steps"] = {}
    progress["steps"][step] = {"done": True, "ts": datetime.now().isoformat()}
    if stats:
        progress["steps"][step]["stats"] = stats
    save_progress(progress)


def source_done(progress: dict, source_key: str) -> bool:
    """Verifica se uma fonte já foi processada."""
    return source_key in progress.get("sources_done", {})


def mark_source(progress: dict, source_key: str, stats: dict):
    """Marca fonte como processada e salva."""
    if "sources_done" not in progress:
        progress["sources_done"] = {}
    progress["sources_done"][source_key] = {
        "ts": datetime.now().isoformat(), **stats
    }
    save_progress(progress)


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
    """Retorna (int_tel, tipo) ou (None, None)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    if not s or len(s) < 8 or len(s) > 13:
        return None, None
    parte_num = s[-9:] if len(s) >= 9 and s[-9] in "9" else s[-8:]
    tipo = "celular" if len(parte_num) == 9 else "fixo"
    try:
        return int(s), tipo
    except ValueError:
        return None, None


def norm_data_nascimento(val) -> str | None:
    """Converte dd/mm/yyyy ou yyyy-mm-dd para yyyy-mm-dd (DATE). Retorna None se inválido."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def norm_cep(val) -> str | None:
    """Normaliza CEP para 8 dígitos sem hífen."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).strip())
    return s[:8] if len(s) >= 8 else (s if s else None)


def colunas_telefone(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if re.match(r"telefone?\D*\d+$|celular\d*$|tel_fixo\d*$|hotfone_\w+$", c)]


def extrair_telefones_row(row, cols_telefone: list[str],
                          cols_multi: list[str]) -> list[tuple[int, str]]:
    """Extrai todos os telefones válidos de uma linha.
    cols_telefone: colunas com 1 telefone por campo.
    cols_multi: colunas de valor múltiplo (separado por , ; | /).
    Retorna lista de (tel_int, tipo) sem duplicatas.
    """
    seen = set()
    result = []
    for col in cols_telefone:
        tel, tipo = norm_telefone(row.get(col))
        if tel and tel not in seen:
            seen.add(tel)
            result.append((tel, tipo))
    for col in cols_multi:
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        for part in re.split(r"[,;|/]", str(val)):
            tel, tipo = norm_telefone(part.strip())
            if tel and tel not in seen:
                seen.add(tel)
                result.append((tel, tipo))
    return result


def detectar_distrib_id(nome_arquivo: str, df: pd.DataFrame) -> int | None:
    for col in ("companhia", "operadora_energia", "lote_nome", "operadora"):
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


# ---------------------------------------------------------------------------
# Fontes de dados
# ---------------------------------------------------------------------------

def listar_fontes() -> list[dict]:
    """
    Retorna lista de dicts com informação de cada fonte.
    Cada dict: {path, tipo, distrib_id_hint}
    """
    fontes = []

    # A) Histórico até 20260312
    f = ROOT / "dados" / "fornecedor2" / "migration_periodo_ate_20260312" / "processed" / "historico_normalizado_para_importar.csv"
    if f.exists():
        fontes.append({"path": f, "tipo": "historico_ate", "distrib_hint": None})

    # B) Histórico pós 20260312
    f = ROOT / "dados" / "fornecedor2" / "migration_periodo_pos_20260312" / "processed" / "historico_normalizado_para_importar.csv"
    if f.exists():
        fontes.append({"path": f, "tipo": "historico_pos", "distrib_hint": None})

    # C) Operacional
    op_dir = ROOT / "dados" / "fornecedor2" / "operacional"
    if op_dir.exists():
        for data_dir in sorted(op_dir.iterdir()):
            if not data_dir.is_dir():
                continue
            for fp in sorted(data_dir.iterdir()):
                if fp.suffix.lower() in (".csv", ".xlsx", ".xls"):
                    fontes.append({"path": fp, "tipo": "operacional", "distrib_hint": None})

    return fontes


def listar_fontes_enriquecimento() -> list[dict]:
    """
    Lista arquivos raw em dados/fornecedor2/.../raw/bases/ que possuem
    dados ricos (telefones, dt_nascimento, endereço) NÃO presentes nos
    CSVs processados.  Usados APENAS para enriquecer clientes existentes.
    """
    fontes = []
    bases_dir = (
        ROOT / "dados" / "fornecedor2"
        / "migration_periodo_ate_20260312" / "raw" / "bases"
    )
    if not bases_dir.exists():
        return fontes
    for sub in sorted(bases_dir.iterdir()):
        if not sub.is_dir():
            continue
        for fp in sorted(sub.iterdir()):
            if fp.suffix.lower() in (".csv", ".xlsx", ".xls"):
                fontes.append({"path": fp, "tipo": "raw_enrich", "distrib_hint": None})
    return fontes


def ler_fonte(fonte: dict) -> tuple[pd.DataFrame, int | None]:
    """Lê uma fonte e retorna (df, distribuidora_id)."""
    fp = fonte["path"]
    ext = fp.suffix.lower()

    if ext == ".csv":
        df = pd.read_csv(fp, dtype=str, encoding="utf-8-sig", sep=None, engine="python")
    else:
        df = pd.read_excel(fp, dtype=str)

    df.columns = [c.strip().lower() for c in df.columns]

    # ── Normalizar nomes de coluna conhecidos ───────────────────────────
    rename = {}
    if "cpf_consultado" in df.columns and "cpf" not in df.columns:
        rename["cpf_consultado"] = "cpf"
    if "clientes_documento" in df.columns and "cpf" not in df.columns:
        rename["clientes_documento"] = "cpf"
    if "estado" in df.columns and "uf" not in df.columns:
        rename["estado"] = "uf"
    if "clientes_nome" in df.columns and "nome" not in df.columns:
        rename["clientes_nome"] = "nome"
    if "api_nome" in df.columns and "nome" not in df.columns:
        rename["api_nome"] = "nome"
    if "api_data_nascimento" in df.columns and "dt_nascimento" not in df.columns:
        rename["api_data_nascimento"] = "dt_nascimento"
    if "logradouro" in df.columns and "endereco" not in df.columns:
        rename["logradouro"] = "endereco"
    if "clientes_endereco" in df.columns and "endereco" not in df.columns:
        rename["clientes_endereco"] = "endereco"
    if "clientes_cep" in df.columns and "cep" not in df.columns:
        rename["clientes_cep"] = "cep"
    if "clientes_cidade" in df.columns and "cidade" not in df.columns:
        rename["clientes_cidade"] = "cidade"
    if "clientes_estado" in df.columns and "uf" not in df.columns and "uf" not in rename.values():
        rename["clientes_estado"] = "uf"
    if "endereço" in df.columns and "endereco" not in df.columns and "endereco" not in rename.values():
        rename["endereço"] = "endereco"
    # Telefone single-value → renomear para padrão com sufixo numérico
    idx = 50
    for col in ("api_telefone", "clientes_telefone",
                "credlink_telefone_principal", "telefone"):
        if col in df.columns:
            rename[col] = f"telefone{idx}"
            idx += 1
    if rename:
        df = df.rename(columns=rename)

    # Para histórico, distribuidora_id vem no CSV
    distrib_id = None
    if "distribuidora_id" in df.columns:
        distrib_id = None  # per-row
    else:
        distrib_id = detectar_distrib_id(fp.name, df)

    return df, distrib_id


# ---------------------------------------------------------------------------
# Etapa 1: Deletar registros sem UC
# ---------------------------------------------------------------------------

def deletar_sem_uc(conn, dry_run: bool) -> int:
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE cliente_uc_id IS NULL")
    total = cur.fetchone()[0]
    log(f"  Registros com cliente_uc_id NULL: {total:,}")

    if total > 0 and not dry_run:
        cur.execute("DELETE FROM tabela_macros WHERE cliente_uc_id IS NULL")
        deleted = cur.rowcount
        conn.commit()
        log(f"  Deletados: {deleted:,}")
    elif total > 0:
        log(f"  [DRY-RUN] Seria deletado: {total:,}")
        deleted = total
    else:
        deleted = 0

    cur.close()
    return deleted


# ---------------------------------------------------------------------------
# Etapa 1.5: Corrigir nomes e enriquecer dados de clientes
# ---------------------------------------------------------------------------

def enriquecer_clientes(conn, fontes: list[dict], dry_run: bool) -> dict:
    """
    1) Corrige clientes com nome = nome de distribuidora.
    2) Preenche data_nascimento onde está NULL.
    3) Insere telefones para clientes que não têm nenhum.
    Retorna dict com stats.
    """
    cur = conn.cursor()
    stats = {"nomes_corrigidos": 0, "dt_nasc_preenchidos": 0, "telefones_inseridos": 0}

    # 1. Buscar clientes que precisam de correção
    placeholders = ",".join(["%s"] * len(NOMES_DISTRIBUIDORA))
    cur.execute(
        f"SELECT id, cpf, nome FROM clientes WHERE LOWER(TRIM(nome)) IN ({placeholders})",
        list(NOMES_DISTRIBUIDORA),
    )
    clientes_nome_ruim = {r[1]: (r[0], r[2]) for r in cur.fetchall()}
    log(f"  Clientes com nome de distribuidora: {len(clientes_nome_ruim):,}")

    cur.execute("SELECT cpf FROM clientes WHERE data_nascimento IS NULL")
    cpfs_sem_dt_nasc = {r[0] for r in cur.fetchall()}
    log(f"  Clientes sem data_nascimento:       {len(cpfs_sem_dt_nasc):,}")

    cur.execute(
        "SELECT c.id, c.cpf FROM clientes c"
        " LEFT JOIN telefones t ON t.cliente_id = c.id"
        " WHERE t.id IS NULL"
    )
    clientes_sem_tel = {r[1]: r[0] for r in cur.fetchall()}  # cpf -> id
    log(f"  Clientes sem telefone:              {len(clientes_sem_tel):,}")

    # Carregar telefones já existentes (para dedup global)
    cur.execute("SELECT cliente_id, telefone FROM telefones WHERE telefone IS NOT NULL")
    tel_existentes = {(r[0], int(r[1])) for r in cur.fetchall()}

    cpfs_alvo_nome = set(clientes_nome_ruim.keys())
    cpfs_alvo_tel = set(clientes_sem_tel.keys())
    cpfs_alvo = cpfs_alvo_nome | cpfs_sem_dt_nasc | cpfs_alvo_tel

    if not cpfs_alvo:
        cur.close()
        return stats

    # 2. Montar mapas CPF -> dado real a partir de TODAS as fontes
    #    (operacionais + raw enriquecimento)
    nome_map: dict[str, str] = {}      # cpf -> nome real
    dt_nasc_map: dict[str, str] = {}   # cpf -> yyyy-mm-dd
    tel_map: dict[str, list[tuple[int, str]]] = {}  # cpf -> [(telefone, tipo)]

    all_fontes = fontes + listar_fontes_enriquecimento()
    log(f"  Fontes para enriquecimento:         {len(all_fontes)} ({len(fontes)} oper + {len(all_fontes)-len(fontes)} raw)")

    for fi, fonte in enumerate(all_fontes, 1):
        try:
            df, _ = ler_fonte(fonte)
        except Exception:
            continue

        fp = fonte["path"]
        if "cpf" not in df.columns:
            continue

        # Vetorizar CPF e filtrar só os alvos
        df["cpf_norm"] = df["cpf"].apply(norm_cpf)
        df_match = df[df["cpf_norm"].isin(cpfs_alvo)].copy()
        if df_match.empty:
            continue

        col_dt = None
        for c in ("dt_nascimento", "data_nascimento"):
            if c in df.columns:
                col_dt = c
                break

        tel_cols = colunas_telefone(df)
        multi_cols = [c for c in df.columns
                      if c in ("api_telefones", "credlink_telefones")]

        n_antes_nome = len(nome_map)
        n_antes_dt = len(dt_nasc_map)
        n_antes_tel = len(tel_map)

        # Processar com itertuples (10-50x mais rápido que iterrows)
        cols_needed = ["cpf_norm"]
        if "nome" in df_match.columns:
            cols_needed.append("nome")
        if col_dt:
            cols_needed.append(col_dt)
        cols_needed += tel_cols + multi_cols
        cols_available = [c for c in cols_needed if c in df_match.columns]

        for tup in df_match[cols_available].itertuples(index=False):
            cpf = tup.cpf_norm

            # Nome
            if cpf in cpfs_alvo_nome and cpf not in nome_map:
                nome_val = getattr(tup, "nome", None) if "nome" in cols_available else None
                nome = norm_str(nome_val, 255)
                if nome and not nome_eh_distribuidora(nome):
                    nome_map[cpf] = nome

            # Data nascimento
            if cpf in cpfs_sem_dt_nasc and cpf not in dt_nasc_map and col_dt:
                dt_val = getattr(tup, col_dt, None)
                dt = norm_data_nascimento(dt_val)
                if dt:
                    dt_nasc_map[cpf] = dt

            # Telefones
            if cpf in cpfs_alvo_tel and (tel_cols or multi_cols):
                row_dict = {c: getattr(tup, c, None) for c in tel_cols + multi_cols if c in cols_available}
                phones = extrair_telefones_row(row_dict, tel_cols, multi_cols)
                if phones:
                    if cpf not in tel_map:
                        tel_map[cpf] = []
                    existing = {t[0] for t in tel_map[cpf]}
                    for tel_val, tipo in phones:
                        if tel_val not in existing:
                            tel_map[cpf].append((tel_val, tipo))
                            existing.add(tel_val)

        d_nome = len(nome_map) - n_antes_nome
        d_dt = len(dt_nasc_map) - n_antes_dt
        d_tel = len(tel_map) - n_antes_tel
        if d_nome or d_dt or d_tel:
            log(f"    [{fi}/{len(all_fontes)}] {fp.parent.name}/{fp.name}: +{d_nome} nomes, +{d_dt} dt_nasc, +{d_tel} tel")

    log(f"  Nomes reais encontrados:            {len(nome_map):,}")
    log(f"  Datas nascimento encontradas:       {len(dt_nasc_map):,}")
    log(f"  Clientes c/ tel nos fontes:         {len(tel_map):,}")

    # 3. Atualizar nomes (via temp table + UPDATE JOIN — 1 round trip)
    if nome_map:
        if not dry_run:
            cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_nome (cpf VARCHAR(14) PRIMARY KEY, nome VARCHAR(255))")
            cur.execute("TRUNCATE TABLE _tmp_nome")
            buf = [(cpf, nome_real) for cpf, nome_real in nome_map.items()]
            for i in range(0, len(buf), BATCH):
                chunk = buf[i:i + BATCH]
                vals = ",".join(["(%s,%s)"] * len(chunk))
                params = [v for cpf, nome in chunk for v in (cpf, nome)]
                cur.execute(f"INSERT INTO _tmp_nome VALUES {vals}", params)
            cur.execute("UPDATE clientes c JOIN _tmp_nome t ON c.cpf = t.cpf SET c.nome = t.nome")
            stats["nomes_corrigidos"] = cur.rowcount
            conn.commit()
            cur.execute("DROP TEMPORARY TABLE _tmp_nome")
            log(f"  Nomes atualizados:                  {stats['nomes_corrigidos']:,}")
        else:
            stats["nomes_corrigidos"] = len(nome_map)

    # 4. Atualizar data_nascimento (via temp table + UPDATE JOIN)
    if dt_nasc_map:
        if not dry_run:
            cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_dt (cpf VARCHAR(14) PRIMARY KEY, dt VARCHAR(10))")
            cur.execute("TRUNCATE TABLE _tmp_dt")
            buf = [(cpf, dt) for cpf, dt in dt_nasc_map.items()]
            for i in range(0, len(buf), BATCH):
                chunk = buf[i:i + BATCH]
                vals = ",".join(["(%s,%s)"] * len(chunk))
                params = [v for cpf, dt in chunk for v in (cpf, dt)]
                cur.execute(f"INSERT INTO _tmp_dt VALUES {vals}", params)
                if (i // BATCH) % 10 == 0:
                    log(f"    temp_dt: {min(i+BATCH, len(buf)):,}/{len(buf):,}")
            cur.execute("UPDATE clientes c JOIN _tmp_dt t ON c.cpf = t.cpf SET c.data_nascimento = t.dt WHERE c.data_nascimento IS NULL")
            stats["dt_nasc_preenchidos"] = cur.rowcount
            conn.commit()
            cur.execute("DROP TEMPORARY TABLE _tmp_dt")
            log(f"  Datas nascimento atualizadas:       {stats['dt_nasc_preenchidos']:,}")
        else:
            stats["dt_nasc_preenchidos"] = len(dt_nasc_map)

    # 5. Inserir telefones (INSERT multi-row — pymysql otimiza)
    data_criacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if tel_map:
        buf = []
        for cpf, tels in tel_map.items():
            cid = clientes_sem_tel[cpf]
            for tel_val, tipo in tels:
                if (cid, tel_val) not in tel_existentes:
                    buf.append((cid, tel_val, tipo, data_criacao))
                    tel_existentes.add((cid, tel_val))
                    stats["telefones_inseridos"] += 1

        if buf and not dry_run:
            for i in range(0, len(buf), BATCH):
                chunk = buf[i:i + BATCH]
                cur.executemany(
                    "INSERT INTO telefones (cliente_id, telefone, tipo, data_criacao)"
                    " VALUES (%s, %s, %s, %s)",
                    chunk,
                )
                conn.commit()
                if (i // BATCH) % 10 == 0:
                    log(f"    telefones: {min(i+BATCH, len(buf)):,}/{len(buf):,}")
            log(f"  Telefones inseridos:                {stats['telefones_inseridos']:,}")

    cur.close()
    return stats


# ---------------------------------------------------------------------------
# Etapa 2: Normalizar endereços existentes
# ---------------------------------------------------------------------------

def normalizar_enderecos(conn, dry_run: bool) -> dict:
    """
    Corrige dados inconsistentes em enderecos existentes:
    - bairro com literal 'NULL' → NULL
    - numero com literal 'NULL' → NULL
    - CEP com hífen → somente dígitos
    - Campos com strings 'NULL, NULL' → parte real ou NULL
    Retorna dict com stats.
    """
    cur = conn.cursor()
    stats = {"bairro_null_fix": 0, "numero_null_fix": 0, "cep_norm": 0}

    # 1. Bairro contendo 'NULL'
    cur.execute("SELECT COUNT(*) FROM enderecos WHERE bairro LIKE '%%NULL%%'")
    cnt = cur.fetchone()[0]
    stats["bairro_null_fix"] = cnt
    log(f"  Bairro com 'NULL' literal: {cnt:,}")
    if cnt and not dry_run:
        # 'NULL, NULL' → NULL;  '480, NULL' → '480'
        cur.execute(
            "UPDATE enderecos SET bairro = CASE"
            "  WHEN TRIM(REPLACE(REPLACE(bairro, 'NULL', ''), ',', '')) = '' THEN NULL"
            "  ELSE TRIM(TRAILING ', ' FROM TRIM(REPLACE(bairro, 'NULL', '')))"
            " END"
            " WHERE bairro LIKE '%%NULL%%'"
        )
        conn.commit()

    # 2. Numero = 'NULL'
    cur.execute("SELECT COUNT(*) FROM enderecos WHERE numero = 'NULL'")
    cnt = cur.fetchone()[0]
    stats["numero_null_fix"] = cnt
    log(f"  Numero = 'NULL' literal:   {cnt:,}")
    if cnt and not dry_run:
        cur.execute("UPDATE enderecos SET numero = NULL WHERE numero = 'NULL'")
        conn.commit()

    # 3. CEP com hífen
    cur.execute("SELECT COUNT(*) FROM enderecos WHERE cep LIKE '%%-%%'")
    cnt = cur.fetchone()[0]
    stats["cep_norm"] = cnt
    log(f"  CEP com hífen:             {cnt:,}")
    if cnt and not dry_run:
        cur.execute(
            "UPDATE enderecos SET cep = REPLACE(cep, '-', '') WHERE cep LIKE '%%-%%'"
        )
        conn.commit()

    cur.close()
    return stats


# ---------------------------------------------------------------------------
# Etapa 3: Registrar no staging + inserir pendentes faltantes
# ---------------------------------------------------------------------------

def registrar_staging(conn, fonte: dict, df: pd.DataFrame,
                      distrib_id_global: int | None, dry_run: bool) -> dict:
    """
    Registra TODAS as linhas com UC válida no staging.
    Retorna dict com stats.
    """
    cur = conn.cursor()
    fp = fonte["path"]
    filename = f"retroativo/{fp.parent.name}/{fp.name}"

    stats = {"total": len(df), "valid": 0, "invalid": 0, "staging_id": 0}

    if not dry_run:
        cur.execute(
            """INSERT INTO staging_imports
               (filename, distribuidora_nome, target_macro_table,
                total_rows, status, imported_by, started_at, finished_at)
               VALUES (%s, %s, %s, %s, 'completed', 'reimport_retroativo', NOW(), NOW())""",
            (filename, str(distrib_id_global) if distrib_id_global else "multi",
             "tabela_macros", len(df)),
        )
        staging_id = cur.lastrowid
        stats["staging_id"] = staging_id
    else:
        staging_id = 0

    # Vetorizar normalização
    df["_cpf"] = df["cpf"].apply(norm_cpf) if "cpf" in df.columns else None
    df["_uc"] = df["uc"].apply(norm_uc) if "uc" in df.columns else None

    valid_mask = df["_cpf"].notna() & df["_uc"].notna() & (df["_uc"] != "0000000000")
    stats["valid"] = int(valid_mask.sum())
    stats["invalid"] = len(df) - stats["valid"]

    if not dry_run:
        # Construir buf vetorizado
        raw_cpf = df["cpf"].fillna("").astype(str).str[:50] if "cpf" in df.columns else pd.Series([""] * len(df))
        raw_nome = df["nome"].fillna("").astype(str).str[:255] if "nome" in df.columns else pd.Series([None] * len(df))
        v_status = valid_mask.map({True: "valid", False: "invalid"})
        v_msg = valid_mask.map({True: None, False: "CPF ou UC ausente/invalida"})

        buf = list(zip(
            [staging_id] * len(df),
            range(len(df)),
            raw_cpf,
            raw_nome,
            [None] * len(df),
            [None] * len(df),
            df["_cpf"],
            df["_uc"],
            v_status,
            v_msg,
        ))

        for i in range(0, len(buf), BATCH):
            cur.executemany(
                """INSERT INTO staging_import_rows
                   (staging_id, row_idx, raw_cpf, raw_nome, raw_telefone,
                    raw_endereco, normalized_cpf, normalized_uc,
                    validation_status, validation_message)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                buf[i:i + BATCH],
            )
            conn.commit()

    if not dry_run:
        cur.execute(
            "UPDATE staging_imports SET rows_success=%s, rows_failed=%s WHERE id=%s",
            (stats["valid"], stats["invalid"], staging_id),
        )
        conn.commit()

    cur.close()
    return stats


def inserir_pendentes_faltantes(conn, fonte: dict, df: pd.DataFrame,
                                distrib_id_global: int | None,
                                dry_run: bool) -> int:
    """
    Para cada CPF+UC+distribuidora da fonte: se não existe em tabela_macros
    (com cliente_uc_id preenchido), insere como pendente.
    Também insere/atualiza nome, telefones e endereços do cliente.
    Usa temp tables para velocidade máxima contra RDS remoto.
    Retorna quantidade de novos pendentes inseridos.
    """
    cur = conn.cursor()
    tel_cols = colunas_telefone(df)
    multi_tel_cols = [c for c in df.columns if c in ("api_telefones", "credlink_telefones")]
    tem_campos_separados = "numero" in df.columns

    # ── 1. Extrair pares unicos (vetorizado) ───────────────────────
    df["n_cpf"] = df["cpf"].apply(norm_cpf) if "cpf" in df.columns else None
    df["n_uc"] = df["uc"].apply(norm_uc) if "uc" in df.columns else None

    valid = df["n_cpf"].notna() & df["n_uc"].notna() & (df["n_uc"] != "0000000000")
    if "distribuidora_id" in df.columns:
        df["n_did"] = pd.to_numeric(df["distribuidora_id"], errors="coerce")
        valid = valid & df["n_did"].notna()
    else:
        if not distrib_id_global:
            cur.close()
            return 0
        df["n_did"] = distrib_id_global

    df_valid = df[valid].copy()
    df_valid["n_did"] = df_valid["n_did"].astype(int)

    # Drop duplicates — keep first occurrence
    df_dedup = df_valid.drop_duplicates(subset=["n_cpf", "n_uc", "n_did"], keep="first")

    pares = {}  # (cpf, uc, did) -> row data dict
    for tup in df_dedup.itertuples(index=False):
        key = (tup.n_cpf, tup.n_uc, tup.n_did)
        pares[key] = {c: getattr(tup, c, None) for c in df_dedup.columns}

    if not pares:
        cur.close()
        return 0

    data_criacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pares_list = list(pares.keys())

    # ── 2. Verificar quais já existem (via temp table) ────────────────
    cur.execute("""CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_pairs (
        cpf VARCHAR(14), uc VARCHAR(20), did INT,
        INDEX idx_cpf (cpf), INDEX idx_uc_did (uc, did)
    )""")
    cur.execute("TRUNCATE TABLE _tmp_pairs")

    for i in range(0, len(pares_list), BATCH):
        chunk = pares_list[i:i + BATCH]
        vals = ",".join(["(%s,%s,%s)"] * len(chunk))
        params = [v for t in chunk for v in t]
        cur.execute(f"INSERT INTO _tmp_pairs VALUES {vals}", params)
    conn.commit()

    cur.execute("""
        SELECT DISTINCT tp.cpf, tp.uc, tp.did
        FROM _tmp_pairs tp
        JOIN clientes c ON c.cpf = tp.cpf
        JOIN cliente_uc cu ON cu.cliente_id = c.id AND cu.uc = tp.uc
                          AND cu.distribuidora_id = tp.did
        JOIN tabela_macros tm ON tm.cliente_uc_id = cu.id
    """)
    existentes = {(r[0], r[1], r[2]) for r in cur.fetchall()}
    cur.execute("DROP TEMPORARY TABLE _tmp_pairs")

    faltantes_keys = sorted(set(pares.keys()) - existentes)
    if not faltantes_keys:
        cur.close()
        return 0

    log(f"    Pares faltantes: {len(faltantes_keys):,} de {len(pares):,}")

    if dry_run:
        cur.close()
        return len(faltantes_keys)

    # ── 3. Resolver/criar clientes (via temp table) ────────────────
    cpfs_needed = list({cpf for cpf, uc, did in faltantes_keys})

    # Load existing clients
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_cpfs (cpf VARCHAR(14) PRIMARY KEY)")
    cur.execute("TRUNCATE TABLE _tmp_cpfs")
    for i in range(0, len(cpfs_needed), BATCH):
        chunk = cpfs_needed[i:i + BATCH]
        vals = ",".join(["(%s)"] * len(chunk))
        cur.execute(f"INSERT IGNORE INTO _tmp_cpfs VALUES {vals}", chunk)
    conn.commit()

    cpf_to_client: dict[str, tuple] = {}
    cur.execute("""
        SELECT c.id, c.cpf, c.nome, c.data_nascimento
        FROM clientes c JOIN _tmp_cpfs t ON c.cpf = t.cpf
    """)
    for r in cur.fetchall():
        cpf_to_client[r[1]] = (r[0], r[2], r[3])
    cur.execute("DROP TEMPORARY TABLE _tmp_cpfs")

    # Categorize: new inserts vs updates
    new_clients_buf = []
    update_nome_buf = []
    update_dt_buf = []
    update_both_buf = []
    cpfs_done = set()

    for cpf, uc, did in faltantes_keys:
        if cpf in cpfs_done:
            continue
        cpfs_done.add(cpf)
        row = pares[(cpf, uc, did)]
        nome = norm_str(row.get("nome"), 255)
        dt_nasc = norm_data_nascimento(
            row.get("dt_nascimento") or row.get("data_nascimento")
        )
        if cpf in cpf_to_client:
            cid, existing_nome, existing_dt = cpf_to_client[cpf]
            need_nome = nome and (not existing_nome or existing_nome == '' or nome_eh_distribuidora(existing_nome))
            need_dt = dt_nasc and not existing_dt
            if need_nome and need_dt:
                update_both_buf.append((nome, dt_nasc, cid))
            elif need_nome:
                update_nome_buf.append((nome, cid))
            elif need_dt:
                update_dt_buf.append((dt_nasc, cid))
        else:
            new_clients_buf.append((cpf, nome, dt_nasc, data_criacao))

    # Insert new clients
    if new_clients_buf:
        for i in range(0, len(new_clients_buf), BATCH):
            cur.executemany(
                "INSERT IGNORE INTO clientes (cpf, nome, data_nascimento, data_criacao)"
                " VALUES (%s, %s, %s, %s)",
                new_clients_buf[i:i + BATCH],
            )
        conn.commit()
        # Re-fetch IDs
        new_cpfs = [c[0] for c in new_clients_buf]
        for i in range(0, len(new_cpfs), BATCH):
            chunk = new_cpfs[i:i + BATCH]
            ph = ",".join(["%s"] * len(chunk))
            cur.execute(
                f"SELECT id, cpf FROM clientes WHERE cpf IN ({ph})", chunk
            )
            for r in cur.fetchall():
                cpf_to_client[r[1]] = (r[0], None, None)

    # Batch updates via temp table
    if update_both_buf or update_nome_buf or update_dt_buf:
        cur.execute("""CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_upd (
            cid INT PRIMARY KEY, nome VARCHAR(255), dt VARCHAR(10), mode TINYINT
        )""")
        cur.execute("TRUNCATE TABLE _tmp_upd")
        all_upd = []
        for nome, dt, cid in update_both_buf:
            all_upd.append((cid, nome, dt, 3))
        for nome, cid in update_nome_buf:
            all_upd.append((cid, nome, None, 1))
        for dt, cid in update_dt_buf:
            all_upd.append((cid, None, dt, 2))
        for i in range(0, len(all_upd), BATCH):
            chunk = all_upd[i:i + BATCH]
            vals = ",".join(["(%s,%s,%s,%s)"] * len(chunk))
            params = [v for row in chunk for v in row]
            cur.execute(f"INSERT INTO _tmp_upd VALUES {vals}", params)
        cur.execute("""
            UPDATE clientes c JOIN _tmp_upd t ON c.id = t.cid
            SET c.nome = CASE WHEN t.mode IN (1,3) THEN t.nome ELSE c.nome END,
                c.data_nascimento = CASE WHEN t.mode IN (2,3) THEN t.dt ELSE c.data_nascimento END
        """)
        conn.commit()
        cur.execute("DROP TEMPORARY TABLE _tmp_upd")

    # ── 4. Resolver/criar cliente_uc (via temp table) ──────────────
    uc_triples = []
    for cpf, uc, did in faltantes_keys:
        cid = cpf_to_client[cpf][0]
        uc_triples.append((cid, uc, did))

    unique_ucs = list(set(uc_triples))
    uc_to_id: dict[tuple, int] = {}

    # Load existing
    cur.execute("""CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ucs (
        cid INT, uc VARCHAR(20), did INT,
        INDEX idx_cud (cid, uc, did)
    )""")
    cur.execute("TRUNCATE TABLE _tmp_ucs")
    for i in range(0, len(unique_ucs), BATCH):
        chunk = unique_ucs[i:i + BATCH]
        vals = ",".join(["(%s,%s,%s)"] * len(chunk))
        params = [v for t in chunk for v in t]
        cur.execute(f"INSERT INTO _tmp_ucs VALUES {vals}", params)
    conn.commit()

    cur.execute("""
        SELECT cu.id, cu.cliente_id, cu.uc, cu.distribuidora_id
        FROM cliente_uc cu
        JOIN _tmp_ucs t ON cu.cliente_id = t.cid AND cu.uc = t.uc
                       AND cu.distribuidora_id = t.did
    """)
    for r in cur.fetchall():
        uc_to_id[(r[1], r[2], r[3])] = r[0]
    cur.execute("DROP TEMPORARY TABLE _tmp_ucs")

    new_ucs = [(cid, uc, did, data_criacao)
               for cid, uc, did in unique_ucs
               if (cid, uc, did) not in uc_to_id]
    if new_ucs:
        for i in range(0, len(new_ucs), BATCH):
            cur.executemany(
                "INSERT IGNORE INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao)"
                " VALUES (%s, %s, %s, %s)",
                new_ucs[i:i + BATCH],
            )
        conn.commit()
        # Re-fetch newly created IDs
        re_ucs = [(cid, uc, did) for cid, uc, did, _ in new_ucs
                   if (cid, uc, did) not in uc_to_id]
        for i in range(0, len(re_ucs), BATCH):
            chunk = re_ucs[i:i + BATCH]
            ph = ",".join(["(%s,%s,%s)"] * len(chunk))
            params = [v for t in chunk for v in t]
            cur.execute(
                f"SELECT id, cliente_id, uc, distribuidora_id FROM cliente_uc"
                f" WHERE (cliente_id, uc, distribuidora_id) IN ({ph})",
                params,
            )
            for r in cur.fetchall():
                uc_to_id[(r[1], r[2], r[3])] = r[0]

    # ── 5. Inserir tabela_macros ────────────────────────────────────
    macro_buf = []
    skipped = 0
    for cpf, uc, did in faltantes_keys:
        cid = cpf_to_client[cpf][0]
        key = (cid, uc, did)
        if key not in uc_to_id:
            cur.execute(
                "SELECT id FROM cliente_uc WHERE cliente_id=%s AND uc=%s AND distribuidora_id=%s",
                key,
            )
            row = cur.fetchone()
            if row:
                uc_to_id[key] = row[0]
            else:
                skipped += 1
                continue
        uc_id = uc_to_id[key]
        macro_buf.append((cid, did, uc_id, RESPOSTA_PENDENTE, data_criacao))

    if skipped:
        log(f"    AVISO: {skipped} pares sem cliente_uc — pulados")

    for i in range(0, len(macro_buf), BATCH):
        cur.executemany(
            "INSERT IGNORE INTO tabela_macros"
            " (cliente_id, distribuidora_id, cliente_uc_id, resposta_id, status, data_criacao)"
            " VALUES (%s, %s, %s, %s, 'pendente', %s)",
            macro_buf[i:i + BATCH],
        )
        conn.commit()

    # ── 6. Inserir telefones ───────────────────────────────────────
    if tel_cols or multi_tel_cols:
        cur.execute("SELECT cliente_id, telefone FROM telefones WHERE telefone IS NOT NULL")
        tel_existentes = {(r[0], int(r[1])) for r in cur.fetchall()}

        tel_buf = []
        for cpf, uc, did in faltantes_keys:
            row = pares[(cpf, uc, did)]
            cid = cpf_to_client[cpf][0]
            phones = extrair_telefones_row(row, tel_cols, multi_tel_cols)
            for tel_val, tipo in phones:
                if (cid, tel_val) not in tel_existentes:
                    tel_buf.append((cid, tel_val, tipo, data_criacao))
                    tel_existentes.add((cid, tel_val))

        if tel_buf:
            for i in range(0, len(tel_buf), BATCH):
                cur.executemany(
                    "INSERT IGNORE INTO telefones (cliente_id, telefone, tipo, data_criacao)"
                    " VALUES (%s, %s, %s, %s)",
                    tel_buf[i:i + BATCH],
                )
                conn.commit()

    # ── 7. Inserir endereços ───────────────────────────────────────
    if tem_campos_separados:
        cur.execute("SELECT cliente_uc_id, COALESCE(cep,'') FROM enderecos")
        end_existentes = {(r[0], str(r[1]).strip()) for r in cur.fetchall()}

        end_buf = []
        for cpf, uc, did in faltantes_keys:
            row = pares[(cpf, uc, did)]
            cid = cpf_to_client[cpf][0]
            uc_id = uc_to_id[(cid, uc, did)]

            logradouro = norm_str(row.get("endereco"), 255)
            if not logradouro:
                continue
            numero_end = norm_str(row.get("numero"), 50)
            bairro = norm_str(row.get("bairro"), 100)
            cep = norm_cep(row.get("cep"))
            cidade = norm_str(row.get("cidade"), 100)
            uf = norm_uf(row.get("uf"))
            cep_key = cep or ""

            if (uc_id, cep_key) not in end_existentes:
                end_buf.append((cid, uc_id, did, logradouro, numero_end,
                                bairro, cidade, uf, cep, data_criacao))
                end_existentes.add((uc_id, cep_key))

        if end_buf:
            for i in range(0, len(end_buf), BATCH):
                cur.executemany(
                    "INSERT IGNORE INTO enderecos"
                    " (cliente_id, cliente_uc_id, distribuidora_id,"
                    "  endereco, numero, bairro, cidade, uf, cep, data_criacao)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    end_buf[i:i + BATCH],
                )
                conn.commit()

    conn.commit()
    cur.close()
    return len(faltantes_keys)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool):
    log(SEP)
    log("REIMPORTAÇÃO RETROATIVA COMPLETA")
    log("Modo: DRY-RUN" if dry_run else "Modo: EXECUÇÃO REAL")
    log(SEP)

    progress = {} if dry_run else load_progress()
    if progress:
        done_steps = [k for k, v in progress.get("steps", {}).items() if v.get("done")]
        done_sources = list(progress.get("sources_done", {}).keys())
        log(f"  Progresso anterior carregado: {len(done_steps)} etapas, {len(done_sources)} fontes")

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    conn.autocommit(False)

    # ── Diagnóstico inicial ─────────────────────────────────────────────
    log()
    log("[1/6] Diagnóstico inicial")
    log("-" * 60)
    cur = conn.cursor()
    cur.execute("""
        SELECT status, COUNT(*),
               SUM(cliente_uc_id IS NOT NULL), SUM(cliente_uc_id IS NULL)
        FROM tabela_macros GROUP BY status
        ORDER BY FIELD(status, 'pendente','processando','reprocessar','consolidado','excluido')
    """)
    rows = cur.fetchall()
    log(f"  {'Status':<15} {'Total':>10} {'Com UC':>10} {'Sem UC':>10}")
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    for status, total, com_uc, sem_uc in rows:
        log(f"  {status:<15} {total:>10,} {com_uc:>10,} {sem_uc:>10,}")
    cur.close()

    # ── Etapa 2: Deletar sem UC ─────────────────────────────────────────
    log()
    if step_done(progress, "delete_null_uc"):
        log("[2/6] Deletar registros sem cliente_uc_id — JÁ FEITO, pulando")
        deleted = progress["steps"]["delete_null_uc"]["stats"]["deleted"]
    else:
        log("[2/6] Deletar registros sem cliente_uc_id")
        log("-" * 60)
        deleted = deletar_sem_uc(conn, dry_run)
        if not dry_run:
            mark_step(progress, "delete_null_uc", {"deleted": deleted})

    # ── Etapa 3: Enriquecer clientes ───────────────────────────────────
    fontes = listar_fontes()
    log()
    if step_done(progress, "enrich"):
        log("[3/6] Enriquecer dados de clientes — JÁ FEITO, pulando")
        enrich_stats = progress["steps"]["enrich"]["stats"]
    else:
        log("[3/6] Enriquecer dados de clientes (nomes + dt nascimento + telefones)")
        log("-" * 60)
        enrich_stats = enriquecer_clientes(conn, fontes, dry_run)
        log(f"  Nomes corrigidos:       {enrich_stats['nomes_corrigidos']:,}")
        log(f"  Dt nascimento preench.: {enrich_stats['dt_nasc_preenchidos']:,}")
        log(f"  Telefones inseridos:    {enrich_stats['telefones_inseridos']:,}")
        if not dry_run:
            mark_step(progress, "enrich", enrich_stats)

    # ── Etapa 4: Normalizar enderecos ──────────────────────────────────
    log()
    if step_done(progress, "normalize_addr"):
        log("[4/6] Normalizar endereços existentes — JÁ FEITO, pulando")
        end_stats = progress["steps"]["normalize_addr"]["stats"]
    else:
        log("[4/6] Normalizar endereços existentes")
        log("-" * 60)
        end_stats = normalizar_enderecos(conn, dry_run)
        if not dry_run:
            mark_step(progress, "normalize_addr", end_stats)

    # ── Etapa 5: Reimportar de todas as fontes ──────────────────────────
    log()
    log("[5/6] Reimportar retroativamente de todas as fontes")
    log("-" * 60)
    log(f"  Fontes encontradas: {len(fontes)}")

    total_staging_valid = 0
    total_staging_invalid = 0
    total_pendentes_novos = 0

    for i, fonte in enumerate(fontes, 1):
        fp = fonte["path"]
        source_key = f"{fp.parent.name}/{fp.name}"

        if source_done(progress, source_key):
            prev = progress["sources_done"][source_key]
            log(f"\n  [{i}/{len(fontes)}] {source_key} — JÁ FEITO (prev: {prev.get('novos',0):,} novos)")
            total_staging_valid += prev.get("valid", 0)
            total_staging_invalid += prev.get("invalid", 0)
            total_pendentes_novos += prev.get("novos", 0)
            continue

        log(f"\n  [{i}/{len(fontes)}] {source_key}")

        try:
            df, distrib_id = ler_fonte(fonte)
        except Exception as e:
            log(f"    ERRO ao ler: {e}")
            if not dry_run:
                mark_source(progress, source_key, {"error": str(e), "valid": 0, "invalid": 0, "novos": 0})
            continue

        log(f"    Linhas: {len(df):,}  |  distrib_id: {distrib_id or 'per-row'}")

        # a) Registrar no staging
        st = registrar_staging(conn, fonte, df, distrib_id, dry_run)
        total_staging_valid += st["valid"]
        total_staging_invalid += st["invalid"]
        log(f"    Staging: {st['valid']:,} validas, {st['invalid']:,} invalidas (sid={st['staging_id']})")

        # b) Inserir pendentes que faltam
        novos = inserir_pendentes_faltantes(conn, fonte, df, distrib_id, dry_run)
        total_pendentes_novos += novos
        log(f"    Pendentes novos inseridos: {novos:,}")

        # c) Salvar progresso desta fonte
        if not dry_run:
            mark_source(progress, source_key, {
                "valid": st["valid"], "invalid": st["invalid"],
                "novos": novos, "staging_id": st["staging_id"],
            })

    # Marcar etapa 5 completa
    if not dry_run:
        mark_step(progress, "reimport", {
            "staging_valid": total_staging_valid,
            "staging_invalid": total_staging_invalid,
            "pendentes_novos": total_pendentes_novos,
        })

    # ── Resumo final ────────────────────────────────────────────────────
    log()
    log("[6/6] Resumo final")
    log("-" * 60)
    cur = conn.cursor()
    cur.execute("""
        SELECT status, COUNT(*),
               SUM(cliente_uc_id IS NOT NULL), SUM(cliente_uc_id IS NULL)
        FROM tabela_macros GROUP BY status
        ORDER BY FIELD(status, 'pendente','processando','reprocessar','consolidado','excluido')
    """)
    rows = cur.fetchall()
    total_g = 0
    total_su = 0
    log(f"  {'Status':<15} {'Total':>10} {'Com UC':>10} {'Sem UC':>10}")
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    for status, total, com_uc, sem_uc in rows:
        log(f"  {status:<15} {total:>10,} {com_uc:>10,} {sem_uc:>10,}")
        total_g += total
        total_su += sem_uc
    log(f"  {'-'*15} {'-'*10} {'-'*10} {'-'*10}")
    log(f"  {'TOTAL':<15} {total_g:>10,} {total_g - total_su:>10,} {total_su:>10,}")
    cur.close()

    enrich_nomes = enrich_stats.get('nomes_corrigidos', 0) if isinstance(enrich_stats, dict) else 0
    enrich_dt = enrich_stats.get('dt_nasc_preenchidos', 0) if isinstance(enrich_stats, dict) else 0
    enrich_tel = enrich_stats.get('telefones_inseridos', 0) if isinstance(enrich_stats, dict) else 0
    end_bairro = end_stats.get('bairro_null_fix', 0) if isinstance(end_stats, dict) else 0
    end_cep = end_stats.get('cep_norm', 0) if isinstance(end_stats, dict) else 0

    log()
    log(f"  Registros sem UC deletados:     {deleted:,}")
    log(f"  Nomes distribuidora corrigidos: {enrich_nomes:,}")
    log(f"  Dt nascimento preenchidos:      {enrich_dt:,}")
    log(f"  Telefones inseridos:            {enrich_tel:,}")
    log(f"  Enderecos normalizados (bairro):{end_bairro:,}")
    log(f"  Enderecos normalizados (CEP):   {end_cep:,}")
    log(f"  Staging registrados (validos):  {total_staging_valid:,}")
    log(f"  Staging registrados (invalidos):{total_staging_invalid:,}")
    log(f"  Pendentes novos em tabela_macros: {total_pendentes_novos:,}")

    conn.close()

    # Limpar arquivo de progresso ao concluir com sucesso
    if not dry_run and PROGRESS_FILE.exists():
        PROGRESS_FILE.rename(PROGRESS_FILE.with_suffix(".json.done"))
        log("  Progresso salvo como progress.json.done")

    log()
    log(SEP)
    log("CONCLUIDO")
    log(SEP)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Limpeza + reimportação retroativa completa"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar nada no banco")
    args = parser.parse_args()
    run(args.dry_run)
