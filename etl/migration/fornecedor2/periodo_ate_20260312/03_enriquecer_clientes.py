"""
04_enriquecer_clientes.py
=========================
Enriquece clientes em `bd_Automacoes_time_dadosV2` com nome, endereÃ§o e
telefones vindos de `controle_bases.neo`.

EstratÃ©gia de performance (sem Ã­ndice em neo.cpf_cnpj):
  - Keyset pagination por `id` (PRIMARY KEY) â€” chunks de 50k linhas.
    Cada chunk abre/fecha sua prÃ³pria conexÃ£o de leitura.  Sem SSCursor,
    sem conexÃ£o longa que o RDS possa derrubar por timeout.
  - Filtro em Python: dict de 115k CPFs â†’ lookup O(1) por linha.
  - Writes em batch com executemany() â€” um round-trip por lote.
  - DeduplicaÃ§Ã£o em memÃ³ria (sets) para evitar INSERTs duplicados.
  - Idempotente: pode ser re-executado sem duplicar dados.

Uso:
    python etl/ingestion/04_enriquecer_clientes.py              # full run
    python etl/ingestion/04_enriquecer_clientes.py --dry-run    # sem gravar
    python etl/ingestion/04_enriquecer_clientes.py --batch 2000 # linhas por commit
    python etl/ingestion/04_enriquecer_clientes.py --limit 5000 # mÃ¡x linhas a ler
    python etl/ingestion/04_enriquecer_clientes.py --chunk 50000 # linhas por query
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pymysql
import pymysql.cursors

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configuração de conexão
# ---------------------------------------------------------------------------
DB_CONFIG = db_destino(autocommit=False, connect_timeout=30)

# ---------------------------------------------------------------------------
# Arquivo de progresso  (mesma pasta do script)
# ---------------------------------------------------------------------------
PROGRESSO_FILE = Path(__file__).resolve().parent / "import_log.txt"


def carregar_progresso() -> dict:
    """Lê o arquivo de estado. Retorna dict vazio se não existir."""
    if PROGRESSO_FILE.exists():
        with open(PROGRESSO_FILE, encoding="utf-8") as f:
            estado = json.load(f)
        print(f"[INFO] Retomando de last_id={estado['last_id']:,}  "
              f"(total lido até agora: {estado['total']:,})")
        return estado
    return {}


def salvar_progresso(last_id: int, total: int, lote: int, stats: dict) -> None:
    """Persiste o estado atual no arquivo JSON."""
    estado = {
        "last_id": last_id,
        "total":   total,
        "lote":    lote,
        "stats":   stats,
        "salvo_em": datetime.now().isoformat(timespec="seconds"),
    }
    with open(PROGRESSO_FILE, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)


def remover_progresso() -> None:
    """Remove o arquivo de estado ao finalizar com sucesso."""
    if PROGRESSO_FILE.exists():
        PROGRESSO_FILE.unlink()


# ---------------------------------------------------------------------------
# Query paginada por id (keyset pagination â€” usa PRIMARY KEY, O(n) total).
# Cada chunk Ã© uma query nova com conexÃ£o prÃ³pria â†’ sem timeout de conexÃ£o.
# ---------------------------------------------------------------------------
QUERY_CHUNK = """
    SELECT
        n.id,
        n.cpf_cnpj,
        n.nome,
        n.endereco,
        n.cidade,
        n.estado,
        n.cep,
        n.dd1, n.telefone1,
        n.dd2, n.telefone2,
        n.dd3, n.telefone3,
        n.dd4, n.telefone4,
        n.dd5, n.telefone5,
        n.dd6, n.telefone6,
        n.dd7, n.telefone7,
        n.dd8, n.telefone8
    FROM controle_bases.neo n
    WHERE n.id > %s
    ORDER BY n.id
    LIMIT %s
"""

SEP = "=" * 70


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalizar_str(valor, maxlen: int = 255):
    if not valor:
        return None
    s = str(valor).strip()
    return s[:maxlen] if s else None


def limpar_uf(estado):
    if not estado:
        return None
    s = re.sub(r"[^A-Za-z]", "", str(estado).strip())
    return s[:2].upper() if len(s) >= 2 else None


def parsear_endereco(endereco_raw: str) -> dict:
    """
    Separa 'R IBITUBA 56, MANGABEIRA, FEIRA DE SANTANA - BA'
    em logradouro='R IBITUBA', numero='56', bairro='MANGABEIRA'.
    """
    resultado = {"logradouro": None, "numero": None, "bairro": None}
    if not endereco_raw:
        return resultado
    s = str(endereco_raw).strip()
    partes = [p.strip() for p in s.split(",")]
    if partes:
        m = re.match(r"^(.*?)\s+(\d[\w\-]*)$", partes[0])
        if m:
            resultado["logradouro"] = m.group(1).strip()[:255]
            resultado["numero"]     = m.group(2).strip()[:50]
        else:
            resultado["logradouro"] = partes[0][:255]
    if len(partes) >= 2:
        bairro_partes = partes[1:-1] if len(partes) > 3 else [partes[1]]
        resultado["bairro"] = ", ".join(bairro_partes)[:100]
    return resultado


def montar_telefone(dd, numero):
    """Retorna (int_tel, tipo) ou (None, None). Tipo: 'celular' (9 dÃ­gitos) ou 'fixo' (8)."""
    if not dd or not numero:
        return None, None
    dd_str  = re.sub(r"\D", "", str(dd).strip())
    num_str = re.sub(r"\D", "", str(numero).strip())
    if not dd_str or not num_str or dd_str == "99":
        return None, None
    if len(num_str) == 9:
        tipo = "celular"
    elif len(num_str) == 8:
        tipo = "fixo"
    else:
        return None, None
    try:
        return int(dd_str + num_str), tipo
    except ValueError:
        return None, None


# ---------------------------------------------------------------------------
# DeduplicaÃ§Ã£o em memÃ³ria â€” carrega apenas IDs (leve)
# ---------------------------------------------------------------------------

def carregar_enderecos_set(cur) -> set:
    print("[INFO] Carregando endereÃ§os existentes...")
    cur.execute("SELECT cliente_id, COALESCE(cep,'') FROM enderecos")
    s = {(r[0], str(r[1]).strip()) for r in cur.fetchall()}
    print(f"[INFO] {len(s):,} endereÃ§os jÃ¡ existentes.")
    return s


def carregar_telefones_set(cur) -> set:
    print("[INFO] Carregando telefones existentes...")
    cur.execute("SELECT cliente_id, telefone FROM telefones WHERE telefone IS NOT NULL")
    s = {(r[0], int(r[1])) for r in cur.fetchall()}
    print(f"[INFO] {len(s):,} telefones jÃ¡ existentes.")
    return s


def carregar_cpf_map(cur) -> dict:
    """cpf (str 11 dÃ­gitos) â†’ cliente_id"""
    print("[INFO] Carregando mapa CPF â†’ cliente_id...")
    cur.execute("SELECT cpf, id FROM clientes")
    m = {r[0]: r[1] for r in cur.fetchall()}
    print(f"[INFO] {len(m):,} CPFs no destino.")
    return m


def carregar_cliente_uc_map(cur) -> dict:
    """cliente_id â†’ cliente_uc_id (o primeiro)"""
    print("[INFO] Carregando mapa cliente_id â†’ cliente_uc_id...")
    cur.execute("SELECT cliente_id, MIN(id) FROM cliente_uc GROUP BY cliente_id")
    m = {r[0]: r[1] for r in cur.fetchall()}
    print(f"[INFO] {len(m):,} clientes com UC.")
    return m


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def conectar():
    return pymysql.connect(**DB_CONFIG)


def processar_row(row, cpf_map, uc_map, enderecos_set, telefones_set,
                  buf_nomes, buf_enderecos, buf_telefones, stats):
    """Processa uma linha de neo e popula os buffers. Retorna True se Ãºtil."""
    (
        _row_id, cpf_raw,
        nome, endereco_raw, cidade, estado, cep,
        dd1, tel1, dd2, tel2, dd3, tel3, dd4, tel4,
        dd5, tel5, dd6, tel6, dd7, tel7, dd8, tel8,
    ) = row

    cpf = str(cpf_raw).strip() if cpf_raw else None
    if not cpf or cpf not in cpf_map:
        stats["ignorados"] += 1
        return False

    cliente_id    = cpf_map[cpf]
    cliente_uc_id = uc_map.get(cliente_id)

    nome_clean = normalizar_str(nome, 255)
    if nome_clean:
        buf_nomes.append((nome_clean, cliente_id))

    partes    = parsear_endereco(endereco_raw)
    cep_clean = normalizar_str(cep, 20)
    cep_key   = cep_clean or ""
    if partes["logradouro"] and cliente_uc_id and (cliente_id, cep_key) not in enderecos_set:
        buf_enderecos.append((
            cliente_id, cliente_uc_id,
            partes["logradouro"], partes["numero"], partes["bairro"],
            normalizar_str(cidade, 100), limpar_uf(estado), cep_clean,
        ))
        enderecos_set.add((cliente_id, cep_key))
        stats["enderecos"] += 1

    for dd, num in [
        (dd1, tel1), (dd2, tel2), (dd3, tel3), (dd4, tel4),
        (dd5, tel5), (dd6, tel6), (dd7, tel7), (dd8, tel8),
    ]:
        tel, tipo = montar_telefone(dd, num)
        if tel and (cliente_id, tel) not in telefones_set:
            buf_telefones.append((cliente_id, tel, tipo))
            telefones_set.add((cliente_id, tel))
            stats["telefones"] += 1

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="NÃ£o grava nada no banco")
    parser.add_argument("--batch", type=int, default=500,
                        help="Linhas por commit/flush (default: 500)")
    parser.add_argument("--chunk", type=int, default=50_000,
                        help="Linhas por query de leitura (default: 50000)")
    parser.add_argument("--limit", type=int, default=0,
                        help="MÃ¡x linhas totais a processar (0 = sem limite)")
    args = parser.parse_args()

    if args.dry_run:
        print("[INFO] Modo DRY-RUN â€” nada serÃ¡ gravado.\n")

    try:
        conn_w = conectar()
        print("[OK] Conectado.\n")
    except Exception as e:
        print(f"[ERRO] {e}")
        sys.exit(1)

    cur_w = conn_w.cursor()

    cpf_map       = carregar_cpf_map(cur_w)
    uc_map        = carregar_cliente_uc_map(cur_w)
    enderecos_set = carregar_enderecos_set(cur_w)
    telefones_set = carregar_telefones_set(cur_w)

    stats        = {"nomes": 0, "enderecos": 0, "telefones": 0, "ignorados": 0}
    buf_nomes    = []
    buf_enderecos = []
    buf_telefones = []
    total        = 0
    lote         = 0
    last_id      = 0
    t0           = time.time()

    def flush(force=False):
        nonlocal lote, last_id
        if not (force or len(buf_nomes) + len(buf_enderecos) + len(buf_telefones) >= args.batch):
            return
        if not args.dry_run:
            if buf_nomes:
                cur_w.executemany(
                    "UPDATE clientes SET nome=%s WHERE id=%s AND nome IS NULL",
                    buf_nomes,
                )
                stats["nomes"] += cur_w.rowcount
            if buf_enderecos:
                cur_w.executemany(
                    """INSERT INTO enderecos
                        (cliente_id, cliente_uc_id, endereco, numero, bairro,
                         cidade, uf, cep, data_criacao)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                    buf_enderecos,
                )
            if buf_telefones:
                cur_w.executemany(
                    "INSERT INTO telefones (cliente_id, telefone, tipo, data_criacao)"
                    " VALUES (%s,%s,%s,NOW())",
                    buf_telefones,
                )
            conn_w.commit()
            salvar_progresso(last_id, total, lote, stats)
        else:
            stats["nomes"] += len(buf_nomes)

        lote   += 1
        elapsed = time.time() - t0
        vel     = total / elapsed if elapsed else 0
        uteis   = total - stats["ignorados"]
        print(
            f"  Lote {lote:>4}  |  {total:>9,} lidos  {uteis:>7,} Ãºteis"
            f"  |  nomes={stats['nomes']}  end={stats['enderecos']}"
            f"  tel={stats['telefones']}  ({elapsed:.0f}s  {vel:.0f} lin/s)"
        )
        buf_nomes.clear()
        buf_enderecos.clear()
        buf_telefones.clear()

    print(f"[INFO] PaginaÃ§Ã£o em chunks de {args.chunk:,} linhas (keyset por id).\n{SEP}")

    while True:
        # â”€â”€ Abre conexÃ£o de leitura nova a cada chunk â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        try:
            conn_r = conectar()
            cur_r  = conn_r.cursor()
            cur_r.execute(QUERY_CHUNK, (last_id, args.chunk))
            rows = cur_r.fetchall()
            cur_r.close()
            conn_r.close()
        except Exception as e:
            print(f"[ERRO leitura] {e}")
            sys.exit(1)

        if not rows:
            break

        for row in rows:
            last_id = row[0]  # atualiza a cada linha para o save ser preciso
            processar_row(
                row, cpf_map, uc_map, enderecos_set, telefones_set,
                buf_nomes, buf_enderecos, buf_telefones, stats,
            )
            total += 1
            flush()
            if args.limit > 0 and total >= args.limit:
                break

        last_id = rows[-1][0]  # garante que o keyset cursor avança ao fim do chunk

        if args.limit > 0 and total >= args.limit:
            break

    flush(force=True)

    cur_w.close()
    conn_w.close()

    elapsed = time.time() - t0
    print(f"\n{SEP}")
    print("RESULTADO FINAL")
    print(SEP)
    print(f"  Linhas lidas (neo) : {total:>10,}")
    print(f"  Ignoradas (CPF nÃ£o encontrado): {stats['ignorados']:>4,}")
    print(f"  Nomes atualizados  : {stats['nomes']:>10,}")
    print(f"  EndereÃ§os inseridos: {stats['enderecos']:>10,}")
    print(f"  Telefones inseridos: {stats['telefones']:>10,}")
    print(f"  Tempo total        : {elapsed:>10.1f}s")
    if args.dry_run:
        print("\n  DRY-RUN — nenhuma alteração foi gravada.")
    else:
        remover_progresso()
        print("  Arquivo de progresso removido (execução completa).")
    print(SEP)


if __name__ == "__main__":
    main()


