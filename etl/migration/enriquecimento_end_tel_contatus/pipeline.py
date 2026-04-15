"""
enriquecimento_end_tel_contatus/pipeline.py
============================================
Enriquece clientes sem endereço e/ou telefone buscando dados no banco
externo bd_contatus (tabela `latest_contacts`).

Estratégia de performance:
  - Busca apenas CPFs de clientes que NÃO possuem endereço ou telefone
  - Queries no bd_contatus via batch IN(...) usando índice `idx_latest_contacts_cpf`
  - Batch size configurável (padrão 500 CPFs por query)
  - Progresso salvo em JSON para retomada automática

Fonte externa:
  bd_contatus.latest_contacts  (~140M registros, índice em CPF)
  Campos usados: ENDERECO, NUM_END, COMPLEMENTO, BAIRRO, cidade, CEP, UF,
                 telefone_1..telefone_6

Destino local:
  - enderecos   (cliente_id, endereco, numero, bairro, cidade, uf, cep, cliente_uc_id)
  - telefones   (cliente_id, telefone, tipo)

Uso:
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py --dry-run
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py --reset
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py --batch-size 200
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py --only endereco
    python etl/migration/enriquecimento_end_tel_contatus/pipeline.py --only telefone
"""

import sys
import re
import json
import argparse
from datetime import datetime
from pathlib import Path

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino, db_contatus  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
STATE_DIR  = Path(__file__).resolve().parent / "state"
PROGRESSO  = STATE_DIR / "enriquecimento_progresso.json"
BATCH_SIZE = 500     # CPFs por query no bd_contatus


# ---------------------------------------------------------------------------
# Progresso
# ---------------------------------------------------------------------------

def carregar_progresso() -> dict:
    if not PROGRESSO.exists():
        return {}
    try:
        with open(PROGRESSO, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def salvar_progresso(data: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    data["atualizado_em"] = datetime.now().isoformat(timespec="seconds")
    with open(PROGRESSO, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def limpar_telefone(val) -> int | None:
    """Extrai só dígitos e retorna como int, ou None se inválido."""
    if val is None:
        return None
    s = re.sub(r"\D", "", str(val).strip())
    if not s or len(s) < 10:
        return None
    return int(s)


def classificar_telefone(num: int) -> str:
    """Classifica telefone como 'celular' ou 'fixo' pelo 9° dígito."""
    s = str(num)
    # DDD (2 dígitos) + número
    if len(s) >= 11 and s[2] in ("9", "8", "7"):
        return "celular"
    return "fixo"


# ---------------------------------------------------------------------------
# Buscar clientes sem dados
# ---------------------------------------------------------------------------

def buscar_clientes_sem_dados(conn_local, modo: str) -> list[tuple]:
    """
    Retorna lista de (cliente_id, cpf) para clientes que precisam de
    enriquecimento.
    modo: 'ambos', 'endereco', 'telefone'
    """
    cur = conn_local.cursor()

    if modo == "endereco":
        cur.execute("""
            SELECT c.id, c.cpf
            FROM clientes c
            LEFT JOIN enderecos e ON e.cliente_id = c.id
            WHERE e.id IS NULL
            ORDER BY c.id
        """)
    elif modo == "telefone":
        cur.execute("""
            SELECT c.id, c.cpf
            FROM clientes c
            LEFT JOIN telefones t ON t.cliente_id = c.id
            WHERE t.id IS NULL
            ORDER BY c.id
        """)
    else:  # ambos — clientes que faltam endereço OU telefone
        cur.execute("""
            SELECT c.id, c.cpf
            FROM clientes c
            LEFT JOIN enderecos e ON e.cliente_id = c.id
            LEFT JOIN telefones t ON t.cliente_id = c.id
            WHERE e.id IS NULL OR t.id IS NULL
            ORDER BY c.id
        """)

    return cur.fetchall()


def buscar_cliente_uc_ids(conn_local, cliente_ids: list) -> dict:
    """Retorna {cliente_id: min_cliente_uc_id} para insert em enderecos."""
    if not cliente_ids:
        return {}
    cur = conn_local.cursor()
    fmt = ",".join(["%s"] * len(cliente_ids))
    cur.execute(f"""
        SELECT cliente_id, MIN(id) AS uc_id
        FROM cliente_uc
        WHERE cliente_id IN ({fmt})
        GROUP BY cliente_id
    """, cliente_ids)
    return {r[0]: r[1] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Buscar dados no bd_contatus
# ---------------------------------------------------------------------------

def buscar_contatus_batch(conn_contatus, cpfs: list) -> dict:
    """
    Busca endereços e telefones no bd_contatus para um batch de CPFs.
    Retorna {cpf: {endereco:..., telefones:[...]}}.

    Usa latest_contacts (índice em CPF, ~0.15s por batch de 500).
    Um CPF pode ter múltiplas linhas (vários endereços) — pega a mais
    recente (por DATA_ENTRADA DESC, ID_COMPLEMENT DESC).
    """
    if not cpfs:
        return {}

    cur = conn_contatus.cursor()
    fmt = ",".join(["%s"] * len(cpfs))

    cur.execute(f"""
        SELECT CPF,
               ENDERECO, NUM_END, COMPLEMENTO, BAIRRO, cidade, CEP, UF,
               telefone_1, telefone_2, telefone_3,
               telefone_4, telefone_5, telefone_6,
               ID_COMPLEMENT
        FROM latest_contacts
        WHERE CPF IN ({fmt})
        ORDER BY CPF, ID_COMPLEMENT DESC
    """, cpfs)

    resultado: dict = {}
    for row in cur.fetchall():
        cpf = str(row[0]).strip().zfill(11)

        # Se já mapeamos esse CPF, pula (pegamos o mais recente — primeiro pelo ORDER)
        if cpf in resultado:
            continue

        endereco_data = None
        if row[1] and str(row[1]).strip():  # ENDERECO não vazio
            endereco_data = {
                "endereco":    str(row[1]).strip()[:255],
                "numero":      str(row[2]).strip()[:50] if row[2] else None,
                "complemento": str(row[3]).strip()[:255] if row[3] else None,
                "bairro":      str(row[4]).strip()[:100] if row[4] else None,
                "cidade":      str(row[5]).strip()[:100] if row[5] else None,
                "cep":         re.sub(r"\D", "", str(row[6]).strip())[:20] if row[6] else None,
                "uf":          str(row[7]).strip().upper()[:2] if row[7] else None,
            }

        telefones = []
        for i in range(8, 14):  # telefone_1..telefone_6
            tel = limpar_telefone(row[i])
            if tel and tel not in telefones:
                telefones.append(tel)

        resultado[cpf] = {
            "endereco": endereco_data,
            "telefones": telefones,
        }

    return resultado


# ---------------------------------------------------------------------------
# Inserir dados locais
# ---------------------------------------------------------------------------

def inserir_enderecos(conn_local, registros: list):
    """
    registros: [(cliente_id, cliente_uc_id, endereco, numero, complemento,
                 bairro, cidade, uf, cep), ...]
    """
    if not registros:
        return 0
    cur = conn_local.cursor()
    ph = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(registros))
    vals = []
    for r in registros:
        vals.extend(r)
    cur.execute(f"""
        INSERT IGNORE INTO enderecos
        (cliente_id, cliente_uc_id, endereco, numero, complemento,
         bairro, cidade, uf, cep)
        VALUES {ph}
    """, vals)
    return cur.rowcount


def inserir_telefones(conn_local, registros: list):
    """
    registros: [(cliente_id, telefone, tipo), ...]
    """
    if not registros:
        return 0
    cur = conn_local.cursor()
    ph = ",".join(["(%s,%s,%s)"] * len(registros))
    vals = []
    for r in registros:
        vals.extend(r)
    cur.execute(f"""
        INSERT IGNORE INTO telefones
        (cliente_id, telefone, tipo)
        VALUES {ph}
    """, vals)
    return cur.rowcount


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enriquece clientes sem endereço/telefone via bd_contatus"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar")
    parser.add_argument("--reset", action="store_true",
                        help="Ignora progresso e recomeça")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"CPFs por query (default: {BATCH_SIZE})")
    parser.add_argument("--only", choices=["endereco", "telefone"],
                        default=None,
                        help="Enriquecer apenas endereço ou telefone")
    args = parser.parse_args()

    modo = args.only or "ambos"
    batch_size = args.batch_size

    print("=" * 70)
    print("ENRIQUECIMENTO  —  Endereço & Telefone via bd_contatus")
    print(f"  Modo: {modo}  |  Batch: {batch_size}  |  Dry-run: {args.dry_run}")
    print("=" * 70)

    # --- Conectar banco local ---
    conn_local = pymysql.connect(**db_destino(autocommit=False))

    # --- Buscar clientes que precisam de enriquecimento ---
    print("\n[1/4] Buscando clientes sem dados ...")
    clientes = buscar_clientes_sem_dados(conn_local, modo)
    total = len(clientes)
    print(f"       {total:,} clientes para enriquecer")

    if total == 0:
        print("\n[OK] Todos os clientes já possuem dados. Nada a fazer.")
        conn_local.close()
        return

    # --- Check existing enderecos/telefones per cliente ---
    # Build sets for clients that already have each type
    cur_local = conn_local.cursor()
    cliente_ids_all = [c[0] for c in clientes]

    # Clients that already have enderecos
    tem_endereco = set()
    tem_telefone = set()
    if modo == "ambos":
        # Fetch in chunks to avoid huge IN clause
        for i in range(0, len(cliente_ids_all), 5000):
            chunk = cliente_ids_all[i:i+5000]
            fmt = ",".join(["%s"] * len(chunk))
            cur_local.execute(f"SELECT DISTINCT cliente_id FROM enderecos WHERE cliente_id IN ({fmt})", chunk)
            tem_endereco.update(r[0] for r in cur_local.fetchall())
            cur_local.execute(f"SELECT DISTINCT cliente_id FROM telefones WHERE cliente_id IN ({fmt})", chunk)
            tem_telefone.update(r[0] for r in cur_local.fetchall())
        print(f"       Já têm endereço: {len(tem_endereco):,}  |  Já têm telefone: {len(tem_telefone):,}")

    # --- Progresso ---
    prog = {} if args.reset else carregar_progresso()
    start_idx = prog.get("ultimo_idx", 0) if prog.get("modo") == modo else 0
    stats = {
        "enderecos_inseridos": prog.get("enderecos_inseridos", 0) if start_idx > 0 else 0,
        "telefones_inseridos": prog.get("telefones_inseridos", 0) if start_idx > 0 else 0,
        "cpfs_encontrados":    prog.get("cpfs_encontrados", 0) if start_idx > 0 else 0,
        "cpfs_nao_encontrados":prog.get("cpfs_nao_encontrados", 0) if start_idx > 0 else 0,
    }
    if start_idx > 0:
        print(f"\n[INFO] Retomando do índice {start_idx}")

    if args.dry_run:
        # Em dry-run, busca um batch pequeno para mostrar preview
        print("\n[DRY-RUN] Preview com primeiro batch ...")
        conn_contatus = pymysql.connect(**db_contatus(read_timeout=30))
        sample_cpfs = [c[1] for c in clientes[:min(10, total)]]
        dados = buscar_contatus_batch(conn_contatus, sample_cpfs)
        for cpf, info in list(dados.items())[:5]:
            end = info["endereco"]
            tels = info["telefones"]
            print(f"  CPF={cpf}: end={'SIM' if end else 'NAO'} "
                  f"({end['endereco'][:40] if end else '-'}...)  "
                  f"tels={len(tels)} ({tels[:2]})")
        conn_contatus.close()
        conn_local.close()
        print(f"\n[DRY-RUN] {total:,} clientes seriam processados.")
        return

    # --- Conectar bd_contatus ---
    print("\n[2/4] Conectando ao bd_contatus ...")
    conn_contatus = pymysql.connect(**db_contatus(read_timeout=60))
    print("       Conectado!")

    # --- Processar em lotes ---
    print(f"\n[3/4] Processando {total:,} clientes em batches de {batch_size} ...")
    iniciado_em = datetime.now().isoformat(timespec="seconds")

    for batch_start in range(start_idx, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = clientes[batch_start:batch_end]

        cpfs = [c[1] for c in batch]
        cid_map = {c[1]: c[0] for c in batch}  # cpf → cliente_id

        # Buscar no contatus
        dados = buscar_contatus_batch(conn_contatus, cpfs)
        stats["cpfs_encontrados"] += len(dados)
        stats["cpfs_nao_encontrados"] += len(cpfs) - len(dados)

        # Preparar inserts
        enderecos_batch = []
        telefones_batch = []

        # Buscar cliente_uc_ids para este batch (necessário para enderecos)
        ids_com_dados = [cid_map[cpf] for cpf in dados if cpf in cid_map]
        uc_map = buscar_cliente_uc_ids(conn_local, ids_com_dados) if ids_com_dados else {}

        for cpf, info in dados.items():
            cid = cid_map.get(cpf)
            if not cid:
                continue

            # Endereço
            if (modo in ("ambos", "endereco")) and cid not in tem_endereco and info["endereco"]:
                end = info["endereco"]
                uc_id = uc_map.get(cid, 0)  # fallback 0 se não tiver UC
                if uc_id:
                    enderecos_batch.append((
                        cid, uc_id,
                        end["endereco"], end["numero"], end["complemento"],
                        end["bairro"], end["cidade"], end["uf"], end["cep"],
                    ))
                    tem_endereco.add(cid)

            # Telefones
            if (modo in ("ambos", "telefone")) and cid not in tem_telefone and info["telefones"]:
                for tel in info["telefones"]:
                    tipo = classificar_telefone(tel)
                    telefones_batch.append((cid, tel, tipo))
                tem_telefone.add(cid)

        # Inserir
        if enderecos_batch:
            n = inserir_enderecos(conn_local, enderecos_batch)
            stats["enderecos_inseridos"] += n

        if telefones_batch:
            n = inserir_telefones(conn_local, telefones_batch)
            stats["telefones_inseridos"] += n

        conn_local.commit()

        # Progresso
        salvar_progresso({
            "modo": modo,
            "total": total,
            "ultimo_idx": batch_end,
            **stats,
            "iniciado_em": iniciado_em,
        })

        # Log
        if batch_end % (batch_size * 20) < batch_size or batch_end >= total:
            pct = batch_end / total * 100
            print(f"  ... {batch_end:,}/{total:,} ({pct:.1f}%)  "
                  f"end={stats['enderecos_inseridos']:,}  "
                  f"tel={stats['telefones_inseridos']:,}  "
                  f"found={stats['cpfs_encontrados']:,}  "
                  f"miss={stats['cpfs_nao_encontrados']:,}")

    # --- Finalizar ---
    conn_contatus.close()
    conn_local.close()

    print(f"\n[4/4] Concluído!")
    print("=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"  Clientes processados   : {total:,}")
    print(f"  Encontrados no contatus: {stats['cpfs_encontrados']:,}")
    print(f"  Não encontrados        : {stats['cpfs_nao_encontrados']:,}")
    print(f"  Endereços inseridos    : {stats['enderecos_inseridos']:,}")
    print(f"  Telefones inseridos    : {stats['telefones_inseridos']:,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
