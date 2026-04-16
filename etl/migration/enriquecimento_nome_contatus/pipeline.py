"""
enriquecimento_nome_contatus/pipeline.py
===========================================
Enriquece clientes sem nome buscando dados no banco
externo bd_contatus (tabela `latest_contacts`).

Estratégia de performance:
  - Busca apenas CPFs de clientes que NÃO possuem nome
  - Queries no bd_contatus via batch IN(...) usando índice `idx_latest_contacts_cpf`
  - Batch size configurável (padrão 500 CPFs por query)
  - Progresso salvo em JSON para retomada automática

Fonte externa:
  bd_contatus.latest_contacts  (~140M registros, índice em CPF)
  Campo usado: NOME

Destino local:
  - clientes (UPDATE nome WHERE id = ...)

Uso:
    python etl/migration/enriquecimento_nome_contatus/pipeline.py
    python etl/migration/enriquecimento_nome_contatus/pipeline.py --dry-run
    python etl/migration/enriquecimento_nome_contatus/pipeline.py --reset
    python etl/migration/enriquecimento_nome_contatus/pipeline.py --batch-size 200
"""

import sys
import json
import argparse
import time
from datetime import datetime
from pathlib import Path

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino, db_contatus  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
STATE_DIR  = Path(__file__).resolve().parent / "state"
PROGRESSO  = STATE_DIR / "enriquecimento_nome_progresso.json"
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
# Buscar clientes sem nome
# ---------------------------------------------------------------------------

def buscar_clientes_sem_nome(conn_local) -> list[tuple]:
    """
    Retorna lista de (cliente_id, cpf) para clientes que precisam de
    enriquecimento de nome.
    """
    cur = conn_local.cursor()
    cur.execute("""
        SELECT id, cpf
        FROM clientes
        WHERE nome IS NULL
        ORDER BY id
    """)
    return cur.fetchall()


def buscar_contatus_batch_nome(conn_contatus, cpfs: list) -> dict:
    """
    Busca nomes no bd_contatus para um batch de CPFs.
    Retorna {cpf: nome}.
    """
    if not cpfs:
        return {}

    cur = conn_contatus.cursor()
    fmt = ",".join(["%s"] * len(cpfs))

    cur.execute(f"""
        SELECT CPF, NOME, ID_COMPLEMENT
        FROM latest_contacts
        WHERE CPF IN ({fmt})
        ORDER BY CPF, ID_COMPLEMENT DESC
    """, cpfs)

    resultado: dict = {}
    for row in cur.fetchall():
        cpf = str(row[0]).strip().zfill(11)
        nome = str(row[1]).strip() if row[1] else None

        # Se já mapeamos esse CPF, pula (pegamos o mais recente)
        if cpf in resultado:
            continue

        if nome:
            resultado[cpf] = nome

    return resultado


def atualizar_nomes(conn_local, registros: list):
    """
    registros: [(cliente_id, nome), ...]
    """
    if not registros:
        return 0
    cur = conn_local.cursor()
    updated = 0
    for cliente_id, nome in registros:
        retries = 3
        while retries > 0:
            try:
                cur.execute("""
                    UPDATE clientes
                    SET nome = %s
                    WHERE id = %s AND nome IS NULL
                """, (nome, cliente_id))
                updated += 1
                break
            except pymysql.err.OperationalError as e:
                if "Deadlock" in str(e):
                    retries -= 1
                    time.sleep(0.1)  # short delay
                    continue
                else:
                    raise
        if retries == 0:
            print(f"Falhou ao atualizar cliente {cliente_id} após 3 tentativas")
    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento de nomes via bd_contatus")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar banco")
    parser.add_argument("--reset", action="store_true", help="Reinicia progresso do zero")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="CPFs por batch")
    args = parser.parse_args()

    progresso = carregar_progresso()
    if args.reset:
        progresso = {}
        salvar_progresso(progresso)
        print("Progresso resetado.")
        return

    batch_size = args.batch_size
    dry_run = args.dry_run

    try:
        conn_local = pymysql.connect(**db_destino())
        conn_contatus = pymysql.connect(**db_contatus())

        # Buscar clientes sem nome
        clientes = buscar_clientes_sem_nome(conn_local)
        total_clientes = len(clientes)
        print(f"Clientes sem nome encontrados: {total_clientes}")

        if total_clientes == 0:
            print("Nenhum cliente precisa de enriquecimento.")
            return

        # Processar em batches
        processados = progresso.get("processados", 0)
        atualizados = progresso.get("atualizados", 0)

        for i in range(processados // batch_size, (total_clientes + batch_size - 1) // batch_size):
            start = i * batch_size
            end = min(start + batch_size, total_clientes)
            batch = clientes[start:end]
            cpfs = [c[1] for c in batch]

            print(f"Processando batch {i+1}: CPFs {start+1}-{end} de {total_clientes}")

            # Buscar nomes no contatus
            nomes = buscar_contatus_batch_nome(conn_contatus, cpfs)

            # Preparar updates
            registros = []
            for cliente_id, cpf in batch:
                nome = nomes.get(cpf)
                if nome:
                    registros.append((cliente_id, nome))

            if registros:
                if not dry_run:
                    count = atualizar_nomes(conn_local, registros)
                    atualizados += count
                    conn_local.commit()
                else:
                    print(f"[DRY-RUN] Atualizaria {len(registros)} nomes")
                print(f"  -> {len(registros)} nomes encontrados e atualizados")

            # Salvar progresso
            progresso["processados"] = end
            progresso["atualizados"] = atualizados
            salvar_progresso(progresso)

        print(f"Enriquecimento concluído. Total atualizados: {atualizados}")

    except Exception as e:
        print(f"Erro: {e}")
        raise
    finally:
        if 'conn_local' in locals():
            conn_local.close()
        if 'conn_contatus' in locals():
            conn_contatus.close()


if __name__ == "__main__":
    main()