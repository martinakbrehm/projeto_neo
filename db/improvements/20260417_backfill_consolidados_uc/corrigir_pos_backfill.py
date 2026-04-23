"""
Correção pós-backfill — etapas cirúrgicas em sequência.
Cada etapa é idempotente e pode ser re-executada com segurança.

Etapa 1: Limpar nomes de distribuidora (SET NULL)
Etapa 2: Limpar staging_imports duplicados (manter 1 por filename retroativo)
Etapa 3: Analisar duplicatas em tabela_macros (relatório antes de agir)
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import config
import pymysql

DRY_RUN = "--dry-run" in sys.argv

c = pymysql.connect(**config.db_destino(), connect_timeout=30)
c.autocommit(False)
cur = c.cursor()

# ═══════════════════════════════════════════════════════════════════
# ETAPA 1: Limpar nomes de distribuidora
# ═══════════════════════════════════════════════════════════════════
print("=" * 60)
print("ETAPA 1: Limpar nomes de distribuidora → NULL")
print("=" * 60)

nomes_dist = (
    'COELBA','COSERN','CELPE','CELP','BRASILIA',
    'coelba','cosern','celpe','celp','brasilia',
    'Coelba','Cosern','Celpe','Celp','Brasilia',
    'Neoenergia Coelba','Neoenergia Cosern','Neoenergia Celpe',
    'Neoenergia Brasilia','neoenergia coelba','neoenergia cosern',
    'neoenergia celpe','neoenergia brasilia',
)
ph = ",".join(["%s"] * len(nomes_dist))

cur.execute(f"SELECT COUNT(*) FROM clientes WHERE nome IN ({ph})", nomes_dist)
count = cur.fetchone()[0]
print(f"  Clientes com nome de distribuidora: {count}")

if count > 0:
    if DRY_RUN:
        print(f"  [DRY-RUN] Não alterado.")
    else:
        cur.execute(f"UPDATE clientes SET nome = NULL WHERE nome IN ({ph})", nomes_dist)
        print(f"  Atualizados: {cur.rowcount}")
        c.commit()

# Verificar
cur.execute(f"SELECT COUNT(*) FROM clientes WHERE nome IN ({ph})", nomes_dist)
print(f"  Restantes após: {cur.fetchone()[0]}")

# ═══════════════════════════════════════════════════════════════════
# ETAPA 2: Limpar staging_imports duplicados
# ═══════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("ETAPA 2: Limpar staging_imports duplicados")
print("=" * 60)

# Encontrar filenames com múltiplas entradas
cur.execute("""
    SELECT filename, COUNT(*) as qtd,
           MIN(id) as keep_id,
           GROUP_CONCAT(id ORDER BY id) as all_ids
    FROM staging_imports
    GROUP BY filename
    HAVING COUNT(*) > 1
    ORDER BY qtd DESC
""")
dups = cur.fetchall()
print(f"  Filenames com duplicatas: {len(dups)}")

total_rows_deleted = 0
total_imports_deleted = 0
for filename, qtd, keep_id, all_ids in dups:
    ids_list = [int(x) for x in all_ids.split(",")]
    to_delete = [x for x in ids_list if x != keep_id]
    print(f"  {filename}")
    print(f"    Total: {qtd}, Manter: sid={keep_id}, Excluir: {to_delete}")

    if not DRY_RUN and to_delete:
        ph_del = ",".join(["%s"] * len(to_delete))
        # Deletar rows primeiro (FK)
        cur.execute(f"DELETE FROM staging_import_rows WHERE staging_id IN ({ph_del})", to_delete)
        rows_del = cur.rowcount
        total_rows_deleted += rows_del
        # Deletar imports
        cur.execute(f"DELETE FROM staging_imports WHERE id IN ({ph_del})", to_delete)
        total_imports_deleted += cur.rowcount
        c.commit()
        print(f"    Excluídos: {len(to_delete)} imports, {rows_del} rows")
    elif DRY_RUN:
        # Contar rows que seriam excluídas
        ph_del = ",".join(["%s"] * len(to_delete))
        cur.execute(f"SELECT COUNT(*) FROM staging_import_rows WHERE staging_id IN ({ph_del})", to_delete)
        rows_count = cur.fetchone()[0]
        total_rows_deleted += rows_count
        total_imports_deleted += len(to_delete)
        print(f"    [DRY-RUN] Excluiria: {len(to_delete)} imports, {rows_count} rows")

print(f"  Total staging_imports excluídos: {total_imports_deleted}")
print(f"  Total staging_import_rows excluídos: {total_rows_deleted}")

# Verificar estado final
cur.execute("SELECT COUNT(*) FROM staging_imports")
print(f"  staging_imports restantes: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM staging_import_rows")
print(f"  staging_import_rows restantes: {cur.fetchone()[0]}")

# ═══════════════════════════════════════════════════════════════════
# ETAPA 3: Remover duplicatas do backfill em tabela_macros
# REGRA DE SEGURANÇA:
#   O backfill só inseriu registros como 'pendente'.
#   Logo, SÓ deletamos registros com status='pendente' que sejam
#   duplicatas (mesmo cliente_id+distribuidora_id+cliente_uc_id)
#   de um registro ORIGINAL (ID menor) que já existia.
#   NUNCA tocamos em consolidado, excluido, processando, reprocessar.
#
#   Caso: CPF+UC tem consolidado (id=100) + pendente (id=500, backfill)
#     → deleta id=500 (pendente duplicado do backfill)
#   Caso: CPF+UC tem pendente (id=100, original) + pendente (id=500, backfill)
#     → deleta id=500 (pendente duplicado do backfill)
#   Caso: CPF+UC tem excluido (id=100) + excluido (id=500)
#     → NÃO deleta nada (ambos podem ser legítimos, backfill não cria excluido)
#   Caso: CPF+UC tem consolidado (id=100) + consolidado (id=500)
#     → NÃO deleta nada (backfill não cria consolidado)
# ═══════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("ETAPA 3: Remover duplicatas PENDENTES do backfill em tabela_macros")
print("=" * 60)

# Panorama geral de duplicatas
cur.execute("""
    SELECT COUNT(*), COALESCE(SUM(qtd - 1), 0) FROM (
        SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
        FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1
    ) t
""")
n_combos, n_excess = cur.fetchone()
print(f"  Combos com duplicatas (geral): {n_combos:,}")
print(f"  Registros em excesso (geral):  {n_excess:,}")

# Identificar: registros PENDENTES que são duplicatas
# (i.e., mesma combo já tem pelo menos um registro com ID menor)
cur.execute("""
    SELECT t.id
    FROM tabela_macros t
    WHERE t.status = 'pendente'
      AND t.cliente_uc_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM tabela_macros o
          WHERE o.cliente_id       = t.cliente_id
            AND o.distribuidora_id = t.distribuidora_id
            AND o.cliente_uc_id    = t.cliente_uc_id
            AND o.id < t.id
      )
""")
ids = [r[0] for r in cur.fetchall()]
print(f"\n  Pendentes duplicados (backfill artifacts) a excluir: {len(ids):,}")

if not ids:
    print("  Nenhum pendente duplicado. OK!")
else:
    # Mostrar o que será MANTIDO nessas combos (o original com ID menor)
    # Para confirmar que consolidados/excluidos estão seguros
    cur.execute("""
        SELECT orig_status, COUNT(*) FROM (
            SELECT (
                SELECT o.status FROM tabela_macros o
                WHERE o.cliente_id = t.cliente_id
                  AND o.distribuidora_id = t.distribuidora_id
                  AND o.cliente_uc_id = t.cliente_uc_id
                  AND o.id < t.id
                ORDER BY o.id ASC
                LIMIT 1
            ) as orig_status
            FROM tabela_macros t
            WHERE t.status = 'pendente'
              AND t.cliente_uc_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM tabela_macros o
                  WHERE o.cliente_id       = t.cliente_id
                    AND o.distribuidora_id = t.distribuidora_id
                    AND o.cliente_uc_id    = t.cliente_uc_id
                    AND o.id < t.id
              )
        ) sub
        GROUP BY orig_status ORDER BY COUNT(*) DESC
    """)
    print("\n  Status do ORIGINAL mantido (ID menor) dessas combos:")
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,}")

    # Quantos consolidados existem em combos com duplicata (para tranquilidade)
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros
        WHERE status = 'consolidado'
          AND cliente_uc_id IS NOT NULL
          AND (cliente_id, distribuidora_id, cliente_uc_id) IN (
              SELECT cliente_id, distribuidora_id, cliente_uc_id
              FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
              GROUP BY cliente_id, distribuidora_id, cliente_uc_id
              HAVING COUNT(*) > 1
          )
    """)
    n_consol = cur.fetchone()[0]
    print(f"\n  Consolidados em combos duplicadas (NENHUM será removido): {n_consol:,}")

    # Verificar combos que ficam duplicadas MESMO DEPOIS (i.e., 2+ não-pendentes)
    # para dar visibilidade
    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(qtd - 1), 0) FROM (
            SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
            FROM tabela_macros
            WHERE cliente_uc_id IS NOT NULL
              AND status != 'pendente'
            GROUP BY cliente_id, distribuidora_id, cliente_uc_id
            HAVING COUNT(*) > 1
        ) t
    """)
    still_dup, still_excess = cur.fetchone()
    print(f"  Combos que continuarão duplicadas (não-pendentes): {still_dup:,} ({still_excess:,} excess)")
    if still_dup:
        print("    ^ Esses são legítimos (ex: excluido+reprocessar), NÃO serão removidos.")

    if DRY_RUN:
        print(f"\n  [DRY-RUN] Nenhum registro excluído.")
    else:
        print(f"\n  Excluindo {len(ids):,} pendentes duplicados do backfill...")
        BATCH = 5000
        deleted = 0
        for i in range(0, len(ids), BATCH):
            chunk = ids[i:i + BATCH]
            ph = ",".join(["%s"] * len(chunk))
            cur.execute(f"DELETE FROM tabela_macros WHERE id IN ({ph})", chunk)
            deleted += cur.rowcount
            c.commit()
            if (i // BATCH) % 5 == 0:
                print(f"    {deleted:,}/{len(ids):,}", flush=True)
        print(f"  Total excluídos: {deleted:,}")

        # Verificação final
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT cliente_id, distribuidora_id, cliente_uc_id
                FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
                GROUP BY cliente_id, distribuidora_id, cliente_uc_id
                HAVING COUNT(*) > 1
            ) t
        """)
        print(f"  Duplicatas restantes (podem ser legítimas): {cur.fetchone()[0]}")

# Estado final
print()
print("=" * 60)
print("ESTADO FINAL")
print("=" * 60)
cur.execute("""
    SELECT status, COUNT(*) FROM tabela_macros
    GROUP BY status
    ORDER BY FIELD(status,'pendente','processando','reprocessar','consolidado','excluido')
""")
total = 0
for r in cur.fetchall():
    print(f"  {r[0]:<15} {r[1]:>10,}")
    total += r[1]
print(f"  {'TOTAL':<15} {total:>10,}")

c.close()
print("\nCONCLUÍDO")
