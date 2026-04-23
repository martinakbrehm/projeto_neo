"""Diagnóstico completo da tabela staging_imports."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

SEP = "=" * 70

print(SEP)
print("DIAGNÓSTICO: staging_imports")
print(SEP)

# Schema
cur.execute("DESCRIBE staging_imports")
print("\nEstrutura:")
for r in cur.fetchall():
    print(f"  {r[0]:25s} {r[1]:30s} {r[2]:5s} {r[3]:5s}")

# Total
cur.execute("SELECT COUNT(*) FROM staging_imports")
total = cur.fetchone()[0]
print(f"\nTotal de registros: {total}")

# All records
print(f"\n{SEP}")
print("TODOS OS REGISTROS (ordenados por id):")
print(SEP)
cur.execute("""
    SELECT id, filename, imported_by, created_at, status,
           total_rows, rows_success, rows_failed,
           distribuidora_nome, target_macro_table
    FROM staging_imports
    ORDER BY id
""")
rows = cur.fetchall()
for r in rows:
    print(f"\n  id={r[0]:>4d}")
    print(f"    filename:      {r[1]!r}")
    print(f"    imported_by:   {r[2]!r}")
    print(f"    created_at:    {r[3]}")
    print(f"    status:        {r[4]}")
    print(f"    total/ok/fail: {r[5]}/{r[6]}/{r[7]}")
    print(f"    dist/table:    {r[8]}/{r[9]}")

# Duplicates by filename (ignoring path)
print(f"\n{SEP}")
print("DUPLICATAS (mesmo nome de arquivo base):")
print(SEP)
cur.execute("""
    SELECT
        SUBSTRING_INDEX(filename, '/', -1) AS base_name,
        GROUP_CONCAT(id ORDER BY id SEPARATOR ', ') AS ids,
        GROUP_CONCAT(filename ORDER BY id SEPARATOR '\n      ') AS filenames,
        COUNT(*) AS qtd
    FROM staging_imports
    GROUP BY SUBSTRING_INDEX(filename, '/', -1)
    HAVING COUNT(*) > 1
    ORDER BY base_name
""")
dups = cur.fetchall()
if dups:
    for r in dups:
        print(f"\n  Base: {r[0]!r}")
        print(f"    IDs: {r[1]}")
        print(f"    Filenames:\n      {r[2]}")
        print(f"    Qtd: {r[3]}")
else:
    print("  Nenhuma duplicata encontrada por nome base.")

# Also check by exact filename
print(f"\n{SEP}")
print("DUPLICATAS (filename exato):")
print(SEP)
cur.execute("""
    SELECT filename,
           GROUP_CONCAT(id ORDER BY id SEPARATOR ', ') AS ids,
           COUNT(*) AS qtd
    FROM staging_imports
    GROUP BY filename
    HAVING COUNT(*) > 1
    ORDER BY filename
""")
dups2 = cur.fetchall()
if dups2:
    for r in dups2:
        print(f"  {r[0]!r}  ids=[{r[1]}] ({r[2]}x)")
else:
    print("  Nenhuma duplicata exata.")

# Filenames with full paths
print(f"\n{SEP}")
print("FILENAMES COM CAMINHO COMPLETO (contém \\ ou mais de 1 /):")
print(SEP)
cur.execute(r"""
    SELECT id, filename FROM staging_imports
    WHERE filename LIKE '%%\\%%'
       OR filename LIKE '%%/%%/%%'
    ORDER BY id
""")
paths = cur.fetchall()
if paths:
    for r in paths:
        print(f"  id={r[0]:>4d}  {r[1]!r}")
else:
    print("  Nenhum com caminho completo.")

# Filenames with single subdir (date folders)
print(f"\n{SEP}")
print("FILENAMES COM SUBPASTA (contém exatamente 1 /):")
print(SEP)
cur.execute("""
    SELECT id, filename FROM staging_imports
    WHERE filename LIKE '%%/%%'
      AND filename NOT LIKE '%%/%%/%%'
    ORDER BY id
""")
subdirs = cur.fetchall()
if subdirs:
    for r in subdirs:
        print(f"  id={r[0]:>4d}  {r[1]!r}")
else:
    print("  Nenhum.")

# imported_by values
print(f"\n{SEP}")
print("VALORES DISTINTOS DE imported_by:")
print(SEP)
cur.execute("""
    SELECT imported_by, COUNT(*) as qtd,
           GROUP_CONCAT(id ORDER BY id SEPARATOR ', ') AS ids
    FROM staging_imports
    GROUP BY imported_by
    ORDER BY imported_by
""")
for r in cur.fetchall():
    print(f"  {r[0]!r:50s} ({r[1]}x) ids=[{r[2]}]")

# Check staging_import_rows references
print(f"\n{SEP}")
print("REFERÊNCIAS em staging_import_rows:")
print(SEP)
cur.execute("""
    SELECT si.id, si.filename,
           COUNT(sir.id) AS rows_in_sir
    FROM staging_imports si
    LEFT JOIN staging_import_rows sir ON sir.staging_id = si.id
    GROUP BY si.id, si.filename
    ORDER BY si.id
""")
for r in cur.fetchall():
    print(f"  id={r[0]:>4d}  rows={r[2]:>8,}  {r[1]!r}")

c.close()
print(f"\n{SEP}")
print("FIM DO DIAGNÓSTICO")
print(SEP)
