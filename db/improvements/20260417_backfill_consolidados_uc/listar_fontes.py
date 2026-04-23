import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

print("=== staging_imports registrados ===")
cur.execute("SELECT id, filename, total_rows, rows_success, rows_failed FROM staging_imports ORDER BY id")
for r in cur.fetchall():
    sid, fn, total, ok, fail = r
    print(f"  sid={sid:>2} total={total or 0:>8,} ok={ok or 0:>8,} fail={fail or 0:>8,}  {fn}")

print("\n=== Fontes que o script deveria ter processado ===")

# Listar fontes do script
dados = Path(ROOT) / "dados" / "fornecedor2"
print(f"\nPasta dados/fornecedor2:")
for sub in sorted(dados.iterdir()):
    if sub.is_dir():
        files = list(sub.rglob("*.csv")) + list(sub.rglob("*.xlsx"))
        print(f"  {sub.name}/ ({len(files)} arquivos)")
        for f in sorted(files):
            rel = f.relative_to(dados)
            print(f"    {rel}")

c.close()
