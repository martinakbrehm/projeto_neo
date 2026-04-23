"""
Subagente especialista: Verificação de consistência do Dashboard vs Banco de Dados.

Verifica:
  1. Existência das 3 tabelas materializadas (dashboard_*_agg)
  2. Existência das 3 stored procedures (sp_refresh_dashboard_*_agg)
  3. Tabelas não estão vazias
  4. Colunas das tabelas agg batem com o esperado
  5. Dados das tabelas agg são consistentes com as tabelas-fonte
  6. SPs executam sem erro (dry-run opcional)
  7. Índices críticos existem

Uso:
    python subagentes/verificar_dashboard.py              # verificação completa (sem executar SPs)
    python subagentes/verificar_dashboard.py --run-sps    # também executa as SPs
    python subagentes/verificar_dashboard.py --json       # saída em JSON
"""

import sys
import os
import json
import argparse
import time
from pathlib import Path
from datetime import datetime

# Ajustar path para importar config do projeto
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import db_destino  # noqa: E402

import pymysql

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
DB_CONFIG = db_destino()

# Esquemas esperados das tabelas materializadas
EXPECTED_TABLES = {
    "dashboard_macros_agg": {
        "columns": [
            "dia", "status", "mensagem", "resposta_status",
            "empresa", "fornecedor", "arquivo_origem", "qtd",
        ],
        "sp": "sp_refresh_dashboard_macros_agg",
    },
    "dashboard_arquivos_agg": {
        "columns": [
            "arquivo", "data_carga", "cpfs_no_arquivo", "cpfs_processados",
            "ativos", "inativos", "cpfs_ineditos", "ucs_ineditas",
            "ineditos_processados", "ineditos_ativos", "ineditos_inativos",
        ],
        "sp": "sp_refresh_dashboard_arquivos_agg",
    },
    "dashboard_cobertura_agg": {
        "columns": [
            "arquivo", "data_carga", "total_combos", "combos_novas",
            "combos_existentes",
        ],
        "sp": "sp_refresh_dashboard_cobertura_agg",
    },
}

# Índices críticos para performance dos SPs
CRITICAL_INDEXES = {
    "staging_import_rows": ["idx_sir_valid_cpf_uc_stg"],
    "tabela_macros": ["idx_tm_sp_arquivos"],
}

# Tabelas-fonte que devem ter dados
SOURCE_TABLES = [
    "staging_imports",
    "staging_import_rows",
    "tabela_macros",
    "clientes",
    "cliente_uc",
    "distribuidoras",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class CheckResult:
    """Resultado de uma verificação individual."""

    def __init__(self, name: str, passed: bool, detail: str = ""):
        self.name = name
        self.passed = passed
        self.detail = detail

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.detail}"

    def to_dict(self):
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
        }


def get_connection():
    """Abre conexão com o banco de destino."""
    return pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=120)


def query_one(cur, sql):
    """Executa query e retorna primeira linha."""
    cur.execute(sql)
    return cur.fetchone()


def query_all(cur, sql):
    """Executa query e retorna todas as linhas."""
    cur.execute(sql)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
def check_table_exists(cur, table_name: str) -> CheckResult:
    """Verifica se uma tabela existe no banco."""
    row = query_one(cur, f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = '{table_name}'
    """)
    exists = row[0] > 0
    return CheckResult(
        f"Tabela {table_name} existe",
        exists,
        "encontrada" if exists else "NÃO ENCONTRADA",
    )


def check_sp_exists(cur, sp_name: str) -> CheckResult:
    """Verifica se uma stored procedure existe."""
    row = query_one(cur, f"""
        SELECT COUNT(*) FROM information_schema.routines
        WHERE routine_schema = DATABASE()
          AND routine_name = '{sp_name}'
          AND routine_type = 'PROCEDURE'
    """)
    exists = row[0] > 0
    return CheckResult(
        f"SP {sp_name} existe",
        exists,
        "encontrada" if exists else "NÃO ENCONTRADA",
    )


def check_table_not_empty(cur, table_name: str) -> CheckResult:
    """Verifica se uma tabela tem pelo menos 1 linha."""
    row = query_one(cur, f"SELECT COUNT(*) FROM `{table_name}`")
    count = row[0]
    return CheckResult(
        f"Tabela {table_name} não vazia",
        count > 0,
        f"{count:,} linhas",
    )


def check_columns(cur, table_name: str, expected_cols: list) -> CheckResult:
    """Verifica se as colunas esperadas existem na tabela."""
    rows = query_all(cur, f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = '{table_name}'
    """)
    actual_cols = {r[0].lower() for r in rows}
    missing = [c for c in expected_cols if c.lower() not in actual_cols]
    if missing:
        return CheckResult(
            f"Colunas de {table_name}",
            False,
            f"faltando: {', '.join(missing)}",
        )
    return CheckResult(
        f"Colunas de {table_name}",
        True,
        f"todas {len(expected_cols)} colunas presentes",
    )


def check_index_exists(cur, table_name: str, index_name: str) -> CheckResult:
    """Verifica se um índice existe na tabela."""
    row = query_one(cur, f"""
        SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = '{table_name}'
          AND index_name = '{index_name}'
    """)
    exists = row[0] > 0
    return CheckResult(
        f"Índice {index_name} em {table_name}",
        exists,
        "encontrado" if exists else "NÃO ENCONTRADO",
    )


def check_macros_agg_consistency(cur) -> CheckResult:
    """Verifica se dashboard_macros_agg reflete dados de tabela_macros.

    Compara total geral: SUM(qtd) na agg vs COUNT(*) de registros
    com resposta_id NOT NULL em tabela_macros.
    """
    agg = query_one(cur, "SELECT COALESCE(SUM(qtd), 0) FROM dashboard_macros_agg")
    src = query_one(cur, """
        SELECT COUNT(*) FROM tabela_macros
        WHERE status != 'pendente' AND resposta_id IS NOT NULL
    """)
    agg_total = agg[0]
    src_total = src[0]
    # Permitir margem de 5% pois a agg agrupa por múltiplas dimensões
    if src_total == 0:
        passed = agg_total == 0
    else:
        ratio = agg_total / src_total
        passed = 0.95 <= ratio <= 1.05
    return CheckResult(
        "Consistência dashboard_macros_agg",
        passed,
        f"agg={agg_total:,} vs fonte={src_total:,} (ratio={agg_total/max(src_total,1):.4f})",
    )


def check_arquivos_agg_consistency(cur) -> CheckResult:
    """Verifica se dashboard_arquivos_agg tem 1 linha por arquivo importado."""
    agg = query_one(cur, "SELECT COUNT(*) FROM dashboard_arquivos_agg")
    src = query_one(cur, "SELECT COUNT(DISTINCT filename) FROM staging_imports")
    agg_count = agg[0]
    src_count = src[0]
    # Pode haver arquivos sem linhas valid → agg pode ter menos
    passed = agg_count > 0 and agg_count <= src_count
    return CheckResult(
        "Consistência dashboard_arquivos_agg",
        passed,
        f"agg={agg_count} arquivos vs fonte={src_count} imports distintos",
    )


def check_cobertura_agg_consistency(cur) -> CheckResult:
    """Verifica se dashboard_cobertura_agg tem dados por arquivo."""
    agg = query_one(cur, "SELECT COUNT(*) FROM dashboard_cobertura_agg")
    # Cobertura deve ter ao menos 1 arquivo se existirem dados no staging
    src = query_one(cur, """
        SELECT COUNT(DISTINCT si.filename)
        FROM staging_imports si
        JOIN staging_import_rows sir ON sir.staging_id = si.id
            AND sir.validation_status = 'valid'
            AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
    """)
    agg_count = agg[0]
    src_count = src[0]
    passed = agg_count > 0 and agg_count <= src_count
    return CheckResult(
        "Consistência dashboard_cobertura_agg",
        passed,
        f"agg={agg_count} arquivos vs fonte={src_count} arquivos com UCs",
    )


def check_source_tables(cur) -> list[CheckResult]:
    """Verifica se as tabelas-fonte têm dados."""
    results = []
    for table in SOURCE_TABLES:
        results.append(check_table_not_empty(cur, table))
    return results


def check_sp_execution(cur, sp_name: str) -> CheckResult:
    """Executa uma stored procedure e verifica se roda sem erro."""
    t0 = time.time()
    try:
        cur.execute(f"CALL {sp_name}()")
        elapsed = time.time() - t0
        return CheckResult(
            f"Execução SP {sp_name}",
            True,
            f"OK em {elapsed:.1f}s",
        )
    except Exception as e:
        elapsed = time.time() - t0
        return CheckResult(
            f"Execução SP {sp_name}",
            False,
            f"ERRO em {elapsed:.1f}s: {e}",
        )


def check_stale_data(cur) -> CheckResult:
    """Verifica se os dados da agg não estão muito desatualizados.

    Compara o MAX(data_carga) da dashboard_arquivos_agg com a data do último
    staging_import. Se differe em mais de 2 dias, pode indicar refresh parado.
    """
    agg = query_one(cur, "SELECT MAX(data_carga) FROM dashboard_arquivos_agg")
    src = query_one(cur, "SELECT MAX(DATE(created_at)) FROM staging_imports")
    if agg[0] is None or src[0] is None:
        return CheckResult(
            "Dados atualizados (arquivos_agg)",
            False,
            "sem dados para comparar",
        )
    diff = (src[0] - agg[0]).days
    passed = diff <= 2
    return CheckResult(
        "Dados atualizados (arquivos_agg)",
        passed,
        f"última agg={agg[0]}, último import={src[0]}, diff={diff} dias",
    )


def check_zombie_processes(cur) -> CheckResult:
    """Verifica se há queries rodando há mais de 10 minutos."""
    rows = query_all(cur, """
        SELECT id, TIME, STATE, LEFT(INFO, 80) AS query_preview
        FROM information_schema.processlist
        WHERE COMMAND != 'Sleep'
          AND TIME > 600
          AND DB = DATABASE()
          AND INFO IS NOT NULL
    """)
    if rows:
        details = "; ".join(f"PID={r[0]} time={r[1]}s state={r[2]}" for r in rows)
        return CheckResult(
            "Sem queries zombie (>10min)",
            False,
            f"{len(rows)} processos: {details}",
        )
    return CheckResult(
        "Sem queries zombie (>10min)",
        True,
        "nenhum processo longo encontrado",
    )


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def run_all_checks(run_sps: bool = False) -> list[CheckResult]:
    """Executa todas as verificações e retorna lista de resultados."""
    results = []
    conn = get_connection()
    cur = conn.cursor()

    # 1. Tabelas materializadas existem
    for table_name, spec in EXPECTED_TABLES.items():
        results.append(check_table_exists(cur, table_name))

    # 2. Stored procedures existem
    for table_name, spec in EXPECTED_TABLES.items():
        results.append(check_sp_exists(cur, spec["sp"]))

    # 3. Tabelas não vazias
    for table_name in EXPECTED_TABLES:
        results.append(check_table_not_empty(cur, table_name))

    # 4. Colunas corretas
    for table_name, spec in EXPECTED_TABLES.items():
        results.append(check_columns(cur, table_name, spec["columns"]))

    # 5. Índices críticos
    for table_name, indexes in CRITICAL_INDEXES.items():
        for idx_name in indexes:
            results.append(check_index_exists(cur, table_name, idx_name))

    # 6. Tabelas-fonte com dados
    results.extend(check_source_tables(cur))

    # 7. Consistência dos dados
    results.append(check_macros_agg_consistency(cur))
    results.append(check_arquivos_agg_consistency(cur))
    results.append(check_cobertura_agg_consistency(cur))

    # 8. Dados não estão stale
    results.append(check_stale_data(cur))

    # 9. Sem zombie processes
    results.append(check_zombie_processes(cur))

    # 10. Execução das SPs (opcional)
    if run_sps:
        for table_name, spec in EXPECTED_TABLES.items():
            results.append(check_sp_execution(cur, spec["sp"]))

    conn.close()
    return results


def print_report(results: list[CheckResult], as_json: bool = False):
    """Imprime relatório das verificações."""
    if as_json:
        output = {
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "checks": [r.to_dict() for r in results],
        }
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
        return

    print("=" * 70)
    print(f"  VERIFICAÇÃO DE CONSISTÊNCIA DO DASHBOARD")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    for r in results:
        icon = "✓" if r.passed else "✗"
        print(f"  {icon} {r}")

    print()
    print("-" * 70)
    print(f"  RESULTADO: {passed}/{len(results)} checks passaram", end="")
    if failed > 0:
        print(f"  |  {failed} FALHA(S)")
    else:
        print("  |  TUDO OK")
    print("-" * 70)


def save_report(results: list[CheckResult]):
    """Salva relatório em arquivo texto na pasta relatorios/."""
    report_dir = Path(__file__).parent / "relatorios"
    report_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = report_dir / f"verificacao_dashboard_{ts}.txt"

    lines = []
    lines.append("=" * 70)
    lines.append("  VERIFICAÇÃO DE CONSISTÊNCIA DO DASHBOARD")
    lines.append(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    for r in results:
        icon = "PASS" if r.passed else "FAIL"
        lines.append(f"  [{icon}] {r.name}: {r.detail}")

    lines.append("")
    lines.append("-" * 70)
    lines.append(f"  RESULTADO: {passed}/{len(results)} checks passaram  |  {failed} falha(s)")
    lines.append("-" * 70)

    filepath.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nRelatório salvo em: {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Subagente de verificação de consistência do Dashboard"
    )
    parser.add_argument(
        "--run-sps",
        action="store_true",
        help="Também executa as stored procedures para verificar se funcionam",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Saída em formato JSON",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Salva relatório em arquivo na pasta relatorios/",
    )
    args = parser.parse_args()

    results = run_all_checks(run_sps=args.run_sps)
    print_report(results, as_json=args.json)

    if args.save:
        save_report(results)

    # Exit code: 0 se tudo passou, 1 se houve falhas
    failed = sum(1 for r in results if not r.passed)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
