"""
auditar.py
==========
Script principal de auditoria da macro.

Roda todos os checks, imprime resultado no terminal
e salva relatorio em relatorios/auditoria_YYYYMMDD_HHMMSS.txt

Uso:
  python auditar.py              # auditoria completa
  python auditar.py --so-tela   # so imprime, nao salva arquivo
  python auditar.py --check volume   # roda apenas um check

Checks disponíveis: volume, status, qualidade, salvamento
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path

ROOT_PROJETO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_PROJETO))

try:
    from config import db_destino
except ImportError:
    print("[ERRO] config.py nao encontrado. Execute a partir da raiz do projeto ou ajuste o PYTHONPATH.")
    sys.exit(1)

import pymysql

from checks import volume as chk_volume
from checks import status as chk_status
from checks import qualidade as chk_qualidade
from checks import salvamento as chk_salvamento

RELATORIOS_DIR = Path(__file__).parent / "relatorios"
CHECKS_DISPONIVEIS = {
    "volume":     chk_volume,
    "status":     chk_status,
    "qualidade":  chk_qualidade,
    "salvamento": chk_salvamento,
}

SEP  = "=" * 72
SEP2 = "-" * 72


def linha_titulo(texto: str) -> str:
    return f"\n{SEP}\n  {texto}\n{SEP2}"


def executar_auditoria(checks: list[str], so_tela: bool = False) -> int:
    """Executa os checks e retorna numero de alertas detectados."""
    agora = datetime.now()
    cabecalho = [
        SEP,
        f"  AUDITORIA DA MACRO  —  {agora.strftime('%d/%m/%Y %H:%M:%S')}",
        SEP2,
        f"  Projeto: {ROOT_PROJETO}",
        f"  Checks:  {', '.join(checks)}",
        SEP,
    ]

    todas_linhas: list[str] = list(cabecalho)
    total_alertas = 0

    try:
        conn = pymysql.connect(**db_destino(autocommit=True))
        cur = conn.cursor()
    except Exception as e:
        print(f"[ERRO] Nao foi possivel conectar ao banco: {e}")
        return 1

    try:
        for nome_check in checks:
            modulo = CHECKS_DISPONIVEIS[nome_check]
            todas_linhas.append(linha_titulo(f"CHECK: {nome_check.upper()}"))
            try:
                dados = modulo.rodar(cur)
                linhas_fmt = modulo.formatar(dados)
                todas_linhas.extend(linhas_fmt)
                # Conta alertas (qualquer linha com [ATENCAO, independente do sufixo)
                alertas_check = sum(1 for l in linhas_fmt if "[ATENCAO" in l)
                if alertas_check:
                    total_alertas += alertas_check
                    todas_linhas.append(f"\n  >>> {alertas_check} alerta(s) neste check <<<")
            except Exception as e:
                todas_linhas.append(f"  [ERRO no check '{nome_check}']: {e}")
                import traceback
                todas_linhas.append("  " + traceback.format_exc().replace("\n", "\n  "))
                total_alertas += 1

        # Rodape
        todas_linhas.append(f"\n{SEP}")
        if total_alertas == 0:
            todas_linhas.append("  RESULTADO: TUDO OK — nenhum alerta detectado.")
        else:
            todas_linhas.append(f"  RESULTADO: {total_alertas} ALERTA(S) DETECTADO(S) — revisar itens marcados com [ATENCAO]")
        todas_linhas.append(SEP)

    finally:
        cur.close()
        conn.close()

    # Imprime no terminal
    output = "\n".join(todas_linhas)
    print(output)

    # Salva relatorio
    if not so_tela:
        RELATORIOS_DIR.mkdir(exist_ok=True)
        nome_arquivo = f"auditoria_{agora.strftime('%Y%m%d_%H%M%S')}.txt"
        caminho = RELATORIOS_DIR / nome_arquivo
        caminho.write_text(output, encoding="utf-8")
        print(f"\n  Relatorio salvo em: {caminho}")

    return total_alertas


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria da macro: verifica salvamento, volume e qualidade dos dados."
    )
    parser.add_argument(
        "--check",
        choices=list(CHECKS_DISPONIVEIS.keys()),
        help="Roda apenas um check especifico",
    )
    parser.add_argument(
        "--so-tela",
        action="store_true",
        help="Nao salva o relatorio em arquivo",
    )
    args = parser.parse_args()

    checks = [args.check] if args.check else list(CHECKS_DISPONIVEIS.keys())
    alertas = executar_auditoria(checks, so_tela=args.so_tela)
    sys.exit(0 if alertas == 0 else 2)


if __name__ == "__main__":
    main()
