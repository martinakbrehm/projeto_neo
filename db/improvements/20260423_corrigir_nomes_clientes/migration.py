"""
Migration: 20260423_corrigir_nomes_clientes
Corrige nomes problemáticos na tabela clientes:
  1. Nomes com encoding corrompido → corrigidos manualmente
  2. Nomes = distribuidora (cosern, coelba) → vazio
  3. Nomes = 'nan' → vazio
  4. Nomes = apenas números → vazio
  5. Nomes NULL → vazio
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import config
import pymysql

SEP = "=" * 70

# ---------------------------------------------------------------------------
# Mapeamento manual de nomes corrompidos → corretos
# ---------------------------------------------------------------------------
NOME_CORRETO = {
    'ADRIANA GONÂ¦ALVES VALENCIO':               'ADRIANA GONÇALVES VALENCIO',
    'ANT+NIO FRANCISCO DA SILVA COSTA':           'ANTÔNIO FRANCISCO DA SILVA COSTA',
    'ANTONIO EDMUNDO JORD+O DE VASCONCELO':       'ANTONIO EDMUNDO JORDÃO DE VASCONCELO',
    'ARL+CIO FERREIRA DE SOUZA':                  'ARLÉCIO FERREIRA DE SOUZA',
    'AURECI GON¦ALVES DE ANDRADE':                'AURECI GONÇALVES DE ANDRADE',
    'BELUCIO ROCHA CALAÂ¦A':                      'BELUCIO ROCHA CALANÇA',
    'CA?NTIA PAIVA E SILVA CAVALCANTE':           'CÂNTIA PAIVA E SILVA CAVALCANTE',
    'CACILDA MARIA SOARES BRAND+O':               'CACILDA MARIA SOARES BRANDÃO',
    'CLAUDIA FERNANDA GON¦ALVES DOS SANTOS':      'CLAUDIA FERNANDA GONÇALVES DOS SANTOS',
    'CLAUDIO JOS+ DA SILVA':                      'CLAUDIO JOSÉ DA SILVA',
    'CLEBER SOARES NERY DE MENDONÂ¦A':            'CLEBER SOARES NERY DE MENDONÇA',
    'CLEONICE GONÂ¦ALVES DO NASCIMENTO':          'CLEONICE GONÇALVES DO NASCIMENTO',
    'DINARA GUIMAR?ES DA SILVA':                   'DINARA GUIMARÃES DA SILVA',
    'DIRCY MARIA RODRIGUES DE FRAN¦A':            'DIRCY MARIA RODRIGUES DE FRANÇA',
    'DOMINGO S++VIO COELHO':                      'DOMINGO SÁVIO COELHO',
    'EDJANE MOTA GONÂ¦ALVES':                     'EDJANE MOTA GONÇALVES',
    'ELIZABETE DE LIMA FRAN¦A SILVA':             'ELIZABETE DE LIMA FRANÇA SILVA',
    'ENILSON SIQUEIRA MONTALV+O':                 'ENILSON SIQUEIRA MONTALVÃO',
    'EURIDICE MARIA GONÂ¦ALVES DO COUTO':         'EURIDICE MARIA GONÇALVES DO COUTO',
    'FÂ¦BIO DE ALMEIDA FERREIRA LIMA':            'FÁBIO DE ALMEIDA FERREIRA LIMA',
    'FERNANDA MASSUMI GONÂ¦ALVES DE CAMPOS':      'FERNANDA MASSUMI GONÇALVES DE CAMPOS',
    'FL VIO JOS+ FREIRE ALVES MOREIRA':           'FLÁVIO JOSÉ FREIRE ALVES MOREIRA',
    'FRANCISCA ASSIS DE ARA¦JO':                  'FRANCISCA ASSIS DE ARAÚJO',
    'GENIVAL GON¦ALVES DE LIMA':                  'GENIVAL GONÇALVES DE LIMA',
    'GERSON GUSMAO GON¦ALVES FILHO':              'GERSON GUSMAO GONÇALVES FILHO',
    'GILVANIA LINS DE FRAN¦A':                    'GILVANIA LINS DE FRANÇA',
    'HILSON GUIMAR+ES DA SILVA':                  'HILSON GUIMARÃES DA SILVA',
    'INALDO FERREIRA BRAND+O':                    'INALDO FERREIRA BRANDÃO',
    'JARISMAR JAQUES GON¦ALVES':                  'JARISMAR JAQUES GONÇALVES',
    'JO?O BATISTA DA SILVA':                      'JOÃO BATISTA DA SILVA',
    'JO?O LOUREN?O FILHO':                        'JOÃO LOURENÇO FILHO',
    'JO+O PEREIRA DE ANDRADE':                    'JOÃO PEREIRA DE ANDRADE',
    'JO+O RAMOS DA SILVA':                        'JOÃO RAMOS DA SILVA',
    'JO¦O SEVERINO HERM NIO':                     'JOÃO SEVERINO HERMÍNIO',
    'JORGE LUIZ DE ARAUJO GALVÂ¦O':               'JORGE LUIZ DE ARAUJO GALVÃO',
    'JOSE ULISSES DE S  MAGALH+ES':               'JOSE ULISSES DE SÁ MAGALHÃES',
    'JURANNY MARIA FRANÂ¦A DA SILVA':              'JURANNY MARIA FRANÇA DA SILVA',
    'KL+CIA VIRGINIA RODRIGUES E SILVA':           'KLÉCIA VIRGINIA RODRIGUES E SILVA',
    'LAURA MARIA DA CONCEIÂ¦AO':                   'LAURA MARIA DA CONCEIÇÃO',
    'MANOEL JOS+ DA SILVA FILHO':                  'MANOEL JOSÉ DA SILVA FILHO',
    'MARIA CONCEI¦+O CARNEIRO DA CUNHA':           'MARIA CONCEIÇÃO CARNEIRO DA CUNHA',
    'MARIA DA CONCEIÂ¦+O DA FONSECA GOMES':        'MARIA DA CONCEIÇÃO DA FONSECA GOMES',
    'MARIA DA CONCEIÂ¦AO AIRES SANTOS':            'MARIA DA CONCEIÇÃO AIRES SANTOS',
    'MARIA DA CONCEIÂ¦AO DE ASSIS':                'MARIA DA CONCEIÇÃO DE ASSIS',
    'MARIA DA CONCEIC?O LISBOA QUEIROZ':           'MARIA DA CONCEIÇÃO LISBOA QUEIROZ',
    'MARIA DAS GRA¦AS ARAUJO DA ROCHA':            'MARIA DAS GRAÇAS ARAUJO DA ROCHA',
    'MARIA DAS GRA¦AS DA SILVA FERREIRA':          'MARIA DAS GRAÇAS DA SILVA FERREIRA',
    'MARIA DAS GRAÂ¦AS DE A FRANÂ¦A':              'MARIA DAS GRAÇAS DE A FRANÇA',
    'MARIA DAS GRAÂ¦AS PIO GONÂ¦ALVES':            'MARIA DAS GRAÇAS PIO GONÇALVES',
    'MARIA DAS GRAÂ¦AS SANTOS SOARES':             'MARIA DAS GRAÇAS SANTOS SOARES',
    'MARIA DAS MERCES SILVA LOUREN¦O':             'MARIA DAS MERCES SILVA LOURENÇO',
    'MARIA DE F¦TIMA GUEDES CAVALCANTI':           'MARIA DE FÁTIMA GUEDES CAVALCANTI',
    'MARIA JOS+ OLIVEIRA DA SILVA':                'MARIA JOSÉ OLIVEIRA DA SILVA',
    'MARIA MARLENE DA APRESENTAÂ¦AO':              'MARIA MARLENE DA APRESENTAÇÃO',
    'MARINEIDE BEZERRA COLAÂ¦O RAMOS':             'MARINEIDE BEZERRA COLAÇO RAMOS',
    'MAURICELIA DA CONCEI¦AO ALVES':               'MAURICELIA DA CONCEIÇÃO ALVES',
    'MAURICELIA DA CONCEI¦AO DA SILVA':            'MAURICELIA DA CONCEIÇÃO DA SILVA',
    'NELSON MENDONÂ¦A DE CARVALHO':                'NELSON MENDONÇA DE CARVALHO',
    'NGELO TIMOLE+O MARANH+O DIAS FILHOS':         'ÂNGELO TIMOLEÃO MARANHÃO DIAS FILHOS',
    'OSAMIR JOS?GON?LVES':                         'OSAMIR JOSÉ GONÇALVES',
    'PENTAGRAMA PROMOCOES E PRODUÂ¦OES LTDA':      'PENTAGRAMA PROMOCOES E PRODUÇÕES LTDA',
    'REGINALDO BRAZÂ¦O TEIXEIRA':                  'REGINALDO BRAZÃO TEIXEIRA',
    'RICARDO JOS+ GOMES TEIXEIRA':                 'RICARDO JOSÉ GOMES TEIXEIRA',
    'SEBASTI?O MARINHO DE BARROS FILHO':           'SEBASTIÃO MARINHO DE BARROS FILHO',
    'SILVANO GON¦ALO DE LYRA':                     'SILVANO GONÇALO DE LYRA',
    'VELÂ¦ZIA MARIA EUFRASIO DE AZEVEDO':          'VELÚZIA MARIA EUFRASIO DE AZEVEDO',
    'BEATRIZ LUBISCO GUAZZELLI *****':              'BEATRIZ LUBISCO GUAZZELLI',
    'NEILDE MARIA ARAGAO28121970':                  'NEILDE MARIA ARAGAO',
}


def run():
    conn = pymysql.connect(**config.db_destino())
    cur = conn.cursor()

    print(SEP)
    print("CORREÇÃO DE NOMES NA TABELA clientes")
    print(SEP)

    total_corrigidos = 0

    # -----------------------------------------------------------------------
    # PASSO 1: Corrigir nomes com encoding corrompido
    # -----------------------------------------------------------------------
    print("\n[1/4] Corrigindo nomes com encoding corrompido...")
    for nome_errado, nome_certo in NOME_CORRETO.items():
        cur.execute(
            "UPDATE clientes SET nome = %s WHERE nome = %s",
            (nome_certo, nome_errado)
        )
        n = cur.rowcount
        if n > 0:
            print(f"  {n:>3d}x  {nome_errado!r}  →  {nome_certo!r}")
            total_corrigidos += n
    conn.commit()
    print(f"  Subtotal encoding corrigido: {total_corrigidos}")

    # -----------------------------------------------------------------------
    # PASSO 2: Nomes = distribuidora → vazio
    # -----------------------------------------------------------------------
    print("\n[2/4] Limpando nomes que são nomes de distribuidoras...")
    cur.execute("""
        UPDATE clientes SET nome = ''
        WHERE LOWER(TRIM(nome)) IN ('cosern', 'coelba', 'celpe', 'brasilia')
    """)
    n_dist = cur.rowcount
    conn.commit()
    print(f"  Limpos: {n_dist}")

    # -----------------------------------------------------------------------
    # PASSO 3: Nomes = 'nan' ou apenas números → vazio
    # -----------------------------------------------------------------------
    print("\n[3/4] Limpando nomes 'nan' e apenas números...")
    cur.execute("""
        UPDATE clientes SET nome = ''
        WHERE LOWER(TRIM(nome)) = 'nan'
           OR nome REGEXP '^[0-9 ./-]+$'
    """)
    n_nan_num = cur.rowcount
    conn.commit()
    print(f"  Limpos: {n_nan_num}")

    # -----------------------------------------------------------------------
    # PASSO 4: Nomes NULL → vazio
    # -----------------------------------------------------------------------
    print("\n[4/4] Convertendo nomes NULL → vazio...")
    cur.execute("UPDATE clientes SET nome = '' WHERE nome IS NULL")
    n_null = cur.rowcount
    conn.commit()
    print(f"  Limpos: {n_null}")

    # -----------------------------------------------------------------------
    # Resumo
    # -----------------------------------------------------------------------
    print(f"\n{SEP}")
    print("RESUMO")
    print(SEP)
    print(f"  Encoding corrigido:   {total_corrigidos:>6d}")
    print(f"  Distribuidoras→'':    {n_dist:>6d}")
    print(f"  nan/números→'':       {n_nan_num:>6d}")
    print(f"  NULL→'':              {n_null:>6d}")
    total = total_corrigidos + n_dist + n_nan_num + n_null
    print(f"  TOTAL CORRIGIDOS:     {total:>6d}")

    # Verificação final
    cur.execute("""
        SELECT COUNT(*) FROM clientes
        WHERE nome REGEXP '[+?¦]'
           OR LOWER(TRIM(nome)) IN ('cosern', 'coelba', 'celpe', 'nan')
           OR nome REGEXP '^[0-9 ./-]+$'
           OR nome IS NULL
    """)
    restantes = cur.fetchone()[0]
    print(f"\n  Problemas restantes:  {restantes}")

    cur.close()
    conn.close()
    print(f"\n{SEP}")
    print("CONCLUIDO")
    print(SEP)


if __name__ == "__main__":
    run()
