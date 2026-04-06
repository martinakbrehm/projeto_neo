import re
import pandas as pd


def detect_request_error(df_input, col_msg=None):
    """Detecta erros de requisição em um DataFrame, retornando Series booleana."""
    if df_input is None or df_input.empty:
        return pd.Series([False] * 0, index=df_input.index if df_input is not None else [])

    idx = df_input.index
    base = pd.Series(False, index=idx)

    # coluna estruturada 'Error'
    if 'Error' in df_input.columns:
        try:
            err_col = df_input['Error'].astype(str).fillna('').str.strip().str.upper()
            is_err_from_error_col = ~err_col.isin({'FALSO', 'FALSE', '0', ''})
            base = base | is_err_from_error_col
        except Exception:
            pass

    keywords = [
        r'TIMEOUT', r'LIMIT_EXCEEDED', r'ERRO', r'ERROR', r'CONNECTION', r'CONNREFUSED',
        r'REFUSED', r'RESET', r'SOCKET', r'PEAK CONNECTIONS LIMIT', r'EXCEEDED', r'UNAVAILABLE',
        r'UNREACHABLE', r'NAME OR SERVICE NOT KNOWN', r'EOF', r'502', r'503', r'504', r'500'
    ]
    kw_pattern = re.compile(r"(" + r"|".join(keywords) + r")", flags=re.IGNORECASE)

    text_cols = []
    if col_msg:
        if col_msg in df_input.columns:
            text_cols.append(col_msg)
    for c in ['Msg', 'mensagem', 'resposta', 'resposta_lista', 'Status']:
        if c in df_input.columns and c not in text_cols:
            text_cols.append(c)

    if text_cols:
        try:
            combined = df_input[text_cols].fillna('').astype(str).agg(' '.join, axis=1)
            found = combined.str.contains(kw_pattern)
            found_codes = combined.str.contains(r'\b5\d{2}\b', case=False, na=False)
            base = base | found | found_codes
        except Exception:
            pass

    return base


def sentence_case(s):
    try:
        s2 = str(s).strip()
        if not s2:
            return s2
        return s2[0].upper() + s2[1:].lower()
    except Exception:
        return str(s)


def pick_message_column(df):
    for c in ['Msg', 'Status', 'Error', 'erro', 'mensagem', 'resposta', 'resposta_lista']:
        if c in df.columns:
            return c
    return None


def aggregate_messages(df, col_msg):
    """Retorna lista de dicts {'mensagem':..., 'quantidade':...} ordenada por quantidade desc."""
    if col_msg is None or df is None or df.empty:
        return []
    try:
        series = df[col_msg].dropna().astype(str).str.strip()
        exclude_upper = {'', 'N/A', 'ERRO: DESTINATION NAME IS NULL'}
        mask = ~series.str.upper().isin(exclude_upper)
        filtered = series[mask]
        cnt = filtered.value_counts().reset_index()
        cnt.columns = ['mensagem', 'quantidade']
        cnt['mensagem'] = cnt['mensagem'].apply(sentence_case)
        cnt = cnt.sort_values('quantidade', ascending=False)
        return cnt.to_dict('records')
    except Exception:
        return []
