def normalize_columns(df):
    """
    Padroniza nomes de colunas para snake_case e lowercase.
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    return df
