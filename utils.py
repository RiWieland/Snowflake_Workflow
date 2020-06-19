
import re

def clear_comma(df, col):

    df[col] = df[col].apply(lambda x: re.sub(',', '.', str(x)))
    return df
