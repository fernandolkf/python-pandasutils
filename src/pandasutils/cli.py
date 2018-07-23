"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mpandasutils` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``pandasutils.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``pandasutils.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import sys
import pandas as pd
from unidecode import unidecode


def format_columns_name(df_data):
    """
    Function to format a DataFrame columns name, removing special characters, and making lower case
    Args:
        df_data: a DataFrame object

    Returns:
        df_data: the DataFrame with formated columns
    """

    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    df_data = df_data.copy()
    df_data.columns = [unidecode(x).lower().strip().replace(' ', '_') for x in df_data.columns]

    return df_data


def print_value_counts(df_data, label, msg='{} : {} ({:.2f})', limit=None):
    """
    Function to print the result of a value counts of a DataFrame
    Args:
        df_data: a DataFrame object
        label: the
        msg:
        limit:

    Returns:

    """
    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    if not limit:
        limit = df_data[label].unique().size

    df_data[label].value_counts().reset_index(name='count').assign(
        percentage=lambda x: 100 * x['count'] / x['count'].sum()).assign(total=lambda x: x['count'].sum()).head(
        limit).apply(lambda x: print(msg.format(x['index'], x['count'], x['percentage'])), axis=1)


def main(argv=sys.argv):
    """
    Args:
        argv (list): List of arguments

    Returns:
        int: A return code

    Does stuff.
    """
    print(argv)
    return 0
