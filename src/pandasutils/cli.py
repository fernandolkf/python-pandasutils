"""

This module contains simple functions for pandas library.  

"""
import sys
import pandas as pd
from unidecode import unidecode
import multiprocessing


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
    df_data.columns = [unidecode(str(x)).lower().strip().replace(' ', '_') for x in df_data.columns]

    return df_data


def print_value_counts(df_data, label, msg='{} : {} ({:.2f})', limit=None):
    """
    Function to print the result of a value counts of a DataFrame
    Args:
        df_data: a DataFrame object 
        label: the label of 
        msg: the print mensage
        limit: limit the unique values

    """
    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    if not limit:
        limit = df_data[label].unique().size

    df_data[label].value_counts().reset_index(name='count').assign(
        percentage=lambda x: 100 * x['count'] / x['count'].sum()).assign(total=lambda x: x['count'].sum()).head(
        limit).apply(lambda x: print(msg.format(x['index'], x['count'], x['percentage'])), axis=1)


def get_field_from_df(data, label, field, df_get_data, return_first_value=True, null_return=None):
    """
    Function to print the result of a value counts of a DataFrame
    Args:
        data: data to search on DataFrame
        label: the x label to compare
        field: the field to return
        df_get_data: dataframe to get value
        return_first_value: boolean to return the first value or all values from DataFrame
        null_return: Value to return if there is no value

    Returns:
    the data from DataFrame

    """
    try:
        if return_first_value:
            return df_get_data.loc[df_get_data[label] == data, field].values[0]

        else:
            return df_get_data.loc[df_get_data[label] == data, field].values

    except Exception as e:
        return null_return


def _apply_function(args):
    """
    Apply fuction
    Args:
        args: Dictionary of df_data, function and function parameters
        
    Returns:
    DataFrame apply

    """
    df_data, function, kwargs = args
    return df_data.apply(function, **kwargs)


def multiprocessing_apply(df_data, function, **kwargs):
    """
    Function to use multiprocessors on pandas apply function
    Args:
        df_data: DataFrame for apply
        function: function to apply on DataFrame
        kwargs: function parameters
        
    Returns:
    the result of the apply function

    """
    try:
        num_cores = kwargs.pop('num_cores')
    except Exception as e:
        num_cores = 2

    try:
        verbose = kwargs.pop('verbose')
    except Exception:
        verbose = False

    if verbose:
        print('Creating multiprocessing with {} cores'.format(num_cores))
    pool = multiprocessing.Pool(processes=num_cores)
    df_list = []
    step = int(df_data.shape[0] / num_cores)
    for i in range(0, num_cores):
        if i == num_cores - 1:
            df_append = df_data[i * step:]
        else:
            df_append = df_data[i * step:(1 + i) * step]
        if df_append.shape[0] > 0:
            df_list.append(df_append)
    if verbose:
        print('Mapping process')
    try:
        res = pool.map(_apply_function, [(df, function, kwargs)
                                         for df in df_list])
        pool.close()
        return pd.concat(list(res))
    except Exception as e:
        if verbose:
            print('Error: {}'.formart(str(e)))
        pool.terminate()


def main(argv=sys.argv):
    """
    Args:
        argv (list): List of arguments

    Returns:
        int: A return code


    """
    print(argv)
    return 0
