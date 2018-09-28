"""

This module contains simple functions for pandas library.

"""
import sys
import pandas as pd
from unidecode import unidecode
import multiprocessing
from os import listdir
from os.path import isfile, join


def format_columns_name(df_data):
    """
    Function to format a DataFrame columns name, removing special characters, making lower case and replace whitespaces for _
 
    Parameters
 |  ----------
        df_data : pandas DataFrame
            The DataFrame object to normalize columns

    Returns
    ----------
        df_data: pandas DataFrame
            The DataFrame object with normalized columns names
    """

    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    df_data = df_data.copy()
    df_data.columns = [unidecode(str(x)).lower().strip().replace(' ', '_') for x in df_data.columns]

    return df_data


def print_value_counts(df_data, field, msg='{} : {} ({:.2f}%)', limit=None):
    """
    Print the result of a value counts of a DataFrame on a format message
    
    Parameters
 |  ----------
        df_data (DataFrmae): Original data to print values
        field (String): the label of
        msg (String): the message to print in format {key} {count} {percentage}
        limit (int): Max number of unique values to print (if is too long)

    """
    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    if not limit:
        limit = df_data[label].unique().size

    df_data[label].value_counts().reset_index(name='count').assign(
        percentage=lambda x: 100 * x['count'] / x['count'].sum()).assign(total=lambda x: x['count'].sum()).head(
        limit).apply(lambda x: print(msg.format(x['index'], x['count'], x['percentage'])), axis=1)


def get_field_from_df(value, value_field, return_field, df_data, return_first_value=True, null_return=None):
    """
    Function to return a value from a DataFrame with a filter. Used on apply functions

    Parameters
    ----------
    value : Object
        Data value to search on DataFrame
    value_field : String
        DataFrame field to compare with value 
    return_field : String
        DataFrame column to return
    df_data : pandas DataFrame
        DataFrame to search value
    return_first_value : boolean (default True):
        If return the first value from DataFrame
    null_return : Object (defaut None)
        Value to return if did not found value on DataFrame
    
    Returns
    -------
    data : Object
        The DataFrame value of return_field or null_return

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
    Calls an apply function on a DataFrame with a set of parameters

    Parameters
    ----------
    args: dict
        Parameters for functon

    Returns
    -------
    data.apply : Function 
        The result of the apply function

    """
    
    df_data, function, kwargs = args
    return df_data.apply(function, **kwargs)


def multiprocessing_apply(df_data, function, **kwargs):
    """
    Pandas apply function using multiprocessors

    Parameters
    ----------
    df_data : pandas DataFrame
        DataFrame for the apply function
    function : Function
        Function to apply on DataFrame 
    **kwargs : dict
        Function parameters 

    Returns
    -------
    res : list
        The result of the apply function

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
                             the print mensage            for df in df_list])
        pool.close()
        return pd.concat(list(res))
    except Exception as e:
        if verbose:
            print('Error: {}'.formart(str(e)))
        pool.terminate()


def join_dataframe_from_folder(folder_path, set_file=True, subfolders=True, format_columns=True):
    """
    Join serveral DataFrames from folder. Can join DataFrames of subfolders too

    Parameters
    ----------
    folder_path : String
        Path of initial Folder 
    set_file : bool (default True) 
        If should add a columns to identify file name from DataFrame
    subfolders : bool (default True)
        If should join files from subfolders
    format_columns : bool (default True)
        Format columns from final DataFrame
    
    Returns
    -------
    df_return : DataFrame
        A DataFrame from all files in folder

    """


    df_return = pd.DataFrame()

    try:
        for item in listdir(folder_path):
            if isfile(join(folder_path, item)):
                if set_file:
                    df_return = df_return.append(_infer_dataframe_filetype(join(folder_path, item)).assign(file=item), sort=False)
                else:
                    df_return = df_return.append(_infer_dataframe_filetype(join(folder_path, item)), sort=False)
            elif subfolders:
                df_return = df_return.append(join_dataframe_from_folder(join(folder_path, item), subfolders, format_columns), sort=False)

    except Exception as e:
        raise(e)

    if format_columns:    
        return format_columns_name(df_return)

    return df_return

def _infer_dataframe_filetype(path, type=None, encoding=None, sep={',': 0, ';': 0, '\t': 0}):
    """
    Infer DataFrame filetype using file extension

    Parameters
    ----------
    path : String
        Path of file 
    type : String (default None)
        Type to read file (None to infer file) 
    encoding : String (default None)
        File encoding
    sep : dict (default {': ':0: ';':0: '\t':0}) 
        Dict of characters to try to split csv files

    Returns
    -------
    dataframe : pandas DataFrame
        A DataFrame object with path data

    """
    
    if not type:

        if '.xls' in path:
            type = 'excel'
        else:
            type = 'csv'

    if type == 'excel':
        try:
            return pd.read_excel(path, encoding='latin')
        except Exception as e_latin:
            try:
                return pd.read_excel(path, encoding='utf8')
            except Exception as e_utf:
                raise(e_latin)
                raise(e_utf)
    elif type == 'csv':
        with open(path) as file:
            first_line = file.readline()

        for key in sep:
            sep[key] = len(first_line.split(key))

        sep = max(sep, key=(lambda key: sep[key]))

        try:
            return pd.read_csv(path, encoding='latin', sep=sep)

        except Exception as e_latin:
            try:
                return pd.read_csv(path, encoding='utf8', sep=sep)
            except Exception as e_utf:
                raise(e_latin)
                raise(e_utf)

    return pd.DataFrame()


def split_unique(df_data, field):
    """
    Split the DataFrame based on unique value from field

    Parameters
    ----------
    df_data : pandas DataFrame
        Original data to split 
    field : String
        Column name to split data on unique value

    Returns
    -------
    df_split : dict
        A dic of DataFrames with field values as keys
    """
    
    df_split = {}
    for f in df_data[field].unique():
        df_split[f] = df_data[df_data[field]==f]
        
    return df_split


def main(argv=sys.argv):
 
    print(argv)
    return 0
