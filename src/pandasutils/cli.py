"""

This module contains simple functions for pandas library.

"""
import sys
import pandas as pd
from unidecode import unidecode
import multiprocessing
from os import listdir
from os.path import isfile, join
import numpy as np


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


def print_value_counts(df_data, field, msg='{index} : {count} ({percentage:.2f}%)', limit=None):
    """
    Print the result of a value counts of a DataFrame on a format message
    
    Parameters
 |  ----------
        df_data (DataFrmae): Original data to print values
        field (String): the label of
        msg (String): the message to print in format {index} {count} {percentage}
        limit (int): Max number of unique values to print (if is too long)

    """
    if not isinstance(df_data, pd.DataFrame):
        raise TypeError('df_data should be instance of {}'.format(pd.DataFrame))

    if not limit:
        limit = df_data[field].unique().size

    df_data[field].value_counts().reset_index(name='count').assign(
        percentage=lambda x: 100 * x['count'] / x['count'].sum()).assign(total=lambda x: x['count'].sum()).head(
        limit).apply(lambda x: print(msg.format(index=x['index'], count=x['count'], percentage=x['percentage'])), axis=1)


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
        res = pool.map(_apply_function, [(df, function, kwargs) for df in df_list])
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

def _mem_usage(pandas_obj, as_string=True):
    """
    Check total amount of memory used by a pandas object
    Based on https://www.dataquest.io/blog/pandas-big-data/
    Parameters
    ----------
    pandas_obj : pandas Object
        A pandas object to check size
    as_string : boolean (default True):
        Return as a formated string or as the number of bytes of object 

    Returns
    -------
    usage :  String or int
        The amount of memory allocated by the object

    """
    
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    if as_string:
        return "{:03.2f} MB".format(usage_mb)
    
    return usage_b

def reduce_dataframe_size(df_data, infer_types=True, int_columns = None, float_columns=None, boolean_columns = None, 
                          categorical_columns = None, category_unique_percentage=0.5, category_null = True,
                          date_columns = None, date_format='%Y-%m-%d', verbose=True):
    
    """
    Reduce the size of a pandas DataFrame by changing the columns type format
    Based on https://www.dataquest.io/blog/pandas-big-data/

    Parameters
    ----------
    df_data : pandas DataFrame 
        The original DataFrame to reduce size
    infer_types : boolean (default True)
        Infer columns types
    int_columns : list (default None)
        List of int columns to reduce memory size
    float_columns : list (default None) 
        List of float columns to reduce memory size
    boolean_columns : list (default None)
        List of boolean columns to reduce memory size
    categorical_columns : list (default None)
        List of categorical columns to reduce memory size
    category_unique_percentage : float (default 0.5)
        Max percentage of unique values to set object type to category
    category_null : boolean (default True)
        Include null values to check for category total size
    date_columns : list (default None)
        List of date columns to reduce memory size
    date_format : String (defailt '%Y-%m-%d')
        Format of date values
    verbose : boolean (default True)
        Print status

    Returns
    -------
    df_reduced : pandas DataFrame
        A new DataFrame with reduced size
    """
    
    df_reduced = pd.DataFrame()
    if verbose:
        print('Initial DataFrame size: {}'.format(_mem_usage(df_data)))
        
    
    if infer_types:
        
        if int_columns is None:
            int_columns = list(df_data.select_dtypes(include=['int']).columns)
            
        if float_columns is None:
            float_columns = list(df_data.select_dtypes(include=['float']).columns)
    
    if int_columns:
        df_reduced[int_columns] = df_data[int_columns].apply(pd.to_numeric,downcast='unsigned')
    
    if float_columns:
        df_reduced[float_columns] = df_data[float_columns].apply(pd.to_numeric,downcast='float')
    
    if boolean_columns:
        boolean_map = {1:True, '1':True, 'True':True, '0':False, 0:False, 'False':False, np.nan:False, False:False, True:True}
        df_reduced[boolean_columns] = df_data[boolean_columns].fillna(False).apply(lambda x: x.map(boolean_map))
        
        for col in [c for c in df_reduced[boolean_columns].columns if df_reduced[c].dtype!=bool]:
            
            if verbose:
                print('Failed to format {col} to boolean column ({unique})'.format(col=col, unique=list(df_data[col].unique())))
                
            df_reduced[col] = df_data[col]
            
    if date_columns:
        df_reduced[date_columns] = df_data[date_columns].apply(lambda x: pd.to_datetime(x, format=date_format, errors='coerce'))
        
        
    if infer_types and categorical_columns is None:
        categorical_columns = [x for x in df_data.columns if df_data[x].dtype==object]
        
    for cat in categorical_columns:
        num_unique_values = df_data[cat].unique().size
        
        if category_null:
            num_total_values = df_data[cat].shape[0]
        else:
            num_total_values = df_data[cat].notnull().sum()
        
        if num_unique_values / num_total_values < category_unique_percentage:
            df_reduced[cat] = df_data[cat].astype('category')
        
    fault_columns = [x for x in df_data.columns if x not in df_reduced.columns]
    
    if fault_columns:
        df_reduced[fault_columns] = df_data[fault_columns]
            
    if verbose:
        mem_original = _mem_usage(df_data, as_string=False)
        mem_final = _mem_usage(df_reduced, as_string=False)
        print('Final DataFrame size: {} ({:.2f}% reduction)'.format(_mem_usage(df_reduced), 100*(1-(mem_final/mem_original))))
        
    return df_reduced 



def main(argv=sys.argv):
 
    print(argv)
    return 0
