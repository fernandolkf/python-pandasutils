"""
Entrypoint module, in case you use `python -mpandasutils`.


Why does this file exist, and why __main__? For more info, read:

- https://www.python.org/dev/peps/pep-0338/
- https://docs.python.org/2/using/cmdline.html#cmdoption-m
- https://docs.python.org/3/using/cmdline.html#cmdoption-m
"""
import sys

from pandasutils.cli import main, format_columns_name, print_value_counts

if __name__ == "__main__":
    sys.exit(main())
