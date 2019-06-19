from string import Template
from google.cloud import bigquery
import argparse

def file_to_string(sql_path):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('export-table')
    ap.add_argument('commitment-table')
    ap.add_argument('query')
    args = ap.parse_args()
    sql = file_to_string(args.query)
    fsql = sql.format(export_table=getattr(args,'export-table'),
                      commitment_table=getattr(args, 'commitment-table'))
    print(fsql)


if __name__ == '__main__':
    main()
