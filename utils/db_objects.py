from typing import Tuple

import pandas as pd
from sqlalchemy.engine.base import Connection
from utils.schemas import (
    q_columns,
    q_extensions,
    q_fdw,
    q_functions,
    q_procedures,
    q_sequences,
    q_tables_list,
    q_triggers,
    q_usage_privileges,
    q_views
)
def get_db_tables(conn: Connection) -> Tuple[pd.DataFrame, list]:
    """Get list of tables in a database"""    
    table_list_df = pd.read_sql(q_tables_list, conn)
    schemas_list = table_list_df.table_schema.unique().tolist()
    return table_list_df, schemas_list

def get_source_row_counts(conn: Connection) -> pd.DataFrame:
    """get the TABLE ROW COUNTS of source DB"""    
    src_tables_row_count_queries_dict = {}
    src_table_rows_dict = {}
    src_table_list_df, src_schemas_list = get_db_tables(conn)
    for schema in src_schemas_list:
        src_tables_list = src_table_list_df.table_name.loc[
            src_table_list_df.table_schema == schema        ].values.tolist()
        for table in src_tables_list:
            src_row_query = f'select count(1) from "{schema}"."{table}";'            
            src_tables_row_count_queries_dict.update({table: src_row_query})
    for table, row_query in src_tables_row_count_queries_dict.items():
        src_query_result = conn.execute(row_query).fetchall()
        src_table_rows_dict[table] = [src_query_result[0][0], row_query]
    src_table_row_counts_df = pd.DataFrame.from_dict(
        data=src_table_rows_dict,
        orient="index",
        columns=["row_count", "query_executed"],
    )
    return src_table_row_counts_df

def get_target_row_counts(conn: Connection) -> pd.DataFrame:
    """get the TABLE ROW COUNTS of target DB"""    
    targ_tables_row_count_queries_dict = {}
    targ_table_rows_dict = {}
    targ_table_list_df, targ_schemas_list = get_db_tables(conn)
    for schema in targ_schemas_list:
        targ_tables_list = targ_table_list_df.table_name.loc[
            targ_table_list_df.table_schema == schema        ].values.tolist()
        for table in targ_tables_list:
            targ_row_query = f'select count(1) from "{schema}"."{table}";'            
            targ_tables_row_count_queries_dict.update({table: targ_row_query})
    for table, row_query in targ_tables_row_count_queries_dict.items():
        targ_query_result = conn.execute(row_query).fetchall()
        targ_table_rows_dict[table] = [targ_query_result[0][0], row_query]
    targ_table_row_counts_df = pd.DataFrame.from_dict(
        data=targ_table_rows_dict,
        orient="index",
        columns=["row_count", "query_executed"],
    )
    return targ_table_row_counts_df

def get_views(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the VIEWS of source and target DB"""    
    src_views_df = pd.read_sql(q_views, source_conn)
    targ_views_df = pd.read_sql(q_views, target_conn)
    return src_views_df, targ_views_df

def get_columns(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the COLUMNS of source and target DB"""    
    src_columns_df = pd.read_sql(q_columns, source_conn)
    targ_columns_df = pd.read_sql(q_columns, target_conn)
    return src_columns_df, targ_columns_df

def get_triggers(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the TRIGGERS of source and target DB"""    
    src_triggers_df = pd.read_sql(q_triggers, source_conn)
    targ_triggers_df = pd.read_sql(q_triggers, target_conn)
    return src_triggers_df, targ_triggers_df

def get_usage_privileges(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the USAGE PRIVILEGES of source and target DB"""    
    src_usage_privileges_df = pd.read_sql(q_usage_privileges, source_conn)
    targ_usage_privileges_df = pd.read_sql(q_usage_privileges, target_conn)
    return src_usage_privileges_df, targ_usage_privileges_df

def get_sequences(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the SEQUENCES of source and target DB"""    
    src_sequences_df = pd.read_sql(q_sequences, source_conn)
    targ_sequences_df = pd.read_sql(q_sequences, target_conn)
    return src_sequences_df, targ_sequences_df

def get_functions(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the FUNCTIONS of source and target DB"""    
    src_functions_df = pd.read_sql(q_functions, source_conn)
    targ_functions_df = pd.read_sql(q_functions, target_conn)
    return src_functions_df, targ_functions_df

def get_procedures(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the PROCEDURES of source and target DB"""    
    src_procedures_df = pd.read_sql(q_procedures, source_conn)
    targ_procedures_df = pd.read_sql(q_procedures, target_conn)
    return src_procedures_df, targ_procedures_df

def get_foreign_data_wrappers(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the FOREIGN DATA WRAPPERS of source and target DB"""    
    src_fdw_df = pd.read_sql(q_fdw, source_conn)
    targ_fdw_df = pd.read_sql(q_fdw, target_conn)
    return src_fdw_df, targ_fdw_df

def get_extensions(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """get the EXTENSIONS of source and target DB"""    
    src_extensions_df = pd.read_sql(q_extensions, source_conn)
    targ_extensions_df = pd.read_sql(q_extensions, target_conn)
    return src_extensions_df, targ_extensions_df