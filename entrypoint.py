import os
from datetime import date
from time import perf_counter
from typing import Dict 
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from utils.objecs_comparison import(
    compare_row_counts,
    compare_views,
    compare_columns,
    compare_triggers,
    compare_usage_privileges,
    compare_sequences,
    compare_functions,
    compare_procedures,
    compare_foreign_data_wrappers,
    compare_extensions,
)
from utils.schemas import q_database_list
MIGRATION_SERVER = ""

def get_db_connection(database: str, env_var_prefix: str) -> Connection:
    """Connect to a postgres database and return a sqlalchemy connection object"""    
    pg_uri = f"postgresql://{os.environ.get(f'{env_var_prefix}_user')}:{os.environ.get(f'{env_var_prefix}_password')}@{os.environ.get(f'{env_var_prefix}_server')}:5432/{database}?sslmode=require"    engine = create_engine(pg_uri, pool_use_lifo=True, pool_recycle=300)
    conn = engine.connect()
    return conn

def get_comparison_connections(
    database: str, source_env_var_prefix: str, target_env_var_prefix: str):
    """Get connection objects for source and target DB to do comparison on"""    
    try:
        source_conn = get_db_connection(database, source_env_var_prefix)
        target_conn = get_db_connection(database, target_env_var_prefix)
        global MIGRATION_SERVER        
        MIGRATION_SERVER = os.environ.get(f"{source_env_var_prefix}_server")
        return source_conn, target_conn    
    except Exception as err:
        print(f"Connection error: {err}")

def generate_report(
    summary_df: pd.DataFrame, detail_compare_dict: Dict[str, pd.DataFrame]
):
    """Ouput the validation results to an excel file"""    
    file_name = f"{os.path.dirname(__file__)}/outputs/{MIGRATION_SERVER}_validation_{date.today()}.xlsx"    
    writer = pd.ExcelWriter(file_name)
    summary_df.to_excel(
        writer,
        sheet_name="Summary_Comparison",
    )
    for db_key in detail_compare_dict:
        df_row_check = 0        
        for df in detail_compare_dict[db_key]:
            start = df_row_check            
            db_df = pd.DataFrame(df)
            start = df_row_check            
            db_df.to_excel(writer, sheet_name=db_key, startrow=start)
            df_row_check += len(df) + 7        
            df_row_check = 0    
            writer.close()
    print(f"Report has been generated to : {file_name}")

def main():
    """ "Perform comparison of the source DB vs target DB"""    
    summary_compare_dict = {}
    detail_compare_dict = {}
    env_prefix_source = "source"    
    env_prefix_target = "target"    
    try:
        conn = get_db_connection("postgres", env_prefix_source)
        databases_list = [
            db            for db in pd.read_sql_query(q_database_list, conn)
            .loc[:, "db_name"]
            .to_list()
        ]
        for database in databases_list:
            summary_compare_dict[database] = {}
            print(f"Performing comparison of database: {database}")
            try:
                source_conn, target_conn = (
                    source_conn,
                    target_conn,
                ) = get_comparison_connections(
                    database, env_prefix_source, env_prefix_target                )
                rows_compared, table_rows_check = compare_row_counts(
                    source_conn, target_conn                )
                views_compared, views_check = compare_views(source_conn, target_conn)
                columns_compared, columns_check = compare_columns(
                    source_conn, target_conn                )
                triggers_compared, triggers_check = compare_triggers(
                    source_conn, target_conn                )
                (
                    usage_privileges_compared,
                    usage_privileges_check,
                ) = compare_usage_privileges(source_conn, target_conn)
                sequences_compared, sequences_check = compare_sequences(
                    source_conn, target_conn                )
                functions_compared, functions_check = compare_functions(
                    source_conn, target_conn                )
                procedures_compared, procedures_check = compare_procedures(
                    source_conn, target_conn                )
                fdw_compared, fdw_check = compare_foreign_data_wrappers(
                    source_conn, target_conn                )
                extensions_compared, extensions_check = compare_extensions(
                    source_conn, target_conn                )
                detail_compare_dict[database] = [
                    rows_compared,
                    views_compared,
                    columns_compared,
                    triggers_compared,
                    usage_privileges_compared,
                    sequences_compared,
                    functions_compared,
                    procedures_compared,
                    fdw_compared,
                    extensions_compared,
                ]
                summary_compare_dict[database][
                    "table_row_counts_equal"] = table_rows_check                
                summary_compare_dict[database]["views_equal"] = views_check                
                summary_compare_dict[database]["columns_equal"] = columns_check                
                summary_compare_dict[database]["triggers_equal"] = triggers_check                
                summary_compare_dict[database][
                    "usage_privileges_equal"] = usage_privileges_check                
                summary_compare_dict[database]["sequences_equal"] = sequences_check                
                summary_compare_dict[database]["functions_equal"] = functions_check                
                summary_compare_dict[database]["procedures_equal"] = procedures_check                
                summary_compare_dict[database]["foreign_data_wrapper_equal"] = fdw_check                
                summary_compare_dict[database]["extensions_equal"] = extensions_check                
                print(f"Comparison complete for database: {database} \n")

                source_conn.close()
                target_conn.close()
            except Exception as err:
                print(f"Could not do comparison for {database}. Error: {err}")
    except Exception as err:
        print(
            f"Could not retrieve list of databases to validate from the source server. Error: {err}")
    summary_df = pd.DataFrame.from_dict(summary_compare_dict, orient="index")
    generate_report(summary_df, detail_compare_dict)

if __name__ == "__main__":
    t1_start = perf_counter()
    print("Starting Validation\n\n")
    main()
    t1_stop = perf_counter()
    print(
        f"\n\nCompleted Validation. Elapsed time {round((t1_stop - t1_start)/60, 2)} mins")