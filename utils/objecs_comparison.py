from datetime import date
from typing import Tuple
import pandas as pd
from sqlalchemy.engine.base import Connection
from utils.db_objects import (
    get_source_row_counts,
    get_target_row_counts,
    get_views,
    get_columns,
    get_triggers,
    get_usage_privileges,
    get_sequences,
    get_functions,
    get_procedures,
    get_foreign_data_wrappers,
    get_extensions,
)
from utils.schemas import (
    q_sequences,
    q_columns,
    q_functions,
    q_extensions,
    q_fdw,
    q_procedures,
    q_tables_list,
    q_triggers,
    q_usage_privileges,
    q_views,
)
merge_lookup = {"both": "both", "left_only": "source_only", "right_only": "target_only"}
def compare_row_counts(
    source_conn: Connection, target_conn: Connection) -> Tuple[pd.DataFrame, bool]:
    """Compare the TABLE ROW COUNTS of source vs target DB"""    
    table_rows_check = False    
    src_table_row_counts_df = get_source_row_counts(source_conn)
    targ_table_row_counts_df = get_target_row_counts(target_conn)
    if src_table_row_counts_df["row_count"].equals(
        targ_table_row_counts_df["row_count"]
    ):
        table_rows_check = True    
        rows_compared = src_table_row_counts_df.copy().join(
        targ_table_row_counts_df.row_count, how="outer", rsuffix="_target"    )
    rows_compared.rename(columns={"row_count": "row_count_source"}, inplace=True)
    rows_compared["difference_count"] = (
        rows_compared.row_count_source - rows_compared.row_count_target    )
    rows_compared.insert(
        len(rows_compared.columns) - 1,
        "query_executed",
        rows_compared.pop("query_executed"),
    )
    rows_compared.columns = [
        ["Table_Row_Counts_Comparison"] * len(rows_compared.columns.to_list()),
        rows_compared.columns.to_list(),
    ]
    if rows_compared.empty:
        rows_compared = pd.DataFrame(
            data=[""],
            columns=[["Table_Row_Counts_Comparison"], ["Row_Counts"]],
        )
        rows_compared.insert(
            loc=len(rows_compared.columns) - 1,
            column=("Table_Row_Counts_Comparison", "query_executed"),
            value="N/A - Tables empty",
        )
    rows_compared.insert(
        loc=len(rows_compared.columns) - 1,
        column=("Table_Row_Counts_Comparison", "migration_date"),
        value=date.today(),
    )
    rows_compared.insert(
        loc=len(rows_compared.columns) - 1,
        column=("Table_Row_Counts_Comparison", "validation_date"),
        value=date.today(),
    )
    return rows_compared, table_rows_check


def compare_views(source_conn: Connection, target_conn: Connection):
    """Compare the VIEWS of source vs target DB"""    
    views_check = False    
    src_views_df, targ_views_df = get_views(source_conn, target_conn)
    views_merged = src_views_df.merge(targ_views_df, how="outer", indicator=True)
    views_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    views_merged["source_v_target"] = views_merged["source_v_target"].map(merge_lookup)
    views_compared = views_merged[~views_merged["source_v_target"].isin(["both"])]
    views_compared.columns = [
        ["Views_Comparision"] * len(views_compared.columns.to_list()),
        views_compared.columns.to_list(),
    ]
    if views_compared.empty:
        views_check = True        
        views_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[["Views_Comparision"], ["Source_v_Target"]],
        )
    views_compared.insert(
        loc=len(views_compared.columns) - 1,
        column=("Views_Comparision", "query_executed"),
        value=q_views,
    )
    views_compared.insert(
        loc=len(views_compared.columns) - 1,
        column=("Views_Comparision", "migration_date"),
        value=date.today(),
    )
    views_compared.insert(
        loc=len(views_compared.columns) - 1,
        column=("Views_Comparision", "validation_date"),
        value=date.today(),
    )
    return views_compared, views_check


def compare_columns(source_conn: Connection, target_conn: Connection):
    """Compare the COLUMNS of source vs target DB"""    
    columns_check = False    
    src_columns_df, targ_columns_df = get_columns(source_conn, target_conn)
    columns_merged = src_columns_df.merge(targ_columns_df, how="outer", indicator=True)
    columns_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    columns_merged["source_v_target"] = columns_merged["source_v_target"].map(
        merge_lookup    )
    columns_compared = columns_merged[~columns_merged["source_v_target"].isin(["both"])]
    columns_compared.columns = [
        ["Columns_Comparision"] * len(columns_compared.columns.to_list()),
        columns_compared.columns.to_list(),
    ]
    if columns_compared.empty:
        columns_check = True        
        columns_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Columns_Comparision"],
                ["Source_v_Target"],
            ],
        )
    columns_compared.insert(
        loc=len(columns_compared.columns) - 1,
        column=("Columns_Comparision", "query_executed"),
        value=q_columns,
    )
    columns_compared.insert(
        loc=len(columns_compared.columns) - 1,
        column=("Columns_Comparision", "migration_date"),
        value=date.today(),
    )
    columns_compared.insert(
        loc=len(columns_compared.columns) - 1,
        column=("Columns_Comparision", "validation_date"),
        value=date.today(),
    )
    return columns_compared, columns_check


def compare_triggers(source_conn: Connection, target_conn: Connection):
    """Compare the TRIGGERS of source vs target DB"""    
    triggers_check = False    
    src_triggers_df, targ_triggers_df = get_triggers(source_conn, target_conn)
    triggers_merged = src_triggers_df.merge(
        targ_triggers_df, how="outer", on="trigger_name", indicator=True    )
    triggers_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    triggers_merged["source_v_target"] = triggers_merged["source_v_target"].map(
        merge_lookup    )
    triggers_compared = triggers_merged[
        ~triggers_merged["source_v_target"].isin(["both"])
    ]
    triggers_compared.columns = [
        ["Triggers_Comparision"] * len(triggers_compared.columns.to_list()),
        triggers_compared.columns.to_list(),
    ]
    if triggers_compared.empty:
        triggers_check = True        
        triggers_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Triggers_Comparision"],
                ["Source_v_Target"],
            ],
        )
    triggers_compared.insert(
        loc=len(triggers_compared.columns) - 1,
        column=("Triggers_Comparision", "query_executed"),
        value=q_triggers,
    )
    triggers_compared.insert(
        loc=len(triggers_compared.columns) - 1,
        column=("Triggers_Comparision", "migration_date"),
        value=date.today(),
    )
    triggers_compared.insert(
        loc=len(triggers_compared.columns) - 1,
        column=("Triggers_Comparision", "validation_date"),
        value=date.today(),
    )
    return triggers_compared, triggers_check


def compare_usage_privileges(source_conn: Connection, target_conn: Connection):
    """Compare the USAGE PRIVILEGES of source vs target DB"""    
    usage_privileges_check = False    
    src_usage_privileges_df, targ_usage_privileges_df = get_usage_privileges(
        source_conn, target_conn    )
    usage_privileges_merged = src_usage_privileges_df.merge(
        targ_usage_privileges_df, how="outer", indicator=True    )
    usage_privileges_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    usage_privileges_merged["source_v_target"] = usage_privileges_merged[
        "source_v_target"    ].map(merge_lookup)
    usage_privileges_compared = usage_privileges_merged[
        ~usage_privileges_merged["source_v_target"].isin(["both"])
    ]
    usage_privileges_compared.columns = [
        ["Usage_privileges_Comparision"]
        * len(usage_privileges_compared.columns.to_list()),
        usage_privileges_compared.columns.to_list(),
    ]
    if usage_privileges_compared.empty:
        usage_privileges_check = True        
        usage_privileges_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Usage_privileges_Comparision"],
                ["Source_v_Target"],
            ],
        )
    usage_privileges_compared.insert(
        loc=len(usage_privileges_compared.columns) - 1,
        column=("Usage_privileges_Comparision", "query_executed"),
        value=q_usage_privileges,
    )
    usage_privileges_compared.insert(
        loc=len(usage_privileges_compared.columns) - 1,
        column=("Usage_privileges_Comparision", "migration_date"),
        value=date.today(),
    )
    usage_privileges_compared.insert(
        loc=len(usage_privileges_compared.columns) - 1,
        column=("Usage_privileges_Comparision", "validation_date"),
        value=date.today(),
    )
    return usage_privileges_compared, usage_privileges_check


def compare_sequences(source_conn: Connection, target_conn: Connection):
    """Compare the SEQUENCES of source vs target DB"""    
    sequences_check = False    
    src_sequences_df, targ_sequences_df = get_sequences(source_conn, target_conn)
    sequences_merged = src_sequences_df.merge(
        targ_sequences_df, how="outer", indicator=True    )
    sequences_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    sequences_merged["source_v_target"] = sequences_merged["source_v_target"].map(
        merge_lookup    )
    sequences_compared = sequences_merged[
        ~sequences_merged["source_v_target"].isin(["both"])
    ]
    sequences_compared.columns = [
        ["Sequences_Comparision"] * len(sequences_compared.columns.to_list()),
        sequences_compared.columns.to_list(),
    ]
    if sequences_compared.empty:
        sequences_check = True        
        sequences_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Sequences_Comparision"],
                ["Source_v_Target"],
            ],
        )
    sequences_compared.insert(
        loc=len(sequences_compared.columns) - 1,
        column=("Sequences_Comparision", "query_executed"),
        value=q_sequences,
    )
    sequences_compared.insert(
        loc=len(sequences_compared.columns) - 1,
        column=("Sequences_Comparision", "migration_date"),
        value=date.today(),
    )
    sequences_compared.insert(
        loc=len(sequences_compared.columns) - 1,
        column=("Sequences_Comparision", "validation_date"),
        value=date.today(),
    )
    return sequences_compared, sequences_check


def compare_functions(source_conn: Connection, target_conn: Connection):
    """Compare the FUNCTIONS of source vs target DB"""    
    
    functions_check = False    
    src_functions_df, targ_functions_df = get_functions(source_conn, target_conn)
    functions_merged = src_functions_df.merge(
        targ_functions_df, how="outer", indicator=True    )
    functions_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    functions_merged["source_v_target"] = functions_merged["source_v_target"].map(
        merge_lookup    )
    functions_compared = functions_merged[
        ~functions_merged["source_v_target"].isin(["both"])
    ]
    functions_compared.columns = [
        ["Functions_Comparision"] * len(functions_compared.columns.to_list()),
        functions_compared.columns.to_list(),
    ]
    if functions_compared.empty:
        functions_check = True        
        functions_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Functions_Comparision"],
                ["Source_v_Target"],
            ],
        )
    functions_compared.insert(
        loc=len(functions_compared.columns) - 1,
        column=("Functions_Comparision", "query_executed"),
        value=q_functions,
    )
    functions_compared.insert(
        loc=len(functions_compared.columns) - 1,
        column=("Functions_Comparision", "migration_date"),
        value=date.today(),
    )
    functions_compared.insert(
        loc=len(functions_compared.columns) - 1,
        column=("Functions_Comparision", "validation_date"),
        value=date.today(),
    )
    return (
        functions_compared,
        functions_check,
    )
def compare_procedures(source_conn: Connection, target_conn: Connection):
    """Compare the PROCEDURES of source vs target DB"""   
    procedures_check = False    
    src_procedures_df, targ_procedures_df = get_procedures(source_conn, target_conn)
    procedures_merged = src_procedures_df.merge(
        targ_procedures_df, how="outer", indicator=True    )
    procedures_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    procedures_merged["source_v_target"] = procedures_merged["source_v_target"].map(
        merge_lookup    )
    procedures_compared = procedures_merged[
        ~procedures_merged["source_v_target"].isin(["both"])
    ]
    procedures_compared.columns = [
        ["Procedures_Comparision"] * len(procedures_compared.columns.to_list()),
        procedures_compared.columns.to_list(),
    ]
    if procedures_compared.empty:
        procedures_check = True        
        procedures_compared = pd.DataFrame(
            data=["source & target are the same", q_procedures],
            columns=[
                ["Procedures_Comparision"],
                ["Source_v_Target"],
            ],
        )
    procedures_compared.insert(
        loc=len(procedures_compared.columns) - 1,
        column=("Procedures_Comparision", "query_executed"),
        value=q_procedures,
    )
    procedures_compared.insert(
        loc=len(procedures_compared.columns) - 1,
        column=("Procedures_Comparision", "migration_date"),
        value=date.today(),
    )
    procedures_compared.insert(
        loc=len(procedures_compared.columns) - 1,
        column=("Procedures_Comparision", "validation_date"),
        value=date.today(),
    )
    return procedures_compared, procedures_check


def compare_foreign_data_wrappers(source_conn: Connection, target_conn: Connection):
    """Compare the FOREIGN DATA WRAPPERS of source vs target DB"""    
    fdw_check = False    
    src_fdw_df, targ_fdw_df = get_foreign_data_wrappers(source_conn, target_conn)
    fdw_merged = src_fdw_df.merge(targ_fdw_df, how="outer", indicator=True)
    fdw_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    fdw_merged["source_v_target"] = fdw_merged["source_v_target"].map(merge_lookup)
    fdw_compared = fdw_merged[~fdw_merged["source_v_target"].isin(["both"])]
    fdw_compared.columns = [
        ["Foreign_Data_Wrapper_Comparision"] * len(fdw_compared.columns.to_list()),
        fdw_compared.columns.to_list(),
    ]
    if fdw_compared.empty:
        fdw_check = True        
        fdw_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Foreign_Data_Wrapper_Comparision"],
                ["Source_v_Target"],
            ],
        )
    fdw_compared.insert(
        loc=len(fdw_compared.columns) - 1,
        column=("Foreign_Data_Wrapper_Comparision", "query_executed"),
        value=q_fdw,
    )
    fdw_compared.insert(
        loc=len(fdw_compared.columns) - 1,
        column=("Foreign_Data_Wrapper_Comparision", "migration_date"),
        value=date.today(),
    )
    fdw_compared.insert(
        loc=len(fdw_compared.columns) - 1,
        column=("Foreign_Data_Wrapper_Comparision", "validation_date"),
        value=date.today(),
    )
    return fdw_compared, fdw_check

def compare_extensions(source_conn: Connection, target_conn: Connection):
    """Compare the EXTENSIONS of source vs target DB"""    
    extensions_check = False    
    src_extensions_df, targ_extensions_df = get_extensions(source_conn, target_conn)
    extensions_merged = src_extensions_df.merge(
        targ_extensions_df, how="outer", indicator=True    )
    extensions_merged.rename(columns={"_merge": "source_v_target"}, inplace=True)
    extensions_merged["source_v_target"] = extensions_merged["source_v_target"].map(
        merge_lookup    )
    extensions_compared = extensions_merged[
        ~extensions_merged["source_v_target"].isin(["both"])
    ]
    extensions_compared.columns = [
        ["Extensions_Comparision"] * len(extensions_compared.columns.to_list()),
        extensions_compared.columns.to_list(),
    ]
    if extensions_compared.empty:
        extensions_check = True        
        extensions_compared = pd.DataFrame(
            data=["source & target are the same"],
            columns=[
                ["Extensions_Comparision"],
                ["Source_v_Target"],
            ],
        )
    extensions_compared.insert(
        loc=len(extensions_compared.columns) - 1,
        column=("Extensions_Comparision", "query_executed"),
        value=q_extensions,
    )
    extensions_compared.insert(
        loc=len(extensions_compared.columns) - 1,
        column=("Extensions_Comparision", "migration_date"),
        value=date.today(),
    )
    extensions_compared.insert(
        loc=len(extensions_compared.columns) - 1,
        column=("Extensions_Comparision", "validation_date"),
        value=date.today(),
    )
    return extensions_compared, extensions_check