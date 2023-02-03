from sqlalchemy import text

q_database_list = text(
    """select datname as db_name from pg_database         
       where datname != 'postgres'        
       and datname not like 'template%'         
       and datname not like 'azure%'        
       order by datname;""")

q_tables_list = text(
    """select table_name, table_schema from information_schema.tables        
       where table_schema not like 'information%'        
       and table_schema not like 'pg_%'        
       and table_name not like 'pg_%'        
       and table_type = 'BASE TABLE'        
       order by table_schema, table_name;""")

q_views = text(
    """select table_catalog, table_schema, table_name, view_definition from information_schema.views         
    where table_schema not like 'pg_%' and table_schema not like 'information%'         
    AND table_name not like 'pg_%'        
    order by table_name ;""")

q_columns = text(
    """select table_schema, table_name, column_name from information_schema.columns         
       where table_schema not like 'pg_%' and table_schema not like 'information%'         
       and table_name not like 'pg_%'        
       ORDER BY table_name, column_name ;""")

q_triggers = text(
    """select * from information_schema.triggers         
       WHERE trigger_schema not like 'information_schema'        
       AND trigger_Schema not like 'pg_%'        
       order by trigger_name, event_object_table;""")
q_usage_privileges = text(
    """select grantee, object_catalog, object_name, object_type, privilege_type, is_grantable     
       from information_schema.usage_privileges     
       where object_schema not like 'pg_%' order by object_name;""")

q_sequences = text(
    """select sequence_catalog, sequence_schema, sequence_name, data_type, numeric_precision 
       from information_schema.sequences    WHERE sequence_schema not like 'information_schema'    
       AND sequence_schema not like 'pg_%'    
       order by sequence_name;""")

q_functions = text(
    """SELECT routine_catalog, routine_schema, routine_name, data_type 
       FROM information_schema.routines WHERE routine_type = 'FUNCTION'
       AND specific_schema NOT LIKE 'information_schema'
       AND specific_schema NOT LIKE 'pg_%'
       AND routine_name not LIKE 'pg_%'
       ORDER BY routine_schema, routine_name;""")

q_procedures = text(
    """SELECT routine_catalog, routine_schema, routine_name, data_type 
       FROM information_schema.routines WHERE routine_type = 'PROCEDURE'
       AND specific_schema NOT LIKE 'information_schema'
       AND specific_schema NOT LIKE 'pg_%'
       AND routine_name not LIKE 'pg_%'
       ORDER BY routine_schema, routine_name;""")

q_fdw = text("SELECT fdwname FROM pg_foreign_data_wrapper order by fdwname;")

q_extensions = text(
    "SELECT extname FROM pg_extension where extname not like 'pg_%' order by extname;")
