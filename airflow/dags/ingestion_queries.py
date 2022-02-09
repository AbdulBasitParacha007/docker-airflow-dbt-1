queries = {
    "jdbc_ingestion": """
        SELECT 
       	sct.name as src_connection_type,
        ingestion.ID as INGESTION_ID, ingestion_mode.NAME as INGESTION_MODE,ingestion.TABLE_ID as TBL_ID, ist.NAME as SOURCE_TYPE, sdb.ID as SDB_ID, sdb.NAME as DB_NAME, schemas.NAME as SCHEMA_NAME, schematable.NAME as TABLE_NAME, schematablehst.NAME as HISTORY_TABLE_NAME, ifreq.FREQUENCY as FREQ, 
		ifreq.FREQUENCY_CRON as FREQ_CRON,
		tbl_col.name as AUDIT_COL,		
        ifp.file_type, ifp.file_reader, ifp.delimeter, 
        ifp.multi_line, ifp.database_name as custom_db_name, ifp.schema_name as custom_schema_name, 
        ifp.table_name as custom_table_name, ifp.schema_file_path, ifp.header_row,     
		string_agg(distinct prime.NAME, ',') as PRIMARY_KEY_COLUMNS,
		string_agg(distinct tdb.ID::text, ',') as TDB_ID
        FROM dmx_db_INGESTION as ingestion
        INNER JOIN dmx_db_ingestion_target as ingTrg
        ON ingTrg .ingestion_id = ingestion .ID
        INNER JOIN dmx_db_SOURCE_DB as tdb
        ON tdb.ID = ingTrg.target_id        
        INNER JOIN public.dmx_db_source_connection_type tct
		ON tdb.src_conn_type_id = tct.id
        INNER JOIN dmx_db_FREQUENCY as ifreq
        ON ingestion .FREQUENCY_ID = ifreq .ID
        INNER JOIN dmx_db_SCHEMA_TABLE as schematable
        ON ingestion .TABLE_ID = schematable .ID
        INNER JOIN dmx_db_SCHEMA as schemas
        ON schematable .SCHEMA_ID = schemas .ID
        INNER JOIN dmx_db_SOURCE_DB as sdb
        ON sdb.ID =schemas .SOURCE_DB_ID
        INNER JOIN dmx_db_SOURCE_TYPE ist
        ON ist.ID = sdb.SOURCE_DB_TYPE_ID
        INNER JOIN public.dmx_db_source_connection_type sct
		ON sdb.src_conn_type_id = sct.id
        INNER JOIN dmx_db_INGESTION_MODE ingestion_mode
        on ingestion_mode.id = ingestion.INGESTION_MODE_ID
        LEFT JOIN dmx_db_TABLE_COLUMN tbl_col
        ON ingestion.column_id = tbl_col.id
        LEFT JOIN (SELECT name, PRIMARY_KEY as pk, TABLE_ID
        FROM dmx_db_TABLE_COLUMN
        WHERE PRIMARY_KEY is True	
        ) prime
        ON ingestion.TABLE_ID = prime.TABLE_ID
        LEFT JOIN dmx_db_SCHEMA_TABLE as schematablehst
        ON ingestion.HISTORY_TABLE_ID = schematablehst .ID
        LEFT JOIN public.dmx_db_file_properties ifp
        ON ingestion.TABLE_ID = ifp.source_TABLE_ID
        group by 
		sct.name,
		ingestion_mode.NAME, ingestion.ID, ingestion.TABLE_ID, ist.NAME, sdb.ID, sdb.NAME, schemas.NAME, schematable.NAME,  schematablehst.NAME, ifreq.FREQUENCY, ifreq.FREQUENCY_CRON,
		ifp.file_type, ifp.file_reader, ifp.delimeter, 
        ifp.multi_line, ifp.database_name, ifp.schema_name, 
        ifp.table_name, ifp.schema_file_path, ifp.header_row,       
		tbl_col.name
        ;
    """,

    "file_ingestion": """
        SELECT
        sct.name as src_connection_type,
        ingestion.ID as INGESTION_ID,
        ingestion_mode.NAME as INGESTION_MODE,ingestion.TABLE_ID as TBL_ID,
        ist.NAME as SOURCE_TYPE,
        sdb.ID as SDB_ID,
        sdb.NAME as DB_NAME,
        schemas.NAME as SCHEMA_NAME,
        schematable.NAME as TABLE_NAME,
        schematablehst.NAME as HISTORY_TABLE_NAME,
        ifreq.FREQUENCY as FREQ,
        ifreq.FREQUENCY_CRON as FREQ_CRON,
        ifp.file_type, ifp.file_reader, ifp.delimeter, 
        ifp.multi_line, ifp.database_name as custom_db_name, ifp.schema_name as custom_schema_name, 
        ifp.table_name as custom_table_name, ifp.schema_file_path,        
        null as TDB_ID
        FROM dmx_db_INGESTION as ingestion
        INNER JOIN dmx_db_FREQUENCY as ifreq
        ON ingestion .FREQUENCY_ID = ifreq .ID
        INNER JOIN dmx_db_SCHEMA_TABLE as schematable
        ON ingestion .TABLE_ID = schematable .ID
        INNER JOIN dmx_db_SCHEMA as schemas
        ON schematable .SCHEMA_ID = schemas .ID
        INNER JOIN dmx_db_SOURCE_DB as sdb
        ON sdb.ID =schemas .SOURCE_DB_ID
        INNER JOIN dmx_db_SOURCE_TYPE ist
        ON ist.ID = sdb.SOURCE_DB_TYPE_ID
        INNER JOIN public.dmx_db_source_connection_type sct
        ON sdb.src_conn_type_id = sct.id
        INNER JOIN dmx_db_INGESTION_MODE ingestion_mode
        on ingestion_mode.id = ingestion.INGESTION_MODE_ID
        LEFT JOIN dmx_db_TABLE_COLUMN tbl_col
        ON ingestion.column_id = tbl_col.id
        LEFT JOIN public.dmx_db_file_properties ifp
        ON ingestion.TABLE_ID = ifp.source_TABLE_ID
        LEFT JOIN dmx_db_SCHEMA_TABLE as schematablehst
        ON ingestion.HISTORY_TABLE_ID = schematablehst .ID
        where sct.name = 'FileSystem'
        group by ifp.file_type, ifp.file_reader, ifp.delimeter, ifp.multi_line, ifp.database_name, ifp.schema_name, 
            ifp.table_name, ifp.schema_file_path, ifreq.FREQUENCY_CRON,
            sct.name, ingestion_mode.NAME, ingestion.ID, ingestion.TABLE_ID, ist.NAME, 
            sdb.ID, sdb.NAME, schemas.NAME, schematable.NAME,  schematablehst.NAME, ifreq.FREQUENCY
        ;
    
    """,

    "ingestion_frequencies": """
        select 
        distinct
        ifreq.FREQUENCY as FREQ,
        ifreq.FREQUENCY_CRON as FREQ_CRON
        FROM dmx_db_INGESTION as ingestion
        INNER JOIN dmx_db_FREQUENCY as ifreq
        ON ingestion .FREQUENCY_ID = ifreq .ID
        order by ifreq.FREQUENCY
            
    """,

    "table_structure": """
        select distinct 
        bdtc.name as column_name, 
        column_position, 
        primary_key, 
        bdt.name as column_data_type, 
        table_id, 
        bdt.source_type_id 
        from dmx_db_table_column bdtc 
        left join dmx_data_type bdt on bdtc.data_type_id = bdt.id 
        ;            
    """,

    "target_ingestion": """
        select distinct 
        target_id, 
        bddm.name as delivery_mode,
        bddm.short_code as dm_short_code,
        bdsd.name as target_name,
        bdsct.name as trg_connection_type
        from dmx_db_ingestion_target bdit 
        inner join dmx_db_ingestion bdi on bdit.ingestion_id = bdi.id
        inner join dmx_db_delivery_mode bddm on bdit.delivery_mode_id = bddm.id
        inner join dmx_db_source_db bdsd on bdit.target_id = bdsd.id 
        inner join dmx_db_source_connection_type bdsct on bdsd.src_conn_type_id = bdsct.id
        ;           
    """,
    "connection_properties": """
        SELECT * from dmx_db_source_db_connection_property
        ;
    """,

    "kafka_ingestion": "",
}