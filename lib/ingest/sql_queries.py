"""Common SQL queries"""

ORACLE = dict()
ORACLE['ddl'] = ("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,"
                 " DATA_SCALE, NULLABLE, CHAR_LENGTH FROM"
                 " ALL_TAB_COLUMNS WHERE OWNER='{database_name}'"
                 " AND TABLE_NAME='{table_name}' ORDER BY column_id")
ORACLE['lastmodified'] = ("SELECT COUNT(*) FROM "
                          "{database_name}.{table_name} "
                          "WHERE "
                          "CAST({check_column} AS TIMESTAMP) >= "
                          "TO_TIMESTAMP('{last_value}',"
                          " 'YYYY-MM-DD HH24:MI:SS.FF9')")
ORACLE['append'] = ("SELECT CAST(COUNT(*) as decimal(38,0)) "
                    "FROM {database_name}.{table_name} "
                    "WHERE {check_column} > {last_value}")
ORACLE['count'] = ("SELECT COUNT(*) FROM {database_name}.{table_name}")
ORACLE['increment'] = ("SELECT COLUMN_NAME, TABLE_NAME, DATA_TYPE,"
                       " OWNER FROM all_tab_columns WHERE"
                       " owner = '{database_name}' AND "
                       "table_name = '{table_name}' AND "
                       "identity_column = 'YES'")
ORACLE['column'] = ("SELECT * FROM {database_name}.{table_name} "
                    "WHERE ROWNUM <= 3")

DB2 = dict()
DB2['ddl'] = ("SELECT NAME, COLTYPE, LENGTH, SCALE, NULLS"
              " FROM SYSIBM.SYSCOLUMNS WHERE"
              " TBNAME = '{table_name}' "
              "AND TBCREATOR = '{database_name}'"
              " ORDER BY colno")
DB2['lastmodified'] = ("SELECT COUNT(*) FROM {database_name}.{table_name} "
                       "WHERE timestamp({check_column}) "
                       "> timestamp('{last_value}')")
DB2['append'] = ("SELECT CAST(COUNT(*) AS DECIMAL) "
                 "FROM {database_name}.{table_name} "
                 "WHERE {check_column} > {last_value}")
DB2['count'] = ("SELECT COUNT(*) FROM {database_name}.{table_name}")
DB2['increment'] = ("SELECT NAME, TBNAME, TBCREATOR, IDENTITY FROM"
                    " SYSIBM.SYSCOLUMNS WHERE TBNAME = '{table_name}' AND"
                    " TBCREATOR = '{database_name}' AND IDENTITY = 'Y'")
DB2['column'] = ("SELECT * FROM {database_name}.{table_name} "
                 "FETCH FIRST 3 ROWS ONLY")

TD = dict()
TD['ddl'] = "HELP COLUMN {table_name}.*;"
TD['lastmodified'] = "SELECT COUNT(*) FROM {database_name}.{table_name} " \
                     "WHERE {check_column} > '{last_value}'"
TD['append'] = ("SELECT CAST(COUNT(*) as decimal(38,0)) "
                "FROM {database_name}.{table_name} "
                "WHERE {check_column} > {last_value}")
TD['count'] = ("SELECT COUNT(*) FROM {database_name}.{table_name}")
TD['increment'] = ("select ColumnName, IdColType, DatabaseName, TableName"
                   " from dbc.columns where DatabaseName='{database_name}' and"
                   " TableName='{table_name}' and IdColType in ('GA', 'GD')")
TD['column'] = ("SELECT TOP 3 * FROM {database_name}.{table_name}")

SQLSERVER = dict()
SQLSERVER['ddl'] = ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,"
                    " NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM"
                    " INFORMATION_SCHEMA.COLUMNS WHERE"
                    " TABLE_CATALOG='{database_name}'"
                    " AND TABLE_NAME='{table_name}'"
                    " AND TABLE_SCHEMA='{schema_name}'"
                    " ORDER BY ordinal_position")
SQLSERVER['lastmodified'] = ("SELECT COUNT(*) FROM "
                             "[{database_name}].[{schema_name}]"
                             ".[{table_name}] WHERE "
                             "CAST({check_column} AS TIMESTAMP) >= "
                             "convert(datetime, '{last_value}')")
SQLSERVER['append'] = ("SELECT CAST(COUNT(*) as decimal(38,0)) "
                       "FROM [{database_name}].[{schema_name}].[{table_name}] "
                       "WHERE {check_column} > {last_value}")
SQLSERVER['count'] = ("SELECT COUNT(*) FROM "
                      "[{database_name}].[{schema_name}].[{table_name}]")
SQLSERVER['increment'] = ("select name, * from sys.columns as c where"
                          " c.is_identity=1 and object_id=(select object_id "
                          "from sys.tables where name='{table_name}')")
SQLSERVER['column'] = ("SELECT TOP 3 * FROM "
                       "[{database_name}].[{schema_name}].[{table_name}]")

MYSQL = dict()
MYSQL['ddl'] = ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,"
                " NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM"
                " INFORMATION_SCHEMA.COLUMNS WHERE"
                " TABLE_SCHEMA='{database_name}'"
                " AND TABLE_NAME='{table_name}'"
                " ORDER BY ordinal_position")
MYSQL['lastmodified'] = ("SELECT COUNT(*) FROM "
                         "{database_name}.{schema_name}.{table_name} "
                         "WHERE CAST({check_column} AS TIMESTAMP) >= "
                         "convert(datetime, '{last_value}')")
MYSQL['append'] = ("SELECT CAST(COUNT(*) as decimal(38,0)) "
                   "FROM {database_name}.{table_name} "
                   "WHERE {check_column} > {last_value}")
MYSQL['count'] = ("SELECT COUNT(*) FROM {database_name}.{table_name}")
MYSQL['increment'] = ("SELECT COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA,"
                      " TABLE_CATALOG FROM information_schema.COLUMNS WHERE"
                      " EXTRA = 'auto_increment' AND TABLE_SCHEMA = "
                      "'{database_name}' AND TABLE_NAME='{table_name}'")
MYSQL['column'] = ("SELECT * FROM "
                   "{schema_name}.{table_name} LIMIT 3")

POSTGRESQL = dict()
POSTGRESQL['ddl'] = ("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,"
                     " NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM"
                     " INFORMATION_SCHEMA.COLUMNS WHERE"
                     " TABLE_CATALOG='{database_name}'"
                     " AND TABLE_NAME='{table_name}'"
                     " AND TABLE_SCHEMA='{schema_name}'"
                     " ORDER BY ordinal_position")
POSTGRESQL['lastmodified'] = ("SELECT COUNT(*) FROM "
                              "{database_name}.{schema_name}.{table_name} "
                              "WHERE CAST({check_column} AS TIMESTAMP) >= "
                              "convert(datetime, '{last_value}')")
POSTGRESQL['append'] = ("SELECT CAST(COUNT(*) as decimal(38,0)) "
                        "FROM {database_name}.{schema_name}.{table_name} "
                        "WHERE {check_column} > {last_value}")
POSTGRESQL['count'] = ("SELECT COUNT(*) FROM "
                       "{database_name}.{schema_name}.{table_name}")
POSTGRESQL['increment'] = ("select name, * from sys.columns as c where"
                           " c.is_identity=1 and object_id=(select object_id "
                           "from sys.tables where name='{table_name}')")
POSTGRESQL['column'] = ("SELECT TOP 3 * FROM "
                        "{database_name}.{schema_name}.{table_name}")
