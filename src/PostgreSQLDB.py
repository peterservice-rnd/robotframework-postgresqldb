# -*- coding: utf-8 -*-

from html import escape
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extensions import connection as db_connection, cursor
from robot.api import logger
from robot.utils import ConnectionCache


class PostgreSQLDB(object):
    """
    Robot Framework library for working with PostgreSQL.

    == Dependencies ==
    | psycopg2 | http://initd.org/psycopg/ | version > 2.7.3 |
    | robot framework | http://robotframework.org |
    """
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self) -> None:
        """Library initialization.
        Robot Framework ConnectionCache() class is prepared for working with concurrent connections."""
        self._connection: Optional[db_connection] = None
        self._cache = ConnectionCache()

    @property
    def connection(self) -> db_connection:
        """Get current connection to Postgres database.

        *Raises:*\n
            RuntimeError: if there isn't any open connection.

        *Returns:*\n
            Current connection to the database.
        """
        if self._connection is None:
            raise RuntimeError('There is no open connection to PostgreSQL database.')
        return self._connection

    def connect_to_postgresql(self, dbname: str, dbusername: str, dbpassword: str, dbhost: str = None,
                              dbport: str = None, alias: str = None) -> db_connection:
        """
        Connection to Postgres DB.

        *Args:*\n
            _dbname_ - database name;\n
            _dbusername_ - username for db connection;\n
            _dbpassword_ - password for db connection;\n
            _dbhost_ - host for db connection, default is localhost;\n
            _dbport_ - port for db connection, default is 5432;\n
            _alias_ - connection alias, used for switching between open connections.

        *Returns:*\n
            Returns the index of the new connection. The connection is set as active.

        *Example:*\n
            | Connect To Postgresql  |  postgres  |  postgres  |  password | localhost | 5332  | None |
        """
        try:
            logger.debug(f'Connecting using : dbhost={dbhost or "localhost"}, dbport={dbport or "5432"}, '
                         f'dbname={dbname}, dbusername={dbusername}, dbpassword={dbpassword}, alias={alias}')

            self._connection = psycopg2.connect(host=dbhost, port=dbport, dbname=dbname, user=dbusername,
                                                password=dbpassword)
            return self._cache.register(self._connection, alias)
        except psycopg2.Error as err:
            raise Exception("Logon to PostgreSQL  Error:", str(err))

    def disconnect_from_postgresql(self) -> None:
        """
        Close active PostgreSQL connection.
        You will have to manually open or switch to a new connection.
        Due to how ConnectionCache works, connection's index and alias are not removed from cache, so you are able to switch to
        closed connection, if it was closed with Disconnect from Postgresql. It will still remain closed though.

        *Example:*\n
            | Connect To Postgresql  |  postgres  |  postgres  |  password |
            | Disconnect From Postgresql |
        """
        self.connection.close()
        self._cache._current = self._cache._no_current

    def close_all_postgresql_connections(self) -> None:
        """
        Close all PostgreSQL connections that were opened.
        After calling this keyword connection index returned by opening new connections [#Connect To Postgresql |Connect To Postgresql],
        starts from 1.

        *Example:*\n
            | Connect To Postgresql  |  postgres  |  postgres |   password  |  alias=plain_pg |
            | Connect To Postgresql  |  postgres  |  login  |  password  |  alias=psc2 |
            | Switch Postgresql Connection  |  plain_pg |
            | @{sql_out_plain_pg}=  |  Execute Sql String  |  select * from postgres |
            | Switch Postgresql Connection  |  psc2 |
            | @{sql_out_psc2}=  |  Execute Sql String  |  select 1 |
            | Close All Postgresql Connections |
        """
        self._connection = self._cache.close_all()

    def switch_postgresql_connection(self, index_or_alias: Union[int, str]) -> int:
        """
        Switch to another existing PostgreSQL connection using its index or alias.\n

        The connection index is obtained on creating connection.
        Connection alias is optional and can be set at connecting to DB [#Connect To Postgresql|Connect To Postgresql].
        Due to how ConnectionCache works, you are able to switch to closed connection,
        if it was closed with Disconnect from Postgresql. It will still remain closed though.


        *Args:*\n
            _index_or_alias_ - connection index or alias assigned to connection;

        *Returns:*\n
            Index of the previous connection.

        *Example:* (switch by alias)\n
            | Connect To Postgresql  |  postgres  |  postgres |   password  |  alias=bis |
            | Connect To Postgresql  |  postgres  |  postgres  |  password  |  alias=bis_dsc |
            | Switch Postgresql Connection  |  bis |
            | @{sql_out_bis}=  |  Execute Sql String  |  select 1 |
            | Switch Postgresql Connection  |  bis_dsc |
            | @{sql_out_bis_dsc}=  |  Execute Sql String  |  select 2 |
            | Close All Postgresql Connections |
            =>\n
            @{sql_out_bis} = BIS\n
            @{sql_out_bis_dcs}= BIS_DCS
            \n
        *Example:* (switch by connection index)\n
            | ${bis_index}=  |  Connect To Postgresql  |  postgres  |  postgres  |  password  |
            | ${bis_dcs_index}=  |  Connect To Postgresql  |  postgres  |  postgres  |  password |
            | @{sql_out_bis_dcs_1}=  |  Execute Sql String  |  select 1 |
            | ${previous_index}=  |  Switch Postgresql Connection  |  ${bis_index} |
            | @{sql_out_bis}=  |  Execute Sql String  |  select 2 |
            | Switch Postgresql Connection  |  ${previous_index} |
            | @{sql_out_bis_dcs_2}=  |  Execute Sql String  |  select 3 |
            | Close All Postgresql Connections |
            =>\n
            ${bis_index}= 1\n
            ${bis_dcs_index}= 2\n
            @{sql_out_bis_dcs_1} = BIS_DCS\n
            ${previous_index}= 2\n
            @{sql_out_bis} = BIS\n
            @{sql_out_bis_dcs_2}= BIS_DCS
        """
        old_index = self._cache.current_index
        self._connection = self._cache.switch(index_or_alias)
        return old_index

    @staticmethod
    def wrap_into_html_details(statement: str, summary: str) -> str:
        """Format statement for html logging.

        *Args:*\n
            _statement_: statement to log.
            _summary_: summary for details tag.

        *Returns:*\n
            Formatted statement.
        """
        statement_html = escape(statement)
        data = f'<details><summary>{summary}</summary><p>{statement_html}</p></details>'
        return data

    def _execute_sql(self, cursor: cursor, statement: str, params: Dict[str, Any]) -> cursor:
        """ Execute SQL query on Postgres DB using active connection.

        *Args*:\n
            _cursor_: cursor object.\n
            _statement_: SQL query to be executed.\n
            _params_: SQL query parameters.\n

        *Returns:*\n
            Query results.
        """
        statement_with_params = self._replace_parameters_in_statement(statement, params)
        data = self.wrap_into_html_details(statement=statement_with_params, summary='Running PL/PGSQL statement')
        logger.info(data, html=True)
        return cursor.execute(statement, params)

    def _replace_parameters_in_statement(self, statement: str, params: Dict[str, Any]) -> str:
        """Update SQL query parameters, if any exist, with their values for logging purposes.

        *Args*:\n
            _statement_: SQL query to be updated.\n
            _params_: SQL query parameters.\n

        *Returns:*\n
            SQL query with parameter names replaced with their values.
        """
        for key, value in params.items():
            if isinstance(value, (int, float)):
                statement = statement.replace(f':{key}', str(value))
            else:
                statement = statement.replace(f':{key}', f"'{value}'")
        return statement

    def execute_plpgsql_block(self, plpgsqlstatement: str, **params: Any) -> None:
        """
        PL/PGSQL block execution.
        For parametrized SQL queries please consider psycopg2 guide on the subject:
        http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        *Args:*\n
            _plpgsqlstatement_ - PL/PGSQL block;\n
            _params_ - PL/PGSQL block parameters;\n

        *Raises:*\n
            PostgreSQL error in form of psycopg2 exception.

        *Returns:*\n
            PL/PGSQL block execution result.

        *Example:*\n
            | *Settings* | *Value* |
            | Library    |       PostgreSQLDB |

            | *Variables* | *Value* |
            | ${var_failed}    |       TRUE |

            | *Test Cases* | *Action* | *Argument* | *Argument* | *Argument* |
            | Simple |
            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DO $$  |
            |    | ...            |             |                     |    DECLARE |
            |    | ...            |             |                     |       a boolean := ${var_failed}; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       if a = TRUE then |
            |    | ...            |             |                     |         RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'This is a custom error'; |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |    END$$; |
            |    | Execute Plpgsql Block   |  plpgsqlstatement=${statement} |
            =>\n
            InternalError: This is a custom error\n

            | *Test Cases* | *Action* | *Argument* | *Argument* | *Argument* |
            | Simple |
            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DO $$  |
            |    | ...            |             |                     |    DECLARE |
            |    | ...            |             |                     |       a boolean := ${var_failed}; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       if a = TRUE then |
            |    | ...            |             |                     |         RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'This is a custom error'; |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |    END$$; |
            |    | Execute Plpgsql Block   |  plpgsqlstatement=${statement} | var=${var_failed} |
            =>\n
            InternalError: This is a custom error
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
            self._execute_sql(cursor, plpgsqlstatement, params)
            self.connection.commit()
        finally:
            if cursor:
                self.connection.rollback()

    def execute_plpgsql_script(self, file_path: str, **params: Any) -> None:
        """
        Execution of PL/PGSQL from file.
        For parametrized SQL queries please consider psycopg2 guide on the subject:
        http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        *Args:*\n
            _file_path_ - path to PL/PGSQL script file;\n
            _params_ - PL/PGSQL code parameters;\n

        *Raises:*\n
            PostgreSQL error in form of psycopg2 exception.

        *Returns:*\n
            PL/PGSQL script execution result.

        *Example:*\n
            |  Execute Plpgsql Script  |  ${CURDIR}${/}plpgsql_script.sql |
            |  Execute Plpgsql Script  |  ${CURDIR}${/}plpgsql_script.sql | first_param=1 | second_param=2 |
        """
        with open(file_path, "r") as script:
            data = script.read()
            self.execute_plpgsql_block(data, **params)

    def execute_sql_string(self, plpgsqlstatement: str, **params: Any) -> List[Tuple[Any, ...]]:
        """
        Execute PL/PGSQL string.
        For parametrized SQL queries please consider psycopg2 guide on the subject:
        http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        *Args:*\n
            _plpgsqlstatement_ - PL/PGSQL string;\n
            _params_ - PL/PGSQL string parameters;\n

        *Raises:*\n
            PostgreSQL error in form of psycopg2 exception.

        *Returns:*\n
            PL/PGSQL string execution result.

        *Example:*\n
            | @{query}= | Execute Sql String | SELECT CURRENT_DATE, CURRENT_DATE+1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |

            | @{query}= | Execute Sql String | SELECT CURRENT_DATE, CURRENT_DATE+%(d)s | d=1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
            self._execute_sql(cursor, plpgsqlstatement, params)
            query_result = cursor.fetchall()
            self.result_logger(query_result)
            return query_result
        finally:
            if cursor:
                self.connection.rollback()

    def execute_sql_string_mapped(self, plpgsqlstatement: str, **params: Any) -> List[Dict[str, Any]]:
        """SQL query execution where each result row is mapped as a dict with column names as keys.

        For parametrized SQL queries please consider psycopg2 guide on the subject:
        http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        *Args:*\n
            _plpgsqlstatement_ - PL/PGSQL string;\n
            _params_ - PL/PGSQL string parameters;\n

        *Returns:*\n
            A list of dictionaries where column names are mapped as keys

        *Example:*\n
            | @{query}= | Execute Sql String Mapped| SELECT CURRENT_DATE, CURRENT_DATE+1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][sysdate]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][sysdate1]} |

            | @{query}= | Execute Sql String Mapped| SELECT CURRENT_DATE, CURRENT_DATE+%(d)s | d=1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][sysdate]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][sysdate1]} |
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
            self._execute_sql(cursor, plpgsqlstatement, params)
            col_name = tuple(i[0] for i in cursor.description)
            query_result = [dict(zip(col_name, row)) for row in cursor]
            self.result_logger(query_result)
            return query_result
        finally:
            if cursor:
                self.connection.rollback()

    def result_logger(self, query_result: List[Any], result_amount: int = 10) -> None:
        """Log first n results of the query results

        *Args:*\n
            _query_result_ - query result to log, must be greater than 0
            _result_amount_ - amount of entries to display from result
        """
        if len(query_result) > result_amount > 0:
            query_result = query_result[:result_amount]
        logged_result = self.wrap_into_html_details(str(query_result), "SQL Query Result")
        logger.info(logged_result, html=True)
