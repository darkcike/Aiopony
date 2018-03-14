import os
import asyncio

import pony
from pony.orm.dbproviders.mysql import *
from pony.orm.dbschema import DBSchema, Table, Constraint, DBIndex, DBObject, ForeignKey, Column
from pony.py23compat import itervalues, basestring
from pony.orm.core import DBSchemaError, log_sql

try:
    import aiomysql as mysql_module
except ImportError:
    raise ImportError('No module named aiomysql found')
from pymysql.converters import escape_str as string_literal
import pymysql.converters as mysql_converters
from pymysql.constants import FIELD_TYPE, FLAG, CLIENT

mysql_module_name = 'pymysql'

localbase = object


class AioPool:
    forked_connections = []

    def __init__(pool, dbapi_module, *args, **kwargs):  # called separately in each thread
        pool.dbapi_module = dbapi_module
        pool.args = args
        pool.kwargs = kwargs
        pool.con = pool.pid = None
        pool.apool = None

    async def connect(pool):
        if pool.apool is not None:
            return await pool.apool.acquire()
        apool = await pool._connect()
        return await apool.acquire()

    async def _connect(pool):
        if 'passwd' in pool.kwargs:
            pool.kwargs['password'] = pool.kwargs['passwd']
            del pool.kwargs['passwd']
        # pool.kwargs['autocommit'] = True
        pool.apool = await mysql_module.create_pool(1, 100, **pool.kwargs)
        return pool.apool

    async def release(pool, con):
        try:
            await con.rollback()
        except:
            pool.drop(con)
            raise

    def release_to_pool(pool, con):
        pool.apool.release(con)

    def drop(pool, con):
        pool.release_to_pool(con)

    def disconnect(pool):
        pool.apool.close()


async def _create(self, provider, connection):
    sql = self.get_create_command()
    if core.debug: log_sql(sql)
    cursor = await connection.cursor()
    await provider.execute(cursor, sql)


DBObject.create = _create


class AioTable(Table):
    async def exists(table, provider, connection, case_sensitive=True):
        return await provider.table_exists(connection, table.name, case_sensitive)


class AioDBIndex(DBIndex):
    async def exists(index, provider, connection, case_sensitive=True):
        return await provider.index_exists(connection, index.table.name, index.name, case_sensitive)


class AioForeignKey(ForeignKey):
    async def exists(foreign_key, provider, connection, case_sensitive=True):
        return await provider.fk_exists(connection, foreign_key.child_table.name, foreign_key.name, case_sensitive)


class AioDBSchema(MySQLSchema):
    table_class = AioTable
    index_class = AioDBIndex
    fk_class = AioForeignKey
    column_class = MySQLColumn

    async def create_tables(schema, provider, connection):
        created_tables = set()
        for table in schema.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                name = await db_object.exists(provider, connection, case_sensitive=False)
                if name is None:
                    await db_object.create(provider, connection)
                elif name != db_object.name:
                    quote_name = schema.provider.quote_name
                    n1, n2 = quote_name(db_object.name), quote_name(name)
                    tn1, tn2 = db_object.typename, db_object.typename.lower()
                    throw(DBSchemaError, '%s %s cannot be created, because %s %s ' \
                                         '(with a different letter case) already exists in the database. ' \
                                         'Try to delete %s %s first.' % (tn1, n1, tn2, n2, n2, tn2))

    async def check_tables(schema, provider, connection):
        cursor = await connection.cursor()
        for table in sorted(itervalues(schema.tables), key=lambda table: table.name):
            if isinstance(table.name, tuple):
                alias = table.name[-1]
            elif isinstance(table.name, basestring):
                alias = table.name
            else:
                assert False  # pragma: no cover
            sql_ast = ['SELECT',
                       ['ALL', ] + [['COLUMN', alias, column.name] for column in table.column_list],
                       ['FROM', [alias, 'TABLE', table.name]],
                       ['WHERE', ['EQ', ['VALUE', 0], ['VALUE', 1]]]
                       ]
            sql, adapter = provider.ast2sql(sql_ast)
            if core.debug: log_sql(sql)
            await provider.execute(cursor, sql)


class AioMysqSqlProvider(MySQLProvider):
    dbschema_cls = AioDBSchema

    def __init__(self):
        pass

    async def _init_(provider, *args, **kwargs):
        pool_mockup = kwargs.pop('pony_pool_mockup', None)
        if pool_mockup:
            provider.pool = pool_mockup
        else:
            provider.pool = provider.get_pool(*args, **kwargs)
        connection = await provider.connect()
        await provider.inspect_connection(connection)
        await provider.release(connection)

    async def inspect_connection(provider, connection):
        cursor = await connection.cursor()
        await cursor.execute('select version()')
        row = await cursor.fetchone()
        assert row is not None
        provider.server_version = get_version_tuple(row[0])
        if provider.server_version >= (5, 6, 4):
            provider.max_time_precision = 6
        await cursor.execute('select database()')
        provider.default_schema_name = (await cursor.fetchone())[0]

    def get_pool(provider, *args, **kwargs):
        if 'conv' not in kwargs:
            conv = mysql_converters.conversions.copy()
            if mysql_module_name == 'MySQLdb':
                conv[FIELD_TYPE.BLOB] = [(FLAG.BINARY, buffer)]
            else:
                if PY2:
                    def encode_buffer(val, encoders=None):
                        return string_literal(str(val), encoders)

                    conv[buffer] = encode_buffer

            def encode_timedelta(val, encoders=None):
                return string_literal(timedelta2str(val), encoders)

            conv[timedelta] = encode_timedelta
            conv[FIELD_TYPE.TIMESTAMP] = str2datetime
            conv[FIELD_TYPE.DATETIME] = str2datetime
            conv[FIELD_TYPE.TIME] = str2timedelta
            kwargs['conv'] = conv
        if 'charset' not in kwargs:
            kwargs['charset'] = 'utf8'
        kwargs['client_flag'] = kwargs.get('client_flag', 0) | CLIENT.FOUND_ROWS
        # print(mysql_module.__name__)
        return AioPool(mysql_module, *args, **kwargs)

    async def set_transaction_mode(provider, connection, cache):
        # assert not cache.in_transaction
        if cache.in_transaction:
            return
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cursor = await connection.cursor()
            await cursor.execute("SHOW VARIABLES LIKE 'foreign_key_checks'")
            fk = await cursor.fetchone()
            if fk is not None: fk = (fk[1] == 'ON')
            if fk:
                sql = 'SET foreign_key_checks = 0'
                if core.debug: log_orm(sql)
                await cursor.execute(sql)
            cache.saved_fk_state = bool(fk)
            cache.in_transaction = True
        cache.immediate = True
        if db_session is not None and db_session.serializable:
            cursor = await connection.cursor()
            sql = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            if core.debug: log_orm(sql)
            await cursor.execute(sql)
            cache.in_transaction = True

    async def release(provider, connection, cache=None):
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                try:
                    cursor = await connection.cursor()
                    sql = 'SET foreign_key_checks = 1'
                    if core.debug: log_orm(sql)
                    await cursor.execute(sql)
                except:
                    provider.pool.drop(connection)
                    raise
        if cache is not None and cache.db_session is not None and cache.db_session.ddl:
            provider.drop(connection, cache)
        else:
            if core.debug:
                core.log_orm('RELEASE CONNECTION')
            await provider.pool.release(connection)

    async def table_exists(provider, connection, table_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        cursor = await connection.cursor()
        if case_sensitive:
            sql = 'SELECT table_name FROM information_schema.tables ' \
                  'WHERE table_schema=%s and table_name=%s'
        else:
            sql = 'SELECT table_name FROM information_schema.tables ' \
                  'WHERE table_schema=%s and UPPER(table_name)=UPPER(%s)'
        await cursor.execute(sql, [db_name, table_name])
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def index_exists(provider, connection, table_name, index_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive:
            sql = 'SELECT index_name FROM information_schema.statistics ' \
                  'WHERE table_schema=%s and table_name=%s and index_name=%s'
        else:
            sql = 'SELECT index_name FROM information_schema.statistics ' \
                  'WHERE table_schema=%s and table_name=%s and UPPER(index_name)=UPPER(%s)'
        cursor = await connection.cursor()
        await cursor.execute(sql, [db_name, table_name, index_name])
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def fk_exists(provider, connection, table_name, fk_name, case_sensitive=True):
        db_name, table_name = provider.split_table_name(table_name)
        if case_sensitive:
            sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                  'WHERE table_schema=%s and table_name=%s ' \
                  "and constraint_type='FOREIGN KEY' and constraint_name=%s"
        else:
            sql = 'SELECT constraint_name FROM information_schema.table_constraints ' \
                  'WHERE table_schema=%s and table_name=%s ' \
                  "and constraint_type='FOREIGN KEY' and UPPER(constraint_name)=UPPER(%s)"
        cursor = await connection.cursor()
        await cursor.execute(sql, [db_name, table_name, fk_name])
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def connect(provider):
        if core.debug:
            core.log_orm('NEW CONNECTION')
        return await provider.pool.connect()

    async def commit(provider, connection, cache=None):
        if core.debug: core.log_orm('COMMIT')
        await connection.commit()
        if cache is not None: cache.in_transaction = False

    async def rollback(provider, connection, cache=None):
        if core.debug: core.log_orm('ROLLBACK')
        await connection.rollback()
        if cache is not None: cache.in_transaction = False

    async def execute(provider, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            assert arguments and not returning_id
            await cursor.executemany(sql, arguments)
        else:
            if arguments is None:
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, arguments)
            if returning_id: return cursor.lastrowid

    def release_to_pool(provider, con):
        if core.debug:
            core.log_orm('RELEASE CONNECTION')
        provider.pool.release_to_pool(con)

    async def drop_table(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = await connection.cursor()
        sql = 'DROP TABLE %s' % table_name
        print(sql)
        await cursor.execute(sql)

    async def table_has_data(provider, connection, table_name):
        table_name = provider.quote_name(table_name)
        cursor = await connection.cursor()
        await cursor.execute('SELECT 1 FROM %s LIMIT 1' % table_name)
        return (await cursor.fetchone()) is not None


provider_cls = AioMysqSqlProvider
