# -*- coding: utf-8 -*-
import itertools
import types
import asyncio
import warnings
from threading import Lock, RLock, currentThread as current_thread, _MainThread
from collections import defaultdict
from operator import attrgetter, itemgetter
from time import time
from itertools import repeat, starmap
from threading import local as localbase

from pony import options
from pony.orm import core
from pony.orm.core import *
from pony.orm.core import EntityMeta, Entity, SessionCache, PartialCommitException, Attribute, NOT_LOADED, \
    SetData, SetInstance, local, EntityIter, DBSessionContextManager, throw_object_was_deleted, populate_criteria_list, \
    adapted_sql_cache, string2ast_cache, select_re, num_counter, orm_logger, sql_logger, orm_log_level, log_orm, \
    log_sql, args2str, _get_caches, transact_reraise, del_statuses, created_or_deleted_statuses, saved_statuses, \
    get_globals_and_locals, string2ast, Query, unpickle_query, QueryStat, QueryResult, ast2src, strcut, \
    throw_db_session_is_over, safe_repr, suppress_debug_change, Local, DEFAULT, new_instance_id_counter, utils

from pony.orm.dbschema import DBSchema
from pony.orm.decompiling import decompile
from pony.thirdparty.compiler import ast
from pony.utils import throw, reraise, truncate_repr, get_lambda_args, decorator, \
    deprecated, import_module, parse_expr, is_ident, tostring, strjoin, concat, cut_traceback, sys
from pony.orm.dbapiprovider import (
    DBAPIProvider, DBException, Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError
)

from pony.py23compat import PY2, izip, imap, iteritems, itervalues, items_list, values_list, xrange, cmp, \
    basestring, unicode, buffer, int_types, builtins, pickle, with_metaclass

from .aiomysql_provider import AioMysqSqlProvider

__all__ = [
    'local',

    'DBException', 'RowNotFound', 'MultipleRowsFound', 'TooManyRowsFound',

    'Warning', 'Error', 'InterfaceError', 'DatabaseError', 'DataError', 'OperationalError',
    'IntegrityError', 'InternalError', 'ProgrammingError', 'NotSupportedError',

    'OrmError', 'ERDiagramError', 'DBSchemaError', 'MappingError',
    'TableDoesNotExist', 'TableIsNotEmpty', 'ConstraintError', 'CacheIndexError',
    'ObjectNotFound', 'MultipleObjectsFoundError', 'TooManyObjectsFoundError', 'OperationWithDeletedObjectError',
    'TransactionError', 'ConnectionClosedError', 'TransactionIntegrityError', 'IsolationError',
    'CommitException', 'RollbackException', 'UnrepeatableReadError', 'OptimisticCheckError',
    'UnresolvableCyclicDependency', 'UnexpectedError', 'DatabaseSessionIsOver',
    'DatabaseContainsIncorrectValue', 'DatabaseContainsIncorrectEmptyValue',
    'TranslationError', 'ExprEvalError', 'PermissionError',

    'AioDatabase', 'sql_debug', 'set_sql_debug', 'sql_debugging', 'show',

    'PrimaryKey', 'Required', 'Optional', 'Set', 'Discriminator',
    'composite_key', 'composite_index',
    'flush', 'commit', 'rollback', 'db_session', 'with_transaction',

    'LongStr', 'LongUnicode', 'Json',

    'select', 'left_join', 'get', 'exists', 'delete',

    'count', 'sum', 'min', 'max', 'avg', 'distinct',

    'JOIN', 'desc', 'between', 'concat', 'coalesce', 'raw_sql',

    'buffer', 'unicode',

    'get_current_user', 'set_current_user', 'perm', 'has_perm',
    'get_user_groups', 'get_user_roles', 'get_object_labels',
    'user_groups_getter', 'user_roles_getter', 'set_debug'
]

core.debug = 0
#
debug = 0


def set_debug():
    global debug
    core.debug = 1
    debug = 1


class AioDBSessionContextManager(DBSessionContextManager):
    __slots__ = 'retry', 'retry_exceptions', 'allowed_exceptions', 'immediate', 'database', 'serializable', 'strict', \
                'optimistic', 'sql_debug', 'show_values', 'cache', 'alive', 'deepth'

    def __init__(db_session, retry=0, immediate=False, ddl=False, serializable=False, strict=False, optimistic=True,
                 retry_exceptions=(TransactionError,), allowed_exceptions=(), sql_debug=None, show_values=None):
        if retry is not 0:
            if type(retry) is not int:
                throw(TypeError, "'retry' parameter of db_session must be of integer type. Got: %s" % type(retry))
            if retry < 0:
                throw(TypeError, "'retry' parameter of db_session must not be negative. Got: %d" % retry)
            if ddl:
                throw(TypeError, "'ddl' and 'retry' parameters of db_session cannot be used together")
        if not callable(allowed_exceptions) and not callable(retry_exceptions):
            for e in allowed_exceptions:
                if e in retry_exceptions:
                    throw(TypeError, 'The same exception %s cannot be specified in both '
                                     'allowed and retry exception lists simultaneously' % e.__name__)
        db_session.retry = retry
        db_session.ddl = ddl
        db_session.serializable = serializable
        db_session.immediate = immediate or ddl or serializable or not optimistic
        db_session.strict = strict
        db_session.optimistic = optimistic and not serializable
        db_session.retry_exceptions = retry_exceptions
        db_session.allowed_exceptions = allowed_exceptions
        db_session.sql_debug = sql_debug
        db_session.show_values = show_values
        db_session.cache = None
        db_session.alive = False
        db_session.deepth = 0

    def __enter__(db_session):
        raise Exception('with is not surpport in aiopony, please use `async with` statement')

    async def __aenter__(db_session):
        if db_session.retry is not 0:
            throw(TypeError,
                  "@db_session can accept 'retry' parameter only when used as decorator and not as context manager")
        if db_session.cache is None:
            assert not db_session.deepth
            db_session.cache = AioSessionCache(local.database, db_session)
            db_session.alive = True
        elif db_session.ddl and not local.db_session.ddl:
            throw(TransactionError, 'Cannot start ddl transaction inside non-ddl transaction')
        elif db_session.serializable and not local.db_session.serializable:
            throw(TransactionError, 'Cannot start serializable transaction inside non-serializable transaction')
        if db_session.sql_debug is not None:
            local.push_debug_state(db_session.sql_debug, db_session.show_values)
        db_session.deepth += 1

    async def __aexit__(db_session, exc_type=None, exc=None, tb=None):
        db_session.deepth -= 1
        if db_session.deepth:
            return
        try:
            if exc_type is None:
                can_commit = True
            elif not callable(db_session.allowed_exceptions):
                can_commit = issubclass(exc_type, tuple(db_session.allowed_exceptions))
            else:
                assert exc is not None  # exc can be None in Python 2.6 even if exc_type is not None
                try:
                    can_commit = db_session.allowed_exceptions(exc)
                except:
                    await db_session.rollback_and_reraise(sys.exc_info())
            if can_commit:
                await db_session.commit()
                await db_session.cache.release()
            else:
                try:
                    await db_session.rollback()
                except:
                    if exc_type is None: raise  # if exc_type is not None it will be reraised outside of __exit__
        finally:
            del exc, tb
            local.user_groups_cache.clear()
            local.user_roles_cache.clear()
            db_session.cache.release_to_pool()
            db_session.cache = None
            db_session.alive = False

    def __call__(db_session, *args, **kwargs):
        if not args and not kwargs: return db_session
        if len(args) > 1:
            throw(TypeError, 'Pass only keyword arguments to db_session or use db_session as decorator')
        if not args:
            return db_session.__class__(**kwargs)
        if kwargs:
            throw(TypeError, 'Pass only keyword arguments to db_session or use db_session as decorator')
        func = args[0]
        if asyncio.iscoroutine(func) or asyncio.iscoroutinefunction(func):
            return db_session._wrap_corotine_function(func)
        else:
            throw(TypeError, 'Only coroutine can be used in Aiopony.Use `asnyc def` to define a function')

    async def commit(db_session):
        cache = db_session.cache
        if not cache:
            return
        await cache.flush()

        exceptions = []
        try:
            await cache.commit()
        except:
            exceptions.append(sys.exc_info())
        else:
            if exceptions:
                transact_reraise(PartialCommitException, exceptions)
        finally:
            del exceptions

    async def flush(db_session):
        cache = db_session.cache
        await cache.flush()

    async def rollback(db_session):
        exceptions = []
        try:
            cache = db_session.cache
            try:
                await cache.rollback()
            except:
                exceptions.append(sys.exc_info())
            if exceptions:
                transact_reraise(RollbackException, exceptions)
        finally:
            del exceptions

    async def rollback_and_reraise(db_session, exc_info):
        try:
            await db_session.rollback()
        finally:
            reraise(*exc_info)

    def _wrap_corotine_function(db_session, func):
        async def new_func(*args, **kwargs):
            nonlocal func
            try:
                old_db_session = sys._getframe(2).f_locals.get('new_db_session')
            except ValueError as e:
                pass
                # raise OrmError('@db_session is required when working with database')
            if old_db_session:
                new_db_session = old_db_session
            else:
                new_db_session = AioDBSessionContextManager(db_session.retry, db_session.immediate, db_session.ddl,
                                                    db_session.serializable, db_session.strict,
                                                    db_session.optimistic, db_session.retry_exceptions,
                                                    db_session.allowed_exceptions, db_session.sql_debug,
                                                    db_session.show_values)
            if new_db_session.sql_debug is not None:
                local.push_debug_state(new_db_session.sql_debug, new_db_session.show_values)
            exc = tb = None
            try:
                for i in xrange(new_db_session.retry + 1):
                    await new_db_session.__aenter__()
                    exc_type = exc = tb = None
                    try:
                        return await func(*args, **kwargs)
                    except:
                        exc_type, exc, tb = sys.exc_info()
                        retry_exceptions = new_db_session.retry_exceptions
                        if not callable(retry_exceptions):
                            do_retry = issubclass(exc_type, tuple(retry_exceptions))
                        else:
                            assert exc is not None  # exc can be None in Python 2.6
                            do_retry = retry_exceptions(exc)
                        if not do_retry: raise
                    finally:
                        await new_db_session.__aexit__(exc_type, exc, tb)
                reraise(exc_type, exc, tb)
            finally:
                del exc, tb
                if new_db_session.sql_debug is not None:
                    local.pop_debug_state()

        return new_func


db_session = AioDBSessionContextManager


class AioDatabase(Database):
    def __init__(self, *args, **kwargs):
        self.priority = 0
        self._insert_cache = {}

        # ER-diagram related stuff:
        self._translator_cache = {}
        self._constructed_sql_cache = {}
        self.entities = {}
        self.schema = None
        self.Entity = type.__new__(EntityMeta, 'Entity', (Entity,), {})
        self.Entity._database_ = self

        # Statistics-related stuff:
        self._global_stats = {}
        self._global_stats_lock = RLock()
        self._dblocal = DbLocal()

        self.provider = None
        if args or kwargs:
            throw(TypeError, 'AioDatabase doesn\'t support any args, please use `.bind()` method to fill in args')

    def merge_local_stats(database):
        setdefault = database._global_stats.setdefault
        # with database._global_stats_lock:
        for sql, stat in iteritems(database._dblocal.stats):
            global_stat = setdefault(sql, stat)
            if global_stat is not stat: global_stat.merge(stat)
        database._dblocal.stats.clear()
    @property
    def global_stats(database):
        # with database._global_stats_lock:
        return {sql: stat.copy() for sql, stat in iteritems(database._global_stats)}
    @property
    def global_stats_lock(database):
        deprecated(3, "global_stats_lock is deprecated, just use global_stats property without any locking")
        return database._global_stats_lock

    async def bind(self, *args, **kwargs):
        await self._bind(*args, **kwargs)

    async def _bind(self, *args, **kwargs):
        # argument 'self' cannot be named 'database', because 'database' can be in kwargs
        if self.provider is not None:
            throw(TypeError, 'Database object was already bound to %s provider' % self.provider.dialect)
        if args:
            provider, args = args[0], args[1:]
        elif 'provider' not in kwargs:
            throw(TypeError, 'Database provider is not specified')
        else:
            provider = kwargs.pop('provider')
        if isinstance(provider, type) and issubclass(provider, DBAPIProvider):
            provider_cls = provider
        else:
            if not isinstance(provider, basestring): throw(TypeError)
            provider_cls = AioMysqSqlProvider()
            await provider_cls._init_(*args, **kwargs)
        self.provider = provider_cls
        local.database = self

    async def get_connection(database):
        cache = database._get_cache()
        if not cache.in_transaction:
            cache.immediate = True
            await cache.prepare_connection_for_query_execution()
            cache.in_transaction = True
        connection = cache.connection
        assert connection is not None
        return connection

    def _get_cache(database):
        if database.provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        try:
            for i in range(2,50):
                db_session = sys._getframe(i).f_locals.get('new_db_session')
                if db_session:
                    break
        except ValueError as e:
            raise OrmError('@db_session() is required when working with database')
        assert db_session is not None
        cache = db_session.cache
        return cache

    async def disconnect(database):
        provider = database.provider
        if provider is None: return
        provider.disconnect()

    async def commit(database):
        cache = database._get_cache()
        if cache is not None:
            await cache.flush_and_commit()

    async def rollback(database):
        cache = database._get_cache()
        if cache is not None:
            try:
                await cache.rollback()
            except:
                transact_reraise(RollbackException, [sys.exc_info()])

    async def select(database, sql, globals=None, locals=None, frame_depth=0):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = await database._exec_raw_sql(sql, globals, locals, frame_depth + 3)
        max_fetch_count = options.MAX_FETCH_COUNT
        if max_fetch_count is not None:
            result = await cursor.fetchmany(max_fetch_count)
            if await cursor.fetchone() is not None: throw(TooManyRowsFound)
        else:
            result = await cursor.fetchall()
        if len(cursor.description) == 1: return [row[0] for row in result]
        row_class = type("row", (tuple,), {})
        for i, column_info in enumerate(cursor.description):
            column_name = column_info[0]
            if not is_ident(column_name): continue
            if hasattr(tuple, column_name) and column_name.startswith('__'): continue
            setattr(row_class, column_name, property(itemgetter(i)))
        return [row_class(row) for row in result]

    async def get(database, sql, globals=None, locals=None):
        rows = await database.select(sql, globals, locals, frame_depth=3)
        if not rows: throw(RowNotFound)
        if len(rows) > 1: throw(MultipleRowsFound)
        row = rows[0]
        return row

    async def exists(database, sql, globals=None, locals=None):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = await database._exec_raw_sql(sql, globals, locals, frame_depth=3)
        result = await cursor.fetchone()
        return bool(result)

    async def insert(database, table_name, returning=None, **kwargs):
        table_name = database._get_table_name(table_name)
        if database.provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        query_key = (table_name,) + tuple(kwargs)  # keys are not sorted deliberately!!
        if returning is not None: query_key = query_key + (returning,)
        cached_sql = database._insert_cache.get(query_key)
        if cached_sql is None:
            ast = ['INSERT', table_name, kwargs.keys(),
                   [['PARAM', (i, None, None)] for i in xrange(len(kwargs))], returning]
            sql, adapter = database._ast2sql(ast)
            cached_sql = sql, adapter
            database._insert_cache[query_key] = cached_sql
        else:
            sql, adapter = cached_sql
        arguments = adapter(values_list(kwargs))  # order of values same as order of keys
        if returning is not None:
            return await database._exec_sql(sql, arguments, returning_id=True, start_transaction=True)
        cursor = await database._exec_sql(sql, arguments, start_transaction=True)
        return getattr(cursor, 'lastrowid', None)

    async def _exec_sql(database, sql, arguments=None, returning_id=False, start_transaction=False):
        cache = database._get_cache()
        if start_transaction: cache.immediate = True
        connection = await cache.prepare_connection_for_query_execution()
        cursor = await connection.cursor()
        if debug: log_sql(sql, arguments)
        provider = database.provider
        t = time()
        try:
            new_id = await provider.execute(cursor, sql, arguments, returning_id)
        except Exception as e:
            connection = await cache.reconnect(e)
            cursor = await connection.cursor()
            if debug: log_sql(sql, arguments)
            t = time()
            new_id = await provider.execute(cursor, sql, arguments, returning_id)
        if cache.immediate: cache.in_transaction = True
        database._update_local_stat(sql, t)
        if not returning_id: return cursor
        return new_id

    async def generate_mapping(database, filename=None, check_tables=True, create_tables=False):
        provider = database.provider
        if provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        if database.schema: throw(MappingError, 'Mapping was already generated')
        if filename is not None: throw(NotImplementedError)
        schema = database.schema = provider.dbschema_cls(provider)
        entities = list(sorted(database.entities.values(), key=attrgetter('_id_')))
        for entity in entities:
            entity._resolve_attr_types_()
        for entity in entities:
            entity._link_reverse_attrs_()

        def get_columns(table, column_names):
            column_dict = table.column_dict
            return tuple(column_dict[name] for name in column_names)

        for entity in entities:
            entity._get_pk_columns_()
            table_name = entity._table_

            is_subclass = entity._root_ is not entity
            if is_subclass:
                if table_name is not None: throw(NotImplementedError)
                table_name = entity._root_._table_
                entity._table_ = table_name
            elif table_name is None:
                table_name = provider.get_default_entity_table_name(entity)
                entity._table_ = table_name
            else:
                assert isinstance(table_name, (basestring, tuple))

            table = schema.tables.get(table_name)
            if table is None:
                table = schema.add_table(table_name)
            elif table.entities:
                for e in table.entities:
                    if e._root_ is not entity._root_:
                        throw(MappingError, "Entities %s and %s cannot be mapped to table %s "
                                            "because they don't belong to the same hierarchy"
                              % (e, entity, table_name))
            table.entities.add(entity)

            for attr in entity._new_attrs_:
                if attr.is_collection:
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    reverse = attr.reverse
                    if not reverse.is_collection:  # many-to-one:
                        if attr.table is not None:
                            throw(MappingError,
                                  "Parameter 'table' is not allowed for many-to-one attribute %s" % attr)
                        elif attr.columns:
                            throw(NotImplementedError,
                                  "Parameter 'column' is not allowed for many-to-one attribute %s" % attr)
                        continue
                    # many-to-many:
                    if not isinstance(reverse, Set): throw(NotImplementedError)
                    if attr.entity.__name__ > reverse.entity.__name__: continue
                    if attr.entity is reverse.entity and attr.name > reverse.name: continue

                    if attr.table:
                        if not reverse.table:
                            reverse.table = attr.table
                        elif reverse.table != attr.table:
                            throw(MappingError, "Parameter 'table' for %s and %s do not match" % (attr, reverse))
                        table_name = attr.table
                    elif reverse.table:
                        table_name = attr.table = reverse.table
                    else:
                        table_name = provider.get_default_m2m_table_name(attr, reverse)

                    m2m_table = schema.tables.get(table_name)
                    if m2m_table is not None:
                        if not attr.table:
                            seq_counter = itertools.count(2)
                            while m2m_table is not None:
                                new_table_name = table_name + '_%d' % next(seq_counter)
                                m2m_table = schema.tables.get(new_table_name)
                            table_name = new_table_name
                        elif m2m_table.entities or m2m_table.m2m:
                            if isinstance(table_name, tuple): table_name = '.'.join(table_name)
                            throw(MappingError, "Table name '%s' is already in use" % table_name)
                        else:
                            throw(NotImplementedError)
                    attr.table = reverse.table = table_name
                    m2m_table = schema.add_table(table_name)
                    m2m_columns_1 = attr.get_m2m_columns(is_reverse=False)
                    m2m_columns_2 = reverse.get_m2m_columns(is_reverse=True)
                    if m2m_columns_1 == m2m_columns_2: throw(MappingError,
                                                             'Different column names should be specified for attributes %s and %s' % (
                                                                 attr, reverse))
                    assert len(m2m_columns_1) == len(reverse.converters)
                    assert len(m2m_columns_2) == len(attr.converters)
                    for column_name, converter in izip(m2m_columns_1 + m2m_columns_2,
                                                       reverse.converters + attr.converters):
                        m2m_table.add_column(column_name, converter.get_sql_type(), converter, True)
                    m2m_table.add_index(None, tuple(m2m_table.column_list), is_pk=True)
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                else:
                    if attr.is_required:
                        pass
                    elif not attr.is_string:
                        if attr.nullable is False:
                            throw(TypeError, 'Optional attribute with non-string type %s must be nullable' % attr)
                        attr.nullable = True
                    elif entity._database_.provider.dialect == 'Oracle':
                        if attr.nullable is False: throw(ERDiagramError,
                                                         'In Oracle, optional string attribute %s must be nullable' % attr)
                        attr.nullable = True

                    columns = attr.get_columns()  # initializes attr.converters
                    if not attr.reverse and attr.default is not None:
                        assert len(attr.converters) == 1
                        if not callable(attr.default): attr.default = attr.validate(attr.default)
                    assert len(columns) == len(attr.converters)
                    if len(columns) == 1:
                        converter = attr.converters[0]
                        table.add_column(columns[0], converter.get_sql_type(attr),
                                         converter, not attr.nullable, attr.sql_default)
                    elif columns:
                        if attr.sql_type is not None: throw(NotImplementedError,
                                                            'sql_type cannot be specified for composite attribute %s' % attr)
                        for (column_name, converter) in izip(columns, attr.converters):
                            table.add_column(column_name, converter.get_sql_type(), converter, not attr.nullable)
                    else:
                        pass  # virtual attribute of one-to-one pair
            entity._attrs_with_columns_ = [attr for attr in entity._attrs_
                                           if not attr.is_collection and attr.columns]
            if not table.pk_index:
                if len(entity._pk_columns_) == 1 and entity._pk_attrs_[0].auto:
                    is_pk = "auto"
                else:
                    is_pk = True
                table.add_index(None, get_columns(table, entity._pk_columns_), is_pk)
            for index in entity._indexes_:
                if index.is_pk: continue
                column_names = []
                attrs = index.attrs
                for attr in attrs: column_names.extend(attr.columns)
                index_name = attrs[0].index if len(attrs) == 1 else None
                table.add_index(index_name, get_columns(table, column_names), is_unique=index.is_unique)
            columns = []
            columns_without_pk = []
            converters = []
            converters_without_pk = []
            for attr in entity._attrs_with_columns_:
                columns.extend(attr.columns)  # todo: inheritance
                converters.extend(attr.converters)
                if not attr.is_pk:
                    columns_without_pk.extend(attr.columns)
                    converters_without_pk.extend(attr.converters)
            entity._columns_ = columns
            entity._columns_without_pk_ = columns_without_pk
            entity._converters_ = converters
            entity._converters_without_pk_ = converters_without_pk
        for entity in entities:
            table = schema.tables[entity._table_]
            for attr in entity._new_attrs_:
                if attr.is_collection:
                    reverse = attr.reverse
                    if not reverse.is_collection: continue
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    if not isinstance(reverse, Set): throw(NotImplementedError)
                    m2m_table = schema.tables[attr.table]
                    parent_columns = get_columns(table, entity._pk_columns_)
                    child_columns = get_columns(m2m_table, reverse.columns)
                    m2m_table.add_foreign_key(None, child_columns, table, parent_columns, attr.index)
                    if attr.symmetric:
                        child_columns = get_columns(m2m_table, attr.reverse_columns)
                        m2m_table.add_foreign_key(None, child_columns, table, parent_columns)
                elif attr.reverse and attr.columns:
                    rentity = attr.reverse.entity
                    parent_table = schema.tables[rentity._table_]
                    parent_columns = get_columns(parent_table, rentity._pk_columns_)
                    child_columns = get_columns(table, attr.columns)
                    table.add_foreign_key(None, child_columns, parent_table, parent_columns, attr.index)
                elif attr.index and attr.columns:
                    columns = tuple(imap(table.column_dict.__getitem__, attr.columns))
                    table.add_index(attr.index, columns, is_unique=attr.is_unique)
            entity._initialize_bits_()

        if create_tables:
            await database.create_tables(check_tables)
        elif check_tables:
            await database.check_tables()

    @db_session(ddl=True)
    async def drop_table(database, table_name, if_exists=False, with_all_data=False):
        table_name = database._get_table_name(table_name)
        await database._drop_tables([table_name], if_exists, with_all_data, try_normalized=True)

    @db_session(ddl=True)
    async def drop_all_tables(database, with_all_data=False):
        # db_session.ddl = True
        if database.schema is None: throw(ERDiagramError, 'No mapping was generated for the database')
        await database._drop_tables(database.schema.tables, True, with_all_data)

    def release_to_pool(database, con):
        database.provider.release_to_pool(con)

    async def _drop_tables(database, table_names, if_exists, with_all_data, try_normalized=False):
        cache = database._get_cache()
        connection = await cache.prepare_connection_for_query_execution()
        provider = database.provider
        existed_tables = []
        for table_name in table_names:
            table_name = database._get_table_name(table_name)
            if await provider.table_exists(connection, table_name):
                existed_tables.append(table_name)
            elif not if_exists:
                if try_normalized:
                    normalized_table_name = provider.normalize_name(table_name)
                    if normalized_table_name != table_name \
                            and (await provider.table_exists(connection, normalized_table_name)):
                        throw(TableDoesNotExist, 'Table %s does not exist (probably you meant table %s)'
                              % (table_name, normalized_table_name))
                throw(TableDoesNotExist, 'Table %s does not exist' % table_name)
        if not with_all_data:
            for table_name in existed_tables:
                if await provider.table_has_data(connection, table_name):
                    throw(TableIsNotEmpty, 'Cannot drop table %s because it is not empty. Specify option '
                                           'with_all_data=True if you want to drop table with all data' % table_name)
        for table_name in existed_tables:
            if debug: log_orm('DROPPING TABLE %s' % table_name)
            await provider.drop_table(connection, table_name)

    @db_session(ddl=True)
    async def create_tables(database, check_tables=False):
        cache = database._get_cache()
        if database.schema is None: throw(MappingError, 'No mapping was generated for the database')
        connection = await cache.prepare_connection_for_query_execution()
        await database.schema.create_tables(database.provider, connection)
        if check_tables: await database.schema.check_tables(database.provider, connection)

    @db_session()
    async def check_tables(database):
        cache = database._get_cache()
        if database.schema is None: throw(MappingError, 'No mapping was generated for the database')
        connection = await cache.prepare_connection_for_query_execution()
        await database.schema.check_tables(database.provider, connection)


class AioSessionCache(SessionCache):
    def __init__(cache, database, db_session=None):
        cache.is_alive = True
        cache.num = next(num_counter)
        cache.database = database
        cache.objects = set()
        cache.indexes = defaultdict(dict)
        cache.seeds = defaultdict(set)
        cache.max_id_cache = {}
        cache.collection_statistics = {}
        cache.for_update = set()
        cache.noflush_counter = 0
        cache.modified_collections = defaultdict(set)
        cache.objects_to_save = []
        cache.saved_objects = []
        cache.query_results = {}
        cache.modified = False
        cache.db_session = db_session
        cache.immediate = db_session is not None and db_session.immediate
        cache.connection = None
        cache.in_transaction = False
        cache.saved_fk_state = None
        cache.perm_cache = defaultdict(lambda: defaultdict(dict))  # user -> perm -> cls_or_attr_or_obj -> bool
        cache.user_roles_cache = defaultdict(dict)  # user -> obj -> roles
        cache.obj_labels_cache = {}  # obj -> labels

    async def connect(cache):
        assert cache.connection is None
        if cache.in_transaction:
            throw(ConnectionClosedError, 'Transaction cannot be continued because database connection failed')
        provider = cache.database.provider
        connection = await provider.connect()
        try:
            await provider.set_transaction_mode(connection, cache)  # can set cache.in_transaction
        except:
            provider.drop(connection, cache)
            raise
        cache.connection = connection
        return connection

    async def reconnect(cache, exc):
        provider = cache.database.provider
        if exc is not None:
            exc = getattr(exc, 'original_exc', exc)
            if not provider.should_reconnect(exc): reraise(*sys.exc_info())
            if debug: log_orm('CONNECTION FAILED: %s' % exc)
            connection = cache.connection
            assert connection is not None
            cache.connection = None
            provider.drop(connection, cache)
        else:
            assert cache.connection is None
        return await cache.connect()

    async def prepare_connection_for_query_execution(cache):
        connection = cache.connection
        if connection is None:
            connection = await cache.connect()
        elif cache.immediate and not cache.in_transaction:
            provider = cache.database.provider
            try:
                await provider.set_transaction_mode(connection, cache)  # can set cache.in_transaction
            except Exception as e:
                connection = await cache.reconnect(e)
        if not cache.noflush_counter and cache.modified: await cache.flush()
        return connection

    async def flush_and_commit(cache):
        try:
            await cache.flush()
        except:
            await cache.rollback()
            raise
        try:
            await cache.commit()
        except:
            transact_reraise(CommitException, [sys.exc_info()])

    async def commit(cache):
        assert cache.is_alive
        try:
            if cache.modified:
                await cache.flush()
            if cache.in_transaction:
                assert cache.connection is not None
                await cache.database.provider.commit(cache.connection, cache)
            cache.for_update.clear()
            cache.query_results.clear()
            cache.max_id_cache.clear()
            cache.immediate = True
        except:
            await cache.rollback()
            raise

    async def rollback(cache):
        await cache.close(rollback=True)

    async def flush(cache):
        if cache.noflush_counter: return
        assert cache.is_alive
        assert not cache.saved_objects
        if not cache.immediate: cache.immediate = True
        for i in xrange(50):
            if not cache.modified: return

            with cache.flush_disabled():
                for obj in cache.objects_to_save:  # can grow during iteration
                    if obj is not None: obj._before_save_()

                cache.query_results.clear()
                modified_m2m = cache._calc_modified_m2m()
                for attr, (added, removed) in iteritems(modified_m2m):
                    if not removed: continue
                    attr.remove_m2m(removed)
                for obj in cache.objects_to_save:
                    if obj is not None: await obj._save_()
                for attr, (added, removed) in iteritems(modified_m2m):
                    if not added: continue
                    await attr.add_m2m(added)

            cache.max_id_cache.clear()
            cache.modified_collections.clear()
            cache.objects_to_save[:] = ()
            cache.modified = False

            cache.call_after_save_hooks()
        else:
            if cache.modified: throw(TransactionError,
                                     'Recursion depth limit reached in obj._after_save_() call')

    async def close(cache, rollback=True):
        assert cache.is_alive
        if not rollback: assert not cache.in_transaction
        database = cache.database
        cache.is_alive = False
        provider = database.provider
        connection = cache.connection
        if connection is None: return
        cache.connection = None

        try:
            if rollback:
                try:
                    await provider.rollback(connection, cache)
                except:
                    provider.release_to_pool(connection, cache)
                    raise
            provider.release_to_pool(connection)
        finally:
            cache.objects = cache.objects_to_save = cache.saved_objects = cache.query_results \
                = cache.indexes = cache.seeds = cache.for_update = cache.max_id_cache \
                = cache.modified_collections = cache.collection_statistics = None

            db_session = cache.db_session
            if db_session and db_session.strict:
                for obj in cache.objects:
                    obj._vals_ = obj._dbvals_ = obj._session_cache_ = None
                cache.perm_cache = cache.user_roles_cache = cache.obj_labels_cache = None

    async def release(cache):
        await cache.close(rollback=False)

    def release_to_pool(cache):
        database = cache.database
        if cache.connection is not None:
            database.release_to_pool(cache.connection)


class DbLocal:
    def __init__(dblocal):
        dblocal.stats = {}
        dblocal.last_sql = None


async def Attr_load(attr, obj):
    if not obj._session_cache_.is_alive:
        throw(DatabaseSessionIsOver,
              'Cannot load attribute %s.%s: the database session is over' % (safe_repr(obj), attr.name))
    if not attr.columns:
        reverse = attr.reverse
        assert reverse is not None and reverse.columns
        dbval = await reverse.entity._find_in_db_({reverse: obj})
        if dbval is None:
            obj._vals_[attr] = None
        else:
            assert obj._vals_[attr] == dbval
        return dbval

    if attr.lazy:
        entity = attr.entity
        database = entity._database_
        if not attr.lazy_sql_cache:
            select_list = ['ALL'] + [['COLUMN', None, column] for column in attr.columns]
            from_list = ['FROM', [None, 'TABLE', entity._table_]]
            pk_columns = entity._pk_columns_
            pk_converters = entity._pk_converters_
            criteria_list = [[converter.EQ, ['COLUMN', None, column], ['PARAM', (i, None, None), converter]]
                             for i, (column, converter) in enumerate(izip(pk_columns, pk_converters))]
            sql_ast = ['SELECT', select_list, from_list, ['WHERE'] + criteria_list]
            sql, adapter = database._ast2sql(sql_ast)
            offsets = tuple(xrange(len(attr.columns)))
            attr.lazy_sql_cache = sql, adapter, offsets
        else:
            sql, adapter, offsets = attr.lazy_sql_cache
        arguments = adapter(obj._get_raw_pkval_())
        cursor = await database._exec_sql(sql, arguments)
        row = await cursor.fetchone()
        dbval = attr.parse_value(row, offsets)
        attr.db_set(obj, dbval)
    else:
        await obj._load_()
    return obj._vals_[attr]


def Attr___get__(attr, obj, cls=None):
    # async def _get_attr(attr, obj, cls):
    if obj is None: return attr
    if attr.pk_offset is not None:
        # return await attr.get(obj)
        return attr.get(obj)
    # value = await attr.get(obj)
    value = attr.get(obj)
    bit = obj._bits_except_volatile_[attr]
    wbits = obj._wbits_
    if wbits is not None and not wbits & bit: obj._rbits_ |= bit
    return value


def Attr_get(attr, obj):
    if attr.pk_offset is None and obj._status_ in ('deleted', 'cancelled'):
        throw_object_was_deleted(obj)
    vals = obj._vals_
    if vals is None: throw_db_session_is_over(obj, attr)
    val = vals[attr] if attr in vals else attr.load(obj)
    if val is not None and attr.reverse and val._subclasses_ and val._status_ not in ('deleted', 'cancelled'):
        seeds = obj._session_cache_.seeds[val._pk_attrs_]
        if val in seeds:
            pass
            # await val._load_()
    return val


def Attr___set__(attr, obj, new_val, undo_funcs=None):
    cache = obj._session_cache_
    if not cache.is_alive: throw(DatabaseSessionIsOver,
                                 'Cannot assign new value to attribute %s.%s: the database session is over' % (
                                     safe_repr(obj), attr.name))
    if obj._status_ in del_statuses: throw_object_was_deleted(obj)
    reverse = attr.reverse
    new_val = attr.validate(new_val, obj, from_db=False)
    if attr.pk_offset is not None:
        pkval = obj._pkval_
        if pkval is None:
            pass
        elif obj._pk_is_composite_:
            if new_val == pkval[attr.pk_offset]: return
        elif new_val == pkval:
            return
        throw(TypeError, 'Cannot change value of primary key')
    with cache.flush_disabled():
        old_val = obj._vals_.get(attr, NOT_LOADED)
        if old_val is NOT_LOADED and reverse and not reverse.is_collection:
            old_val = attr.load(obj)
        status = obj._status_
        wbits = obj._wbits_
        bit = obj._bits_[attr]
        objects_to_save = cache.objects_to_save
        objects_to_save_needs_undo = False
        if wbits is not None and bit:
            obj._wbits_ = wbits | bit
            if status != 'modified':
                assert status in ('loaded', 'inserted', 'updated')
                assert obj._save_pos_ is None
                obj._status_ = 'modified'
                obj._save_pos_ = len(objects_to_save)
                objects_to_save.append(obj)
                objects_to_save_needs_undo = True
                cache.modified = True
        if not attr.reverse and not attr.is_part_of_unique_index:
            obj._vals_[attr] = new_val
            return
        is_reverse_call = undo_funcs is not None
        if not is_reverse_call: undo_funcs = []
        undo = []

        def undo_func():
            obj._status_ = status
            obj._wbits_ = wbits
            if objects_to_save_needs_undo:
                assert objects_to_save
                obj2 = objects_to_save.pop()
                assert obj2 is obj and obj._save_pos_ == len(objects_to_save)
                obj._save_pos_ = None

            if old_val is NOT_LOADED:
                obj._vals_.pop(attr)
            else:
                obj._vals_[attr] = old_val
            for cache_index, old_key, new_key in undo:
                if new_key is not None: del cache_index[new_key]
                if old_key is not None: cache_index[old_key] = obj

        undo_funcs.append(undo_func)
        if old_val == new_val: return
        try:
            if attr.is_unique:
                cache.update_simple_index(obj, attr, old_val, new_val, undo)
            get_val = obj._vals_.get
            for attrs, i in attr.composite_keys:
                vals = [get_val(a) for a in attrs]  # In Python 2 var name leaks into the function scope!
                prev_vals = tuple(vals)
                vals[i] = new_val
                new_vals = tuple(vals)
                cache.update_composite_index(obj, attrs, prev_vals, new_vals, undo)

            obj._vals_[attr] = new_val

            if not reverse:
                pass
            elif not is_reverse_call:
                attr.update_reverse(obj, old_val, new_val, undo_funcs)
            elif old_val not in (None, NOT_LOADED):
                if not reverse.is_collection:
                    if new_val is not None:
                        if reverse.is_required:
                            throw(ConstraintError,
                                  'Cannot unlink %r from previous %s object, because %r attribute is required'
                                  % (old_val, obj, reverse))
                        reverse.__set__(old_val, None, undo_funcs)
                elif isinstance(reverse, Set):
                    reverse.reverse_remove((old_val,), obj, undo_funcs)
                else:
                    throw(NotImplementedError)
        except:
            if not is_reverse_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise
        # pass

    # asyncio.ensure_future(_set_attr(obj, new_val, undo_funcs))


def Attr_validate(attr, val, obj=None, entity=None, from_db=False):
    if val is None:
        if not attr.nullable and not from_db and not attr.is_required:
            # for required attribute the exception will be thrown later with another message
            throw(ValueError, 'Attribute %s cannot be set to None' % attr)
        return val
    assert val is not NOT_LOADED
    if val is DEFAULT:
        default = attr.default
        if default is None: return None
        if callable(default):
            val = default()
        else:
            val = default

    if entity is not None:
        pass
    elif obj is not None:
        entity = obj.__class__
    else:
        entity = attr.entity

    reverse = attr.reverse
    if not reverse:
        if isinstance(val, Entity): throw(TypeError, 'Attribute %s must be of %s type. Got: %s'
                                          % (attr, attr.py_type.__name__, val))
        if not attr.converters:
            return val if type(val) is attr.py_type else attr.py_type(val)
        if len(attr.converters) != 1: throw(NotImplementedError)
        converter = attr.converters[0]
        if converter is not None:
            try:
                if from_db: return converter.sql2py(val)
                val = converter.validate(val, obj)
            except UnicodeDecodeError as e:
                throw(ValueError, 'Value for attribute %s cannot be converted to %s: %s'
                      % (attr, unicode.__name__, truncate_repr(val)))
    else:
        rentity = reverse.entity
        if not isinstance(val, rentity):
            vals = val if type(val) is tuple else (val,)
            if len(vals) != len(rentity._pk_columns_):
                throw(TypeError, 'Invalid number of columns were specified for attribute %s. Expected: %d, got: %d'
                      % (attr, len(rentity._pk_columns_), len(vals)))
            try:
                val = rentity._get_by_raw_pkval_(vals, from_db=from_db)
            except TypeError:
                throw(TypeError, 'Attribute %s must be of %s type. Got: %r'
                      % (attr, rentity.__name__, val))
        # else:
        #     pass
        # if obj is not None and obj._status_ is not None:
        #     cache = obj._session_cache_
        # else:
        #     cache = obj.db_session.cache
        # if cache is not val._session_cache_:
        #     pass
        # throw(TransactionError, 'An attempt to mix objects belonging to different transactions')
    if attr.py_check is not None and not attr.py_check(val):
        throw(ValueError, 'Check for attribute %s failed. Value: %s' % (attr, truncate_repr(val)))
    return val


Attribute.__get__ = Attr___get__
# Attribute.__set__ = Attr___set__
Attribute.get = Attr_get
Attribute.load = Attr_load
Attribute.validate = Attr_validate


async def Set_load(attr, obj, items=None):
    cache = obj._session_cache_
    if not cache.is_alive:
        throw(DatabaseSessionIsOver, 'Cannot load collection %s.%s: the database session is over' % (
            safe_repr(obj), attr.name))
    assert obj._status_ not in del_statuses
    setdata = obj._vals_.get(attr)
    if setdata is None:
        setdata = obj._vals_[attr] = SetData()
    elif setdata.is_fully_loaded:
        return setdata
    entity = attr.entity
    reverse = attr.reverse
    rentity = reverse.entity
    if not reverse: throw(NotImplementedError)
    database = obj._database_
    if cache is not database._get_cache():
        throw(TransactionError, "Transaction of object %s belongs to different thread")

    if items:
        if not reverse.is_collection:
            items = {item for item in items if reverse not in item._vals_}
        else:
            items = set(items)
            items -= setdata
            if setdata.removed: items -= setdata.removed
        if not items: return setdata

    if items and (attr.lazy or not setdata):
        items = list(items)
        if not reverse.is_collection:
            sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(items))
            arguments = adapter(items)
            cursor = await database._exec_sql(sql, arguments)
            items = await rentity._fetch_objects(cursor, attr_offsets)
            return setdata

        sql, adapter = attr.construct_sql_m2m(1, len(items))
        items.append(obj)
        arguments = adapter(items)
        cursor = await database._exec_sql(sql, arguments)
        loaded_items = {rentity._get_by_raw_pkval_(row) for row in (await cursor.fetchall())}
        setdata |= loaded_items
        reverse.db_reverse_add(loaded_items, obj)
        return setdata

    counter = cache.collection_statistics.setdefault(attr, 0)
    nplus1_threshold = attr.nplus1_threshold
    prefetching = options.PREFETCHING and not attr.lazy and nplus1_threshold is not None \
                  and (counter >= nplus1_threshold or cache.noflush_counter)

    objects = [obj]
    setdata_list = [setdata]
    if prefetching:
        pk_index = cache.indexes[entity._pk_attrs_]
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        for obj2 in itervalues(pk_index):
            if obj2 is obj: continue
            if obj2._status_ in created_or_deleted_statuses: continue
            setdata2 = obj2._vals_.get(attr)
            if setdata2 is None:
                setdata2 = obj2._vals_[attr] = SetData()
            elif setdata2.is_fully_loaded:
                continue
            objects.append(obj2)
            setdata_list.append(setdata2)
            if len(objects) >= max_batch_size: break

    if not reverse.is_collection:
        sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(objects), reverse)
        arguments = adapter(objects)
        cursor = await database._exec_sql(sql, arguments)
        items = await rentity._fetch_objects(cursor, attr_offsets)
    else:
        sql, adapter = attr.construct_sql_m2m(len(objects))
        arguments = adapter(objects)
        cursor = await database._exec_sql(sql, arguments)
        pk_len = len(entity._pk_columns_)
        d = {}
        if len(objects) > 1:
            for row in await cursor.fetchall():
                obj2 = entity._get_by_raw_pkval_(row[:pk_len])
                item = rentity._get_by_raw_pkval_(row[pk_len:])
                items = d.get(obj2)
                if items is None: items = d[obj2] = set()
                items.add(item)
        else:
            d[obj] = {rentity._get_by_raw_pkval_(row) for row in cursor.fetchall()}
        for obj2, items in iteritems(d):
            setdata2 = obj2._vals_.get(attr)
            if setdata2 is None:
                setdata2 = obj._vals_[attr] = SetData()
            else:
                phantoms = setdata2 - items
                if setdata2.added: phantoms -= setdata2.added
                if phantoms: throw(UnrepeatableReadError,
                                   'Phantom object %s disappeared from collection %s.%s'
                                   % (safe_repr(phantoms.pop()), safe_repr(obj), attr.name))
            items -= setdata2
            if setdata2.removed: items -= setdata2.removed
            setdata2 |= items
            reverse.db_reverse_add(items, obj2)

    for setdata2 in setdata_list:
        setdata2.is_fully_loaded = True
        setdata2.absent = None
        setdata2.count = len(setdata2)
    cache.collection_statistics[attr] = counter + 1
    return setdata


async def Set_remove_m2m(attr, removed):
    assert removed
    entity = attr.entity
    database = entity._database_
    cached_sql = attr.cached_remove_m2m_sql
    if cached_sql is None:
        reverse = attr.reverse
        where_list = ['WHERE']
        if attr.symmetric:
            columns = attr.columns + attr.reverse_columns
            converters = attr.converters + attr.converters
        else:
            columns = reverse.columns + attr.columns
            converters = reverse.converters + attr.converters
        for i, (column, converter) in enumerate(izip(columns, converters)):
            where_list.append([converter.EQ, ['COLUMN', None, column], ['PARAM', (i, None, None), converter]])
        from_ast = ['FROM', [None, 'TABLE', attr.table]]
        sql_ast = ['DELETE', None, from_ast, where_list]
        sql, adapter = database._ast2sql(sql_ast)
        attr.cached_remove_m2m_sql = sql, adapter
    else:
        sql, adapter = cached_sql
    arguments_list = [adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                      for obj, robj in removed]
    await database._exec_sql(sql, arguments_list)


async def Set_add_m2m(attr, added):
    assert added
    entity = attr.entity
    database = entity._database_
    cached_sql = attr.cached_add_m2m_sql
    if cached_sql is None:
        reverse = attr.reverse
        if attr.symmetric:
            columns = attr.columns + attr.reverse_columns
            converters = attr.converters + attr.converters
        else:
            columns = reverse.columns + attr.columns
            converters = reverse.converters + attr.converters
        params = [['PARAM', (i, None, None), converter] for i, converter in enumerate(converters)]
        sql_ast = ['INSERT', attr.table, columns, params]
        sql, adapter = database._ast2sql(sql_ast)
        attr.cached_add_m2m_sql = sql, adapter
    else:
        sql, adapter = cached_sql
    arguments_list = [adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                      for obj, robj in added]
    await database._exec_sql(sql, arguments_list)


@db_session(ddl=True)
async def Set_drop_table(attr, with_all_data=False):
    if attr.reverse.is_collection:
        table_name = attr.table
    else:
        table_name = attr.entity._table_
    await attr.entity._database_._drop_tables([table_name], True, with_all_data)


def Set_validate(attr, val, obj=None, entity=None, from_db=False):
    assert val is not NOT_LOADED
    if val is DEFAULT: return set()
    reverse = attr.reverse
    if val is None: throw(ValueError, 'A single %(cls)s instance or %(cls)s iterable is expected. '
                                      'Got: None' % dict(cls=reverse.entity.__name__))
    if entity is not None:
        pass
    elif obj is not None:
        entity = obj.__class__
    else:
        entity = attr.entity
    if not reverse: throw(NotImplementedError)
    if isinstance(val, reverse.entity):
        items = set((val,))
    else:
        rentity = reverse.entity
        try:
            items = set(val)
        except TypeError:
            throw(TypeError, 'Item of collection %s.%s must be an instance of %s. Got: %r'
                  % (entity.__name__, attr.name, rentity.__name__, val))
        for item in items:
            if not isinstance(item, rentity):
                throw(TypeError, 'Item of collection %s.%s must be an instance of %s. Got: %r'
                      % (entity.__name__, attr.name, rentity.__name__, item))
    if obj is not None and obj._status_ is not None:
        cache = obj._session_cache_
    else:
        cache = entity._database_._get_cache()
    for item in items:
        if item.db_session.cache is not cache:
            # pass
            throw(TransactionError, 'An attempt to mix objects belonging to different transactions')
    return items

def Set__get__(attr, obj, cls=None):
    if obj is None: return attr
    if obj._status_ in del_statuses: throw_object_was_deleted(obj)
    rentity = attr.py_type
    wrapper_class = rentity._get_set_wrapper_subclass_()
    return wrapper_class(obj, attr)

def Set___get__(attr, obj, cls=None):
    if obj is None: return attr
    if obj._status_ in del_statuses: throw_object_was_deleted(obj)
    rentity = attr.py_type
    attr_reverse = attr.reverse
    target = [k for k, v in rentity._adict_.items() if v == attr_reverse]
    assert len(target) == 1
    # return select(m for m in rentity if m.student == obj)
    return select(m for m in rentity if getattr(m, target[0]) == obj)


async def Set_copy(attr, obj):
    if obj._status_ in del_statuses: throw_object_was_deleted(obj)
    if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
    setdata = obj._vals_.get(attr)
    if setdata is None or not setdata.is_fully_loaded:
        setdata = await attr.load(obj)
    reverse = attr.reverse
    if not reverse.is_collection and reverse.pk_offset is None:
        added = setdata.added or ()
        for item in setdata:
            if item in added: continue
            bit = item._bits_except_volatile_[reverse]
            assert item._wbits_ is not None
            if not item._wbits_ & bit: item._rbits_ |= bit
    return set(setdata)


Set.load = Set_load
Set.remove_m2m = Set_remove_m2m
Set.add_m2m = Set_add_m2m
Set.drop_table = Set_drop_table
Set.validate = Set_validate
Set.__get__ = Set___get__
Set.copy = Set_copy
Set._get = Set__get__


class AioSetInstance(SetInstance):
    async def iter(wrapper):
        return await wrapper.copy()

    async def copy(wrapper):
        return await wrapper._attr_.copy(wrapper._obj_)

    def __str__(wrapper):
        cache = wrapper._obj_._session_cache_
        if cache is None or not cache.is_alive:
            content = '...'
        else:
            content = ', '.join(imap(str, wrapper))
        return '%s([%s])' % (wrapper.__class__.__name__, content)

    def is_empty(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None:
            setdata = obj._vals_[attr] = SetData()
        elif setdata.is_fully_loaded:
            return not setdata
        elif setdata:
            return False
        elif setdata.count is not None:
            return not setdata.count
        entity = attr.entity
        reverse = attr.reverse
        rentity = reverse.entity
        database = entity._database_
        cached_sql = attr.cached_empty_sql
        if cached_sql is None:
            where_list = ['WHERE']
            for i, (column, converter) in enumerate(izip(reverse.columns, reverse.converters)):
                where_list.append([converter.EQ, ['COLUMN', None, column], ['PARAM', (i, None, None), converter]])
            if not reverse.is_collection:
                table_name = rentity._table_
                select_list, attr_offsets = rentity._construct_select_clause_()
            else:
                table_name = attr.table
                select_list = ['ALL'] + [['COLUMN', None, column] for column in attr.columns]
                attr_offsets = None
            sql_ast = ['SELECT', select_list, ['FROM', [None, 'TABLE', table_name]],
                       where_list, ['LIMIT', ['VALUE', 1]]]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_empty_sql = sql, adapter, attr_offsets
        else:
            sql, adapter, attr_offsets = cached_sql
        arguments = adapter(obj._get_raw_pkval_())
        cursor = database._exec_sql(sql, arguments)
        if reverse.is_collection:
            row = cursor.fetchone()
            if row is not None:
                loaded_item = rentity._get_by_raw_pkval_(row)
                setdata.add(loaded_item)
                reverse.db_reverse_add((loaded_item,), obj)
        else:
            rentity._fetch_objects(cursor, attr_offsets)
        if setdata: return False
        setdata.is_fully_loaded = True
        setdata.absent = None
        setdata.count = 0
        return True

    def __len__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None or not setdata.is_fully_loaded: setdata = attr.load(obj)
        return len(setdata)

    def count(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        cache = obj._session_cache_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None:
            setdata = obj._vals_[attr] = SetData()
        elif setdata.count is not None:
            return setdata.count
        if cache is None or not cache.is_alive: throw_db_session_is_over('read value of', obj, attr)
        entity = attr.entity
        reverse = attr.reverse
        database = entity._database_
        cached_sql = attr.cached_count_sql
        if cached_sql is None:
            where_list = ['WHERE']
            for i, (column, converter) in enumerate(izip(reverse.columns, reverse.converters)):
                where_list.append([converter.EQ, ['COLUMN', None, column], ['PARAM', (i, None, None), converter]])
            if not reverse.is_collection:
                table_name = reverse.entity._table_
            else:
                table_name = attr.table
            sql_ast = ['SELECT', ['AGGREGATES', ['COUNT', 'ALL']],
                       ['FROM', [None, 'TABLE', table_name]], where_list]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_count_sql = sql, adapter
        else:
            sql, adapter = cached_sql
        arguments = adapter(obj._get_raw_pkval_())
        with cache.flush_disabled():
            cursor = database._exec_sql(sql, arguments)
        setdata.count = cursor.fetchone()[0]
        if setdata.added: setdata.count += len(setdata.added)
        if setdata.removed: setdata.count -= len(setdata.removed)
        return setdata.count

    async def _iter_(wrapper):
        cls = await wrapper.copy()
        return iter(cls)

    def __iter__(wrapper):
        return wrapper._iter_()

    def __eq__(wrapper, other):
        if isinstance(other, SetInstance):
            if wrapper._obj_ is other._obj_ and wrapper._attr_ is other._attr_:
                return True
            else:
                other = other.copy()
        elif not isinstance(other, set):
            other = set(other)
        items = wrapper.copy()
        return items == other

    def __ne__(wrapper, other):
        return not wrapper.__eq__(other)

    def __add__(wrapper, new_items):
        return wrapper.copy().union(new_items)

    def __sub__(wrapper, items):
        return wrapper.copy().difference(items)

    def __contains__(wrapper, item):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
        if not isinstance(item, attr.py_type): return False

        reverse = attr.reverse
        if not reverse.is_collection:
            obj2 = item._vals_[reverse] if reverse in item._vals_ else reverse.load(item)
            wbits = item._wbits_
            if wbits is not None:
                bit = item._bits_except_volatile_[reverse]
                if not wbits & bit: item._rbits_ |= bit
            return obj is obj2

        setdata = obj._vals_.get(attr)
        if setdata is not None:
            if item in setdata: return True
            if setdata.is_fully_loaded: return False
            if setdata.absent is not None and item in setdata.absent: return False
        else:
            reverse_setdata = item._vals_.get(reverse)
            if reverse_setdata is not None and reverse_setdata.is_fully_loaded:
                return obj in reverse_setdata
        setdata = attr.load(obj, (item,))
        if item in setdata: return True
        if setdata.absent is None: setdata.absent = set()
        setdata.absent.add(item)
        return False

    def create(wrapper, **kwargs):
        attr = wrapper._attr_
        reverse = attr.reverse
        if reverse.name in kwargs: throw(TypeError,
                                         'When using %s.%s.create(), %r attribute should not be passed explicitly'
                                         % (attr.entity.__name__, attr.name, reverse.name))
        kwargs[reverse.name] = wrapper._obj_
        item_type = attr.py_type
        item = item_type(**kwargs)
        return item

    def add(wrapper, new_items):
        obj = wrapper._obj_
        attr = wrapper._attr_
        cache = obj._session_cache_
        if cache is None or not cache.is_alive: throw_db_session_is_over('change collection', obj, attr)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            reverse = attr.reverse
            if not reverse: throw(NotImplementedError)
            new_items = attr.validate(new_items, obj)
            if not new_items: return
            setdata = obj._vals_.get(attr)
            if setdata is not None: new_items -= setdata
            if setdata is None or not setdata.is_fully_loaded:
                setdata = attr.load(obj, new_items)
            new_items -= setdata
            undo_funcs = []
            try:
                if not reverse.is_collection:
                    for item in new_items: reverse.__set__(item, obj, undo_funcs)
                else:
                    reverse.reverse_add(new_items, obj, undo_funcs)
            except:
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        setdata |= new_items
        if setdata.count is not None: setdata.count += len(new_items)
        added = setdata.added
        removed = setdata.removed
        if removed: (new_items, setdata.removed) = (new_items - removed, removed - new_items)
        if added:
            added |= new_items
        else:
            setdata.added = new_items  # added may be None

        cache.modified_collections[attr].add(obj)
        cache.modified = True

    def __iadd__(wrapper, items):
        wrapper.add(items)
        return wrapper

    def remove(wrapper, items):
        obj = wrapper._obj_
        attr = wrapper._attr_
        cache = obj._session_cache_
        if cache is None or not cache.is_alive: throw_db_session_is_over('change collection', obj, attr)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            reverse = attr.reverse
            if not reverse: throw(NotImplementedError)
            items = attr.validate(items, obj)
            setdata = obj._vals_.get(attr)
            if setdata is not None and setdata.removed:
                items -= setdata.removed
            if not items: return
            if setdata is None or not setdata.is_fully_loaded:
                setdata = attr.load(obj, items)
            items &= setdata
            undo_funcs = []
            try:
                if not reverse.is_collection:
                    if attr.cascade_delete:
                        for item in items: item._delete_(undo_funcs)
                    else:
                        for item in items: reverse.__set__(item, None, undo_funcs)
                else:
                    reverse.reverse_remove(items, obj, undo_funcs)
            except:
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        setdata -= items
        if setdata.count is not None: setdata.count -= len(items)
        added = setdata.added
        removed = setdata.removed
        if added: (items, setdata.added) = (items - added, added - items)
        if removed:
            removed |= items
        else:
            setdata.removed = items  # removed may be None

        cache.modified_collections[attr].add(obj)
        cache.modified = True

    def __isub__(wrapper, items):
        wrapper.remove(items)
        return wrapper

    def clear(wrapper):
        obj = wrapper._obj_
        attr = wrapper._attr_
        cache = obj._session_cache_
        if cache is None or not obj._session_cache_.is_alive: throw_db_session_is_over('change collection', obj, attr)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr.__set__(obj, ())

    async def load(wrapper):
        await wrapper._attr_.load(wrapper._obj_)

    def select(wrapper, *args):
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        query = reverse.entity._select_all()
        s = 'lambda item: JOIN(obj in item.%s)' if reverse.is_collection else 'lambda item: item.%s == obj'
        query = query.filter(s % reverse.name, {'obj': obj, 'JOIN': JOIN})
        if args:
            func, globals, locals = get_globals_and_locals(args, kwargs=None, frame_depth=3)
            query = query.filter(func, globals, locals)
        return query

    filter = select

    def limit(wrapper, limit, offset=None):
        return wrapper.select().limit(limit, offset)

    def page(wrapper, pagenum, pagesize=10):
        return wrapper.select().page(pagenum, pagesize)

    def order_by(wrapper, *args):
        return wrapper.select().order_by(*args)

    def sort_by(wrapper, *args):
        return wrapper.select().sort_by(*args)

    def random(wrapper, limit):
        return wrapper.select().random(limit)

    async def __nonzero__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None: setdata = await attr.load(obj)
        if setdata: return True
        if not setdata.is_fully_loaded: setdata = await attr.load(obj)
        return bool(setdata)


async def SetData___nonzero__(wrapper):
    attr = wrapper._attr_
    obj = wrapper._obj_
    if obj._status_ in del_statuses: throw_object_was_deleted(obj)
    if obj._vals_ is None: throw_db_session_is_over('read value of', obj, attr)
    setdata = obj._vals_.get(attr)
    if setdata is None: setdata = await attr.load(obj)
    if setdata: return True
    if not setdata.is_fully_loaded: setdata = await attr.load(obj)
    return bool(setdata)


def SetInstance___iter__(wrapper):
    return wrapper.copy()


# SetInstance.__nonzero__ = SetData___nonzero__
# SetInstance.__iter__ = AioSetInstance.__iter__
# SetInstance.load = AioSetInstance.load


class EntityMeta(EntityMeta):

    async def _fetch_objects(entity, cursor, attr_offsets, max_fetch_count=None, for_update=False, used_attrs=(),
                             cache=None):
        if max_fetch_count is None: max_fetch_count = options.MAX_FETCH_COUNT
        if max_fetch_count is not None:
            rows = await cursor.fetchmany(max_fetch_count + 1)
            if len(rows) == max_fetch_count + 1:
                if max_fetch_count == 1: throw(MultipleObjectsFoundError,
                                               'Multiple objects were found. Use %s.select(...) to retrieve them' % entity.__name__)
                throw(TooManyObjectsFoundError,
                      'Found more then pony.options.MAX_FETCH_COUNT=%d objects' % options.MAX_FETCH_COUNT)
        else:
            rows = await cursor.fetchall()
        objects = []
        if attr_offsets is None:
            objects = [entity._get_by_raw_pkval_(row, for_update) for row in rows]
            await entity._load_many_(objects)
        else:
            for row in rows:
                real_entity_subclass, pkval, avdict = entity._parse_row_(row, attr_offsets)
                obj = real_entity_subclass._get_from_identity_map_(pkval, 'loaded', for_update)
                if obj._status_ in del_statuses: continue
                obj._db_set_(avdict)
                objects.append(obj)
        if used_attrs: entity._set_rbits(objects, used_attrs)
        return objects

    async def _load_many_(entity, objects):
        database = entity._database_
        cache = database._get_cache()
        seeds = cache.seeds[entity._pk_attrs_]
        if not seeds: return
        objects = {obj for obj in objects if obj in seeds}
        objects = sorted(objects, key=attrgetter('_pkval_'))
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        while objects:
            batch = objects[:max_batch_size]
            objects = objects[max_batch_size:]
            sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(batch))
            arguments = adapter(batch)
            cursor = await database._exec_sql(sql, arguments)
            result = await entity._fetch_objects(cursor, attr_offsets)
            if len(result) < len(batch):
                for obj in result:
                    if obj not in batch:
                        throw(UnrepeatableReadError, 'Phantom object %s disappeared' % safe_repr(obj))

    @db_session(ddl=True)
    async def drop_table(entity, with_all_data=False):
        await entity._database_._drop_tables([entity._table_], True, with_all_data)

    def select(entity, *args):
        return entity._query_from_args_(args, kwargs=None, frame_depth=3)

    def _select_all(entity):
        return AioQuery(entity._default_iter_name_, entity._default_genexpr_, {}, {'.0': entity})

    def _query_from_args_(entity, args, kwargs, frame_depth):
        if not args and not kwargs: return entity._select_all()
        func, globals, locals = get_globals_and_locals(args, kwargs, frame_depth + 1)

        if type(func) is types.FunctionType:
            names = get_lambda_args(func)
            code_key = id(func.func_code if PY2 else func.__code__)
            cond_expr, external_names, cells = decompile(func)
        elif isinstance(func, basestring):
            code_key = func
            lambda_ast = string2ast(func)
            if not isinstance(lambda_ast, ast.Lambda):
                throw(TypeError, 'Lambda function is expected. Got: %s' % func)
            names = get_lambda_args(lambda_ast)
            cond_expr = lambda_ast.code
            cells = None
        else:
            assert False  # pragma: no cover

        if len(names) != 1: throw(TypeError,
                                  'Lambda query requires exactly one parameter name, like %s.select(lambda %s: ...). '
                                  'Got: %d parameters' % (entity.__name__, entity.__name__[0].lower(), len(names)))
        name = names[0]

        if_expr = ast.GenExprIf(cond_expr)
        for_expr = ast.GenExprFor(ast.AssName(name, 'OP_ASSIGN'), ast.Name('.0'), [if_expr])
        inner_expr = ast.GenExprInner(ast.Name(name), [for_expr])
        locals = locals.copy() if locals is not None else {}
        assert '.0' not in locals
        locals['.0'] = entity
        return AioQuery(code_key, inner_expr, globals, locals, cells)

    async def _find_in_db_(entity, avdict, unique=False, for_update=False, nowait=False):
        database = entity._database_
        query_attrs = {attr: value is None for attr, value in iteritems(avdict)}
        limit = 2 if not unique else None
        sql, adapter, attr_offsets = entity._construct_sql_(query_attrs, False, limit, for_update, nowait)
        arguments = adapter(avdict)
        if for_update: database._get_cache().immediate = True
        cursor = await database._exec_sql(sql, arguments)
        objects = await entity._fetch_objects(cursor, attr_offsets, 1, for_update, avdict)
        return objects[0] if objects else None

    async def _find_in_cache_(entity, pkval, avdict, for_update=False):
        cache = entity._database_._get_cache()
        cache_indexes = cache.indexes
        obj = None
        unique = False
        if pkval is not None:
            unique = True
            obj = cache_indexes[entity._pk_attrs_].get(pkval)
        if obj is None:
            for attr in entity._simple_keys_:
                val = avdict.get(attr)
                if val is not None:
                    unique = True
                    obj = cache_indexes[attr].get(val)
                    if obj is not None: break
        if obj is None:
            for attrs in entity._composite_keys_:
                get_val = avdict.get
                vals = tuple(get_val(attr) for attr in attrs)
                if None in vals: continue
                unique = True
                cache_index = cache_indexes.get(attrs)
                if cache_index is None: continue
                obj = cache_index.get(vals)
                if obj is not None: break
        if obj is None:
            for attr, val in iteritems(avdict):
                if val is None: continue
                reverse = attr.reverse
                if reverse and not reverse.is_collection:
                    obj = await reverse.__get__(val)
                    break
        if obj is not None:
            if obj._discriminator_ is not None:
                if obj._subclasses_:
                    cls = obj.__class__
                    if not issubclass(entity, cls) and not issubclass(cls, entity):
                        throw(ObjectNotFound, entity, pkval)
                    seeds = cache.seeds[entity._pk_attrs_]
                    if obj in seeds: await obj._load_()
                if not isinstance(obj, entity): throw(ObjectNotFound, entity, pkval)
            if obj._status_ == 'marked_to_delete': throw(ObjectNotFound, entity, pkval)
            for attr, val in iteritems(avdict):
                if val != attr.__get__(obj): throw(ObjectNotFound, entity, pkval)
            if for_update and obj not in cache.for_update:
                return None, unique  # object is found, but it is not locked
            entity._set_rbits((obj,), avdict)
            return obj, unique
        return None, unique

    async def _find_one_(entity, kwargs, for_update=False, nowait=False):
        if entity._database_.schema is None:
            throw(ERDiagramError, 'Mapping is not generated for entity %r' % entity.__name__)
        avdict = {}
        get_attr = entity._adict_.get
        for name, val in iteritems(kwargs):
            attr = get_attr(name)
            if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
            avdict[attr] = attr.validate(val, None, entity, from_db=False)
        if entity._pk_is_composite_:
            pkval = tuple(imap(avdict.get, entity._pk_attrs_))
            if None in pkval: pkval = None
        else:
            pkval = avdict.get(entity._pk_attrs_[0])
        for attr in avdict:
            if attr.is_collection:
                throw(TypeError, 'Collection attribute %s cannot be specified as search criteria' % attr)
        obj, unique = await entity._find_in_cache_(pkval, avdict, for_update)
        if obj is None: obj = await entity._find_in_db_(avdict, unique, for_update, nowait)
        if obj is None: throw(ObjectNotFound, entity, pkval)
        return obj

    async def get(entity, *args, **kwargs):
        if args: return await entity._query_from_args_(args, kwargs, frame_depth=3).get()
        try:
            return await entity._find_one_(kwargs)  # can throw MultipleObjectsFoundError
        except ObjectNotFound:
            return None

    def _get_set_wrapper_subclass_(entity):
        result_cls = entity._set_wrapper_subclass_
        if result_cls is None:
            mixin = entity._get_propagation_mixin_()
            cls_name = entity.__name__ + 'Set'
            result_cls = type(cls_name, (AioSetInstance, mixin), {})
            entity._set_wrapper_subclass_ = result_cls
        return result_cls


class Entity(Entity):
    async def _load_(obj):
        cache = obj._session_cache_
        if not cache.is_alive:
            throw(DatabaseSessionIsOver, 'Cannot load object %s: the database session is over' % safe_repr(obj))
        entity = obj.__class__
        database = entity._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Object %s doesn't belong to current transaction" % safe_repr(obj))
        seeds = cache.seeds[entity._pk_attrs_]
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        objects = [obj]
        if options.PREFETCHING:
            for seed in seeds:
                if len(objects) >= max_batch_size: break
                if seed is not obj: objects.append(seed)
        sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(objects))
        arguments = adapter(objects)
        cursor = await database._exec_sql(sql, arguments)
        objects = await entity._fetch_objects(cursor, attr_offsets)
        if obj not in objects:
            throw(UnrepeatableReadError, 'Phantom object %s disappeared' % safe_repr(obj))

    async def load(obj, *attrs):
        cache = obj._session_cache_
        if not cache.is_alive:
            throw(DatabaseSessionIsOver, 'Cannot load object %s: the database session is over' % safe_repr(obj))
        entity = obj.__class__
        database = entity._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Object %s doesn't belong to current transaction" % safe_repr(obj))
        if obj._status_ in created_or_deleted_statuses: return
        if not attrs:
            attrs = tuple(attr for attr, bit in iteritems(entity._bits_) if bit and attr not in obj._vals_)
        else:
            args = attrs
            attrs = set()
            for arg in args:
                if isinstance(arg, basestring):
                    attr = entity._adict_.get(arg)
                    if attr is None:
                        if not is_ident(arg): throw(ValueError, 'Invalid attribute name: %r' % arg)
                        throw(AttributeError, 'Object %s does not have attribute %r' % (obj, arg))
                elif isinstance(arg, Attribute):
                    attr = arg
                    if not isinstance(obj, attr.entity):
                        throw(AttributeError, 'Attribute %s does not belong to object %s' % (attr, obj))
                else:
                    throw(TypeError, 'Invalid argument type: %r' % arg)
                if attr.is_collection:
                    throw(NotImplementedError,
                          'The load() method does not support collection attributes yet. Got: %s' % attr.name)
                if entity._bits_[attr] and attr not in obj._vals_: attrs.add(attr)
            attrs = tuple(sorted(attrs, key=attrgetter('id')))

        sql_cache = entity._root_._load_sql_cache_
        cached_sql = sql_cache.get(attrs)
        if cached_sql is None:
            if entity._discriminator_attr_ is not None:
                attrs = (entity._discriminator_attr_,) + attrs
            attrs = entity._pk_attrs_ + attrs

            attr_offsets = {}
            select_list = ['ALL']
            for attr in attrs:
                attr_offsets[attr] = offsets = []
                for column in attr.columns:
                    offsets.append(len(select_list) - 1)
                    select_list.append(['COLUMN', None, column])
            from_list = ['FROM', [None, 'TABLE', entity._table_]]
            criteria_list = [[converter.EQ, ['COLUMN', None, column], ['PARAM', (i, None, None), converter]]
                             for i, (column, converter) in enumerate(izip(obj._pk_columns_, obj._pk_converters_))]
            where_list = ['WHERE'] + criteria_list

            sql_ast = ['SELECT', select_list, from_list, where_list]
            sql, adapter = database._ast2sql(sql_ast)
            cached_sql = sql, adapter, attr_offsets
            sql_cache[attrs] = cached_sql
        else:
            sql, adapter, attr_offsets = cached_sql
        arguments = adapter(obj._get_raw_pkval_())

        cursor = await database._exec_sql(sql, arguments)
        objects = await entity._fetch_objects(cursor, attr_offsets)
        if obj not in objects: throw(UnrepeatableReadError,
                                     'Phantom object %s disappeared' % safe_repr(obj))

    async def _save_created_(obj):
        auto_pk = (obj._pkval_ is None)
        attrs = []
        values = []
        new_dbvals = {}
        for attr in obj._attrs_with_columns_:
            if auto_pk and attr.is_pk: continue
            val = obj._vals_[attr]
            if val is not None:
                attrs.append(attr)
                if not attr.reverse:
                    assert len(attr.converters) == 1
                    dbval = attr.converters[0].val2dbval(val, obj)
                    new_dbvals[attr] = dbval
                    values.append(dbval)
                else:
                    values.extend(attr.get_raw_values(val))
        attrs = tuple(attrs)

        database = obj._database_
        cached_sql = obj._insert_sql_cache_.get(attrs)
        if cached_sql is None:
            columns = []
            converters = []
            for attr in attrs:
                columns.extend(attr.columns)
                converters.extend(attr.converters)
            assert len(columns) == len(converters)
            params = [['PARAM', (i, None, None), converter] for i, converter in enumerate(converters)]
            entity = obj.__class__
            if not columns and database.provider.dialect == 'Oracle':
                sql_ast = ['INSERT', entity._table_, obj._pk_columns_,
                           [['DEFAULT'] for column in obj._pk_columns_]]
            else:
                sql_ast = ['INSERT', entity._table_, columns, params]
            if auto_pk: sql_ast.append(entity._pk_columns_[0])
            sql, adapter = database._ast2sql(sql_ast)
            entity._insert_sql_cache_[attrs] = sql, adapter
        else:
            sql, adapter = cached_sql

        arguments = adapter(values)
        try:
            if auto_pk:
                new_id = await database._exec_sql(sql, arguments, returning_id=True,
                                                  start_transaction=True)
            else:
                await database._exec_sql(sql, arguments, start_transaction=True)
        except IntegrityError as e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(TransactionIntegrityError,
                  'Object %r cannot be stored in the database. %s: %s'
                  % (obj, e.__class__.__name__, msg), e)
        except DatabaseError as e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(UnexpectedError, 'Object %r cannot be stored in the database. %s: %s'
                  % (obj, e.__class__.__name__, msg), e)

        if auto_pk:
            pk_attrs = obj._pk_attrs_
            cache_index = obj._session_cache_.indexes[pk_attrs]
            obj2 = cache_index.setdefault(new_id, obj)
            if obj2 is not obj: throw(TransactionIntegrityError,
                                      'Newly auto-generated id value %s was already used in transaction cache for another object' % new_id)
            obj._pkval_ = obj._vals_[pk_attrs[0]] = new_id
            obj._newid_ = None

        obj._status_ = 'inserted'
        obj._rbits_ = obj._all_bits_except_volatile_
        obj._wbits_ = 0
        obj._update_dbvals_(True, new_dbvals)

    async def _save_updated_(obj):
        update_columns = []
        values = []
        new_dbvals = {}
        for attr in obj._attrs_with_bit_(obj._attrs_with_columns_, obj._wbits_):
            update_columns.extend(attr.columns)
            val = obj._vals_[attr]
            if not attr.reverse:
                assert len(attr.converters) == 1
                dbval = attr.converters[0].val2dbval(val, obj)
                new_dbvals[attr] = dbval
                values.append(dbval)
            else:
                values.extend(attr.get_raw_values(val))
        if update_columns:
            for attr in obj._pk_attrs_:
                val = obj._vals_[attr]
                values.extend(attr.get_raw_values(val))
            cache = obj._session_cache_
            optimistic_session = cache.db_session is None or cache.db_session.optimistic
            if optimistic_session and obj not in cache.for_update:
                optimistic_ops, optimistic_columns, optimistic_converters, optimistic_values = \
                    obj._construct_optimistic_criteria_()
                values.extend(optimistic_values)
            else:
                optimistic_columns = optimistic_converters = optimistic_ops = ()
            query_key = tuple(update_columns), tuple(optimistic_columns), tuple(optimistic_ops)
            database = obj._database_
            cached_sql = obj._update_sql_cache_.get(query_key)
            if cached_sql is None:
                update_converters = []
                for attr in obj._attrs_with_bit_(obj._attrs_with_columns_, obj._wbits_):
                    update_converters.extend(attr.converters)
                assert len(update_columns) == len(update_converters)
                update_params = [['PARAM', (i, None, None), converter] for i, converter in enumerate(update_converters)]
                params_count = len(update_params)
                where_list = ['WHERE']
                pk_columns = obj._pk_columns_
                pk_converters = obj._pk_converters_
                params_count = populate_criteria_list(where_list, pk_columns, pk_converters, repeat('EQ'), params_count)
                if optimistic_columns: populate_criteria_list(
                    where_list, optimistic_columns, optimistic_converters, optimistic_ops, params_count,
                    optimistic=True)
                sql_ast = ['UPDATE', obj._table_, list(izip(update_columns, update_params)), where_list]
                sql, adapter = database._ast2sql(sql_ast)
                obj._update_sql_cache_[query_key] = sql, adapter
            else:
                sql, adapter = cached_sql
            arguments = adapter(values)
            cursor = await database._exec_sql(sql, arguments, start_transaction=True)
            if cursor.rowcount != 1:
                throw(OptimisticCheckError, 'Object %s was updated outside of current transaction' % safe_repr(obj))
        obj._status_ = 'updated'
        obj._rbits_ |= obj._wbits_ & obj._all_bits_except_volatile_
        obj._wbits_ = 0
        obj._update_dbvals_(False, new_dbvals)

    async def _save_deleted_(obj):
        values = []
        values.extend(obj._get_raw_pkval_())
        cache = obj._session_cache_
        if obj not in cache.for_update:
            optimistic_ops, optimistic_columns, optimistic_converters, optimistic_values = \
                obj._construct_optimistic_criteria_()
            values.extend(optimistic_values)
        else:
            optimistic_columns = optimistic_converters = optimistic_ops = ()
        query_key = tuple(optimistic_columns), tuple(optimistic_ops)
        database = obj._database_
        cached_sql = obj._delete_sql_cache_.get(query_key)
        if cached_sql is None:
            where_list = ['WHERE']
            params_count = populate_criteria_list(where_list, obj._pk_columns_, obj._pk_converters_, repeat('EQ'))
            if optimistic_columns: populate_criteria_list(
                where_list, optimistic_columns, optimistic_converters, optimistic_ops, params_count, optimistic=True)
            from_ast = ['FROM', [None, 'TABLE', obj._table_]]
            sql_ast = ['DELETE', None, from_ast, where_list]
            sql, adapter = database._ast2sql(sql_ast)
            obj.__class__._delete_sql_cache_[query_key] = sql, adapter
        else:
            sql, adapter = cached_sql
        arguments = adapter(values)
        cur = await database._exec_sql(sql, arguments, start_transaction=True)
        obj._status_ = 'deleted'
        cache.indexes[obj._pk_attrs_].pop(obj._pkval_)

    async def _save_(obj, dependent_objects=None):
        status = obj._status_

        if status in ('created', 'modified'):
            obj._save_principal_objects_(dependent_objects)

        if status == 'created':
            await obj._save_created_()
        elif status == 'modified':
            await obj._save_updated_()
        elif status == 'marked_to_delete':
            await obj._save_deleted_()
        else:
            assert False, "_save_() called for object %r with incorrect status %s" % (obj, status)  # pragma: no cover

        assert obj._status_ in saved_statuses
        cache = obj._session_cache_
        cache.saved_objects.append((obj, obj._status_))
        objects_to_save = cache.objects_to_save
        save_pos = obj._save_pos_
        if save_pos == len(objects_to_save) - 1:
            objects_to_save.pop()
        else:
            objects_to_save[save_pos] = None
        obj._save_pos_ = None

    async def _delete_(obj, undo_funcs=None):
        status = obj._status_
        if status in del_statuses: return
        is_recursive_call = undo_funcs is not None
        if not is_recursive_call: undo_funcs = []
        cache = obj._session_cache_
        assert cache is not None and cache.is_alive
        with cache.flush_disabled():
            get_val = obj._vals_.get
            undo_list = []
            objects_to_save = cache.objects_to_save
            save_pos = obj._save_pos_

            def undo_func():
                if obj._status_ == 'marked_to_delete':
                    assert objects_to_save
                    obj2 = objects_to_save.pop()
                    assert obj2 is obj
                    if save_pos is not None:
                        assert objects_to_save[save_pos] is None
                        objects_to_save[save_pos] = obj
                    obj._save_pos_ = save_pos
                obj._status_ = status
                for cache_index, old_key in undo_list: cache_index[old_key] = obj

            undo_funcs.append(undo_func)
            try:
                for attr in obj._attrs_:
                    if not attr.is_collection: continue
                    if isinstance(attr, Set):
                        set_wrapper = attr._get(obj)
                        if not await set_wrapper.__nonzero__():
                            pass
                        elif attr.cascade_delete:
                            for robj in await set_wrapper._iter_(): await robj._delete_(undo_funcs)
                        elif not attr.reverse.is_required:
                            await attr.__set__(obj, (), undo_funcs)
                        else:
                            throw(ConstraintError, "Cannot delete object %s, because it has non-empty set of %s, "
                                                   "and 'cascade_delete' option of %s is not set" % (
                                  obj, attr.name, attr))
                    else:
                        throw(NotImplementedError)

                for attr in obj._attrs_:
                    if not attr.is_collection:
                        reverse = attr.reverse
                        if not reverse: continue
                        if not reverse.is_collection:
                            val = await get_val(attr) if attr in obj._vals_ else await attr.load(obj)
                            if val is None: continue
                            if attr.cascade_delete:
                                await val._delete_(undo_funcs)
                            elif not reverse.is_required:
                                await reverse.__set__(val, None, undo_funcs)
                            else:
                                throw(ConstraintError,
                                      "Cannot delete object %s, because it has associated %s, and 'cascade_delete' option"
                                      " of %s is not set" % (obj, attr.name, attr))
                        elif isinstance(reverse, Set):
                            if attr not in obj._vals_: continue
                            val = get_val(attr)
                            if val is None: continue
                            reverse.reverse_remove((val,), obj, undo_funcs)
                        else:
                            throw(NotImplementedError)

                cache_indexes = cache.indexes
                for attr in obj._simple_keys_:
                    val = await get_val(attr)
                    if val is None: continue
                    cache_index = cache_indexes[attr]
                    obj2 = cache_index.pop(val)
                    assert obj2 is obj
                    undo_list.append((cache_index, val))

                for attrs in obj._composite_keys_:
                    vals = tuple(await get_val(attr) for attr in attrs)
                    if None in vals: continue
                    cache_index = cache_indexes[attrs]
                    obj2 = cache_index.pop(vals)
                    assert obj2 is obj
                    undo_list.append((cache_index, vals))

                if status == 'created':
                    assert save_pos is not None
                    objects_to_save[save_pos] = None
                    obj._save_pos_ = None
                    obj._status_ = 'cancelled'
                    if obj._pkval_ is not None:
                        pk_index = cache_indexes[obj._pk_attrs_]
                        obj2 = pk_index.pop(obj._pkval_)
                        assert obj2 is obj
                        undo_list.append((pk_index, obj._pkval_))
                else:
                    if status == 'modified':
                        assert save_pos is not None
                        objects_to_save[save_pos] = None
                    else:
                        assert status in ('loaded', 'inserted', 'updated')
                        assert save_pos is None
                    obj._save_pos_ = len(objects_to_save)
                    objects_to_save.append(obj)
                    obj._status_ = 'marked_to_delete'
                    cache.modified = True
            except:
                if not is_recursive_call:
                    for undo_func in reversed(undo_funcs): undo_func()
                raise

    async def delete(obj):
        cache = obj._session_cache_
        if cache is None or not cache.is_alive: throw_db_session_is_over('delete object', obj)
        await obj._delete_()
        exceptions = []
        try:
            await obj._session_cache_.commit()
            # obj.db_session.commit()
        except:
            exceptions.append(sys.exc_info())
            transact_reraise(CommitException, exceptions)
        else:
            if exceptions:
                transact_reraise(PartialCommitException, exceptions)
        finally:
            del exceptions
        return


class AioQuery(Query):

    def __reduce__(query):
        async def wrap():
            qr = await query._fetch()
            return unpickle_query, (qr,)

        return wrap()

    async def _fetch(query, range=None):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(range)
        database = query._database
        cache = database._get_cache()
        if query._for_update: cache.immediate = True
        # await cache.prepare_connection_for_query_execution()  # may clear cache.query_results
        try:
            result = cache.query_results[query_key]
        except KeyError:
            cursor = await database._exec_sql(sql, arguments)
            if isinstance(translator.expr_type, EntityMeta):
                entity = translator.expr_type
                result = await entity._fetch_objects(cursor, attr_offsets, for_update=query._for_update,
                                                     used_attrs=translator.get_used_attrs())
            elif len(translator.row_layout) == 1:
                func, slice_or_offset, src = translator.row_layout[0]
                result = list(starmap(func, cursor.fetchall()))
            else:
                result = [tuple(func(sql_row[slice_or_offset])
                                for func, slice_or_offset, src in translator.row_layout)
                          for sql_row in await cursor.fetchall()]
                for i, t in enumerate(translator.expr_type):
                    if isinstance(t, EntityMeta) and t._subclasses_: await t._load_many_(row[i] for row in result)
            if query_key is not None: cache.query_results[query_key] = result
        else:
            stats = database._dblocal.stats
            stat = stats.get(sql)
            if stat is not None:
                stat.cache_count += 1
            else:
                stats[sql] = QueryStat(sql)
        # finally:

        if query._prefetch:
            await query._do_prefetch(result)
        return QueryResult(result, query, translator.expr_type, translator.col_names)

    async def _do_prefetch(query, result):
        expr_type = query._translator.expr_type
        object_list = []
        object_set = set()
        append_to_object_list = object_list.append
        add_to_object_set = object_set.add

        if isinstance(expr_type, EntityMeta):
            for obj in result:
                if obj not in object_set:
                    add_to_object_set(obj)
                    append_to_object_list(obj)
        elif type(expr_type) is tuple:
            for i, t in enumerate(expr_type):
                if not isinstance(t, EntityMeta): continue
                for row in result:
                    obj = row[i]
                    if obj not in object_set:
                        add_to_object_set(obj)
                        append_to_object_list(obj)

        cache = query._database._get_cache()
        entities_to_prefetch = query._entities_to_prefetch
        attrs_to_prefetch_dict = query._attrs_to_prefetch_dict
        prefetching_attrs_cache = {}
        for obj in object_list:
            entity = obj.__class__
            if obj in cache.seeds[entity._pk_attrs_]: await obj._load_()

            all_attrs_to_prefetch = prefetching_attrs_cache.get(entity)
            if all_attrs_to_prefetch is None:
                all_attrs_to_prefetch = []
                append = all_attrs_to_prefetch.append
                attrs_to_prefetch = attrs_to_prefetch_dict[entity]
                for attr in obj._attrs_:
                    if attr.is_collection:
                        if attr in attrs_to_prefetch: append(attr)
                    elif attr.is_relation:
                        if attr in attrs_to_prefetch or attr.py_type in entities_to_prefetch: append(attr)
                    elif attr.lazy:
                        if attr in attrs_to_prefetch: append(attr)
                prefetching_attrs_cache[entity] = all_attrs_to_prefetch

            for attr in all_attrs_to_prefetch:
                if attr.is_collection:
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    setdata = obj._vals_.get(attr)
                    if setdata is None or not setdata.is_fully_loaded: setdata = await attr.load(obj)
                    for obj2 in setdata:
                        if obj2 not in object_set:
                            add_to_object_set(obj2)
                            append_to_object_list(obj2)
                elif attr.is_relation:
                    obj2 = await attr.get(obj)
                    if obj2 is not None and obj2 not in object_set:
                        add_to_object_set(obj2)
                        append_to_object_list(obj2)
                elif attr.lazy:
                    await attr.get(obj)
                else:
                    assert False  # pragma: no cover

    async def show(query, width=None):
        query_result = await query._fetch()
        await query_result.show(width)

    async def get(query):
        objects = await query[:2]
        if not objects: return None
        if len(objects) > 1: throw(MultipleObjectsFoundError,
                                   'Multiple objects were found. Use select(...) to retrieve them')
        return objects[0]

    async def first(query):
        translator = query._translator
        if translator.order:
            pass
        elif type(translator.expr_type) is tuple:
            query = query.order_by(*[i + 1 for i in xrange(len(query._translator.expr_type))])
        else:
            query = query.order_by(1)
        objects = await query.without_distinct()[:1]
        if not objects: return None
        return await objects[0]

    async def exists(query):
        objects = await query[:1]
        return bool(objects)

    async def delete(query, bulk=None):
        if not bulk:
            if not isinstance(query._translator.expr_type, EntityMeta):
                throw(TypeError, 'Delete query should be applied to a single entity. Got: %s'
                      % ast2src(query._translator.tree.expr))
            objects = await query._fetch()
            for obj in objects: await obj._delete_()
            return len(objects)
        translator = query._translator
        sql_key = query._key + ('DELETE',)
        database = query._database
        cache = database._get_cache()
        cache_entry = database._constructed_sql_cache.get(sql_key)
        if cache_entry is None:
            sql_ast = translator.construct_delete_sql_ast()
            cache_entry = database.provider.ast2sql(sql_ast)
            database._constructed_sql_cache[sql_key] = cache_entry
        sql, adapter = cache_entry
        arguments = adapter(query._vars)
        cache.immediate = True
        # await cache.prepare_connection_for_query_execution()  # may clear cache.query_results
        cursor = await database._exec_sql(sql, arguments)
        return cursor.rowcount

    def __len__(query):
        async def wrap():
            qr = await query._fetch()
            return len(qr)

        return wrap()

    def __iter__(query):
        async def wrap():
            qr = await query._fetch()
            return iter(qr)

        return wrap()

    def __getitem__(query, key):
        if isinstance(key, slice):
            step = key.step
            if step is not None and step != 1: throw(TypeError, "Parameter 'step' of slice object is not allowed here")
            start = key.start
            if start is None:
                start = 0
            elif start < 0:
                throw(TypeError, "Parameter 'start' of slice object cannot be negative")
            stop = key.stop
            if stop is None:
                if not start:
                    return query._fetch()
                else:
                    throw(TypeError, "Parameter 'stop' of slice object should be specified")
        else:
            throw(TypeError, 'If you want apply index to query, convert it to list first')
        if start >= stop: return []
        return query._fetch(range=(start, stop))

    def limit(query, limit, offset=None):
        start = offset or 0
        stop = start + limit
        return query[start:stop]

    def page(query, pagenum, pagesize=10):
        start = (pagenum - 1) * pagesize
        stop = pagenum * pagesize
        return query[start:stop]

    async def _aggregate(query, aggr_func_name):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(aggr_func_name=aggr_func_name)
        cache = query._database._get_cache()
        try:
            result = cache.query_results[query_key]
        except KeyError:
            cursor = await query._database._exec_sql(sql, arguments)
            row = await cursor.fetchone()
            if row is not None:
                result = row[0]
            else:
                result = None
            if result is None and aggr_func_name == 'SUM': result = 0
            if result is None:
                pass
            elif aggr_func_name == 'COUNT':
                pass
            else:
                expr_type = float if aggr_func_name == 'AVG' else translator.expr_type
                provider = query._database.provider
                converter = provider.get_converter_by_py_type(expr_type)
                result = converter.sql2py(result)
            if query_key is not None: cache.query_results[query_key] = result
        return result

    async def sum(query):
        return await query._aggregate('SUM')

    async def avg(query):
        return await query._aggregate('AVG')

    async def min(query):
        return await query._aggregate('MIN')

    async def max(query):
        return await query._aggregate('MAX')

    async def count(query):
        return await query._aggregate('COUNT')

    def random(query, limit):
        return query.order_by('random()')[:limit]

    def distinct(query):
        return query._clone(_distinct=True)

    def _clone(query, **kwargs):
        new_query = object.__new__(AioQuery)
        new_query.__dict__.update(query.__dict__)
        new_query.__dict__.update(kwargs)
        return new_query


async def QueryResult_show(result, width=None):
    if not width: width = options.CONSOLE_WIDTH
    max_columns = width // 5
    expr_type = result._expr_type
    col_names = result._col_names

    def to_str(x):
        return tostring(x).replace('\n', ' ')

    if isinstance(expr_type, EntityMeta):
        entity = expr_type
        col_names = [attr.name for attr in entity._attrs_ if not attr.is_collection and not attr.lazy][:max_columns]
        if len(col_names) == 1:
            col_name = col_names[0]

            async def _wrap(obj):
                return await (getattr(obj, col_name))
        else:
            async def _wrap(obj):
                res = ()
                for col_nam in col_names:
                    res1 = getattr(obj, col_nam)
                    if type(res1) is not int:
                        res1 = res1
                    res += (res1,)
                return res

        row_maker = _wrap
        rows = [tuple(to_str(value) for value in await row_maker(obj)) for obj in result]
    elif len(col_names) == 1:
        rows = [(to_str(obj),) for obj in result]
    else:
        rows = [tuple(to_str(value) for value in row) for row in result]

    remaining_columns = {}
    for col_num, colname in enumerate(col_names):
        if not rows:
            max_len = len(colname)
        else:
            max_len = max(len(colname), max(len(row[col_num]) for row in rows))
        remaining_columns[col_num] = max_len

    width_dict = {}
    available_width = width - len(col_names) + 1
    while remaining_columns:
        base_len = (available_width - len(remaining_columns) + 1) // len(remaining_columns)
        for col_num, max_len in remaining_columns.items():
            if max_len <= base_len:
                width_dict[col_num] = max_len
                del remaining_columns[col_num]
                available_width -= max_len
                break
        else:
            break
    if remaining_columns:
        base_len = available_width // len(remaining_columns)
        for col_num, max_len in remaining_columns.items():
            width_dict[col_num] = base_len

    print(strjoin('|', (strcut(colname, width_dict[i]) for i, colname in enumerate(col_names))))
    print(strjoin('+', ('-' * width_dict[i] for i in xrange(len(col_names)))))
    for row in rows:
        print(strjoin('|', (strcut(item, width_dict[i]) for i, item in enumerate(row))))


QueryResult.show = QueryResult_show


def make_aggrfunc(std_func):
    def aggrfunc(*args, **kwargs):
        if kwargs: return std_func(*args, **kwargs)
        if len(args) != 1: return std_func(*args)
        arg = args[0]
        if type(arg) is types.GeneratorType:
            try:
                iterator = arg.gi_frame.f_locals['.0']
            except:
                return std_func(*args)
            if isinstance(iterator, EntityIter):
                return getattr(select(arg), std_func.__name__)()
        return std_func(*args)

    aggrfunc.__name__ = std_func.__name__
    return aggrfunc


distinct = make_aggrfunc(utils.distinct)


def make_query(args, frame_depth, left_join=False):
    gen, globals, locals = get_globals_and_locals(
        args, kwargs=None, frame_depth=frame_depth + 1, from_generator=True)
    if isinstance(gen, types.GeneratorType):
        tree, external_names, cells = decompile(gen)
        code_key = id(gen.gi_frame.f_code)
    elif isinstance(gen, basestring):
        tree = string2ast(gen)
        if not isinstance(tree, ast.GenExpr):
            throw(TypeError, 'Source code should represent generator. Got: %s' % gen)
        code_key = gen
        cells = None
    else:
        assert False
    return AioQuery(code_key, tree.code, globals, locals, cells, left_join)


def select(*args):
    return make_query(args, frame_depth=3)

# db = AioDatabase()
# db_session = DBSessionContextManager(ddl=True)
