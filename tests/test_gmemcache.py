# -*- coding: utf-8 -*-

import time
import uuid

from gmemcache import MemcacheConnection

from nose.tools import *

MEMCACHED_SERVER = '127.0.0.1:11211'

_conn = None


def _setup_connection():
    global _conn
    _conn = MemcacheConnection([MEMCACHED_SERVER])


def _drop_connection():
    global _conn
    _conn.close()
    _conn = None


def test_open_lazy():
    conn = MemcacheConnection([MEMCACHED_SERVER], lazy=True)
    ok_(not conn.is_connected())
    conn.open()
    ok_(conn.is_connected())
    conn.close()


def test_close():
    conn = MemcacheConnection([MEMCACHED_SERVER])
    conn.open()
    ok_(conn.is_connected())
    conn.close()
    ok_(not conn.is_connected())


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_get():
    key = uuid.uuid1().hex
    _conn.set(key, 'value')

    eq_('value', _conn.get(key))


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_get_multi():
    key1 = uuid.uuid1().hex
    key2 = uuid.uuid1().hex
    key3 = uuid.uuid1().hex
    key4 = uuid.uuid1().hex
    _conn.set_multi({key1: 'value1', key2: 'value2', key3: 'value3'})

    eq_({key1: 'value1', key2: 'value2', key3: 'value3'},
        _conn.get_multi([key1, key2, key3]))

    eq_({key1: 'value1'},
        _conn.get_multi([key1, key4]))


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_set():
    key = uuid.uuid1().hex
    _conn.set(key, 'value')
    eq_('value', _conn.get(key))


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_set_with_lifetime():
    key = uuid.uuid1().hex
    _conn.set(key, 'value', lifetime=1)
    eq_('value', _conn.get(key))
    time.sleep(2)
    eq_(None, _conn.get(key))


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_set_multi():
    key1 = uuid.uuid1().hex
    key2 = uuid.uuid1().hex
    key3 = uuid.uuid1().hex
    _conn.set_multi({key1: 'value1', key2: 'value2', key3: 'value3'})
    eq_({key1: 'value1', key2: 'value2', key3: 'value3'}, _conn.get_multi([key1, key2, key3]))


@with_setup(setup=_setup_connection, teardown=_drop_connection)
def test_set_multi_with_lifetime():
    key = uuid.uuid1().hex
    _conn.set_multi({key: 'value'}, lifetime=1)
    eq_('value', _conn.get(key))
    time.sleep(2)
    eq_(None, _conn.get(key))
