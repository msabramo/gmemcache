gmemcache
=========

Installation
------------

.. code-block:: bash

    $ pip install Cython
    $ pip install gmemcache


Usage
-----

.. code-block:: python

    >>> from gmemcache import MemcacheConnection
    >>> client = MemcacheConnection(['127.0.0.1:11211'])
    >>> client.is_connected()
    True
    >>> client.set_multi({'key1': 'value1', 'key2': 'value2'})
    True
    >>> client.get_multi(['key1', 'key2', 'key3'])
    {'key1': u'value1', 'key2': u'value2'}
    >>> client.close()
