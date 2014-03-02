# -*- coding: utf-8 -*-

import socket
import struct
import msgpack
import logging
from collections import defaultdict
from hash_ring import HashRing

import gsocketpool

HEADER_SIZE = 24

STRUCT_HEADER = '!BBHBBHLLQ'
STRUCT_GET = STRUCT_HEADER + '%ds'
STRUCT_SET = STRUCT_HEADER + 'LL%ds%ds'

REQUEST_MAGIC = 0x80
RESPONSE_MAGIC = 0x81

STATUS_SUCCESS = 0x00

COMMAND_GET = 0x00
COMMAND_GETK = 0x0C
COMMAND_GETKQ = 0x0D
COMMAND_SET = 0x01
COMMAND_SETQ = 0x11


class MemcacheConnection(object):
    """Memcache connection.

    Usage:
        >>> from gmemcache import MemcacheConnection
        >>> client = MemcacheConnection(['127.0.0.1:11211'])
        >>> client.is_connected()
        True
        >>> client.set_multi({'key1': 'value1', 'key2': 'value2'})
        True
        >>> client.get_multi(['key1', 'key2', 'key3'])
        {'key1': u'value1', 'key2': u'value2'}
        >>> client.close()

    :param list hosts: Hostnames.
    :param int timeout: (optional) Timeout.
    :param bool lazy: (optional) If set to True, the socket connection is not
        established until you specifically call :func:`open() <gmemcache.MemcacheConnection.open>`.
    """

    def __init__(self, hosts, timeout=5, lazy=False):
        self._hosts = hosts
        self._timeout = timeout

        self._sockets = None
        self._ring = HashRing(self._hosts)

        if not lazy:
            self.open()

    def open(self):
        self._sockets = {}
        for host in self._hosts:
            self._sockets[host] = self._connect(host)

    def close(self):
        try:
            for sock in self._sockets.values():
                sock.close()
        except:
            logging.exception('Failed to close the memcache connection')

        self._sockets = None

    def _connect(self, host):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._timeout)
        (server, port) = host.split(':')
        sock.connect((server, int(port)))

        return sock

    def is_connected(self):
        if self._sockets:
            return True
        else:
            return False

    def reconnect(self, host):
        try:
            self._sockets[host].close()
        except:
            logging.exception('Failed to close the memcache connection: %s' % (host,))

        try:
            self._sockets[host] = self._connect(host)
        except:
            logging.exception('Failed to connect to the memcached: %s' % (host,))

    def get(self, key):
        assert self._sockets is not None, 'The connection has not been created'

        host = self._ring.get_node(key)
        sock = self._sockets[host]
        try:
            sock.sendall(struct.pack(STRUCT_GET % (len(key),),
                                     REQUEST_MAGIC,
                                     COMMAND_GET,
                                     len(key), 0, 0, 0, len(key), 0, 0, key))

            resp = self._get_response(sock)

        except:
            self.reconnect(host)
            raise

        (_, value) = struct.unpack('!L%ds' % (resp['bodylen'] - 4,), resp['content'])

        if resp['status'] == STATUS_SUCCESS:
            return self._unpack(value)

        else:
            return None

    def get_multi(self, keys):
        assert self._sockets is not None, 'The connection has not been created'

        if not keys:
            return {}

        divided_keys = defaultdict(list)
        for key in keys:
            divided_keys[self._ring.get_node(key)].append(key)

        msgs = defaultdict(str)
        for (host, host_keys) in divided_keys.iteritems():
            for (n, key) in enumerate(host_keys):
                if n != len(host_keys) - 1:
                    msgs[host] += struct.pack(STRUCT_GET % (len(key),),
                                              REQUEST_MAGIC,
                                              COMMAND_GETKQ,
                                              len(key), 0, 0, 0, len(key), 0, 0, key)
                else:
                    msgs[host] += struct.pack(STRUCT_GET % (len(key),),
                                              REQUEST_MAGIC,
                                              COMMAND_GETK,
                                              len(key), 0, 0, 0, len(key), 0, 0, key)

        failed_hosts = []
        for (host, msg) in msgs.iteritems():
            try:
                self._sockets[host].sendall(msg)
            except:
                logging.exception('An error has occurred while sending a request to memcached')
                failed_hosts.append(host)
                self.reconnect(host)
                continue

        ret = {}
        for host in msgs.iterkeys():
            if host in failed_hosts:
                continue

            sock = self._sockets[host]
            opcode = -1

            while opcode != COMMAND_GETK:
                try:
                    resp = self._get_response(sock)
                except:
                    logging.exception('An error has occurred while receiving a response from memcached')
                    self.reconnect(host)
                    break

                opcode = resp['opcode']

                if resp['status'] == STATUS_SUCCESS:
                    (_, key, value) = struct.unpack('!L%ds%ds' % (resp['keylen'], resp['bodylen'] - resp['keylen'] - 4),
                                                    resp['content'])
                    ret[key] = self._unpack(value)

        return ret

    def set(self, key, value, lifetime=0):
        assert self._sockets is not None, 'The connection has not been created'

        host = self._ring.get_node(key)
        sock = self._sockets[host]
        packed_value = self._pack(value)

        try:
            sock.sendall(struct.pack(STRUCT_SET % (len(key), len(packed_value)),
                                     REQUEST_MAGIC,
                                     COMMAND_SET,
                                     len(key),
                                     8, 0, 0, len(key) + len(packed_value) + 8, 0, 0, 0,
                                     lifetime, key, packed_value))

            resp = self._get_response(sock)

        except:
            self.reconnect(host)
            raise

        if resp['status'] == STATUS_SUCCESS:
            return True
        else:
            return False

    def set_multi(self, data, lifetime=0):
        assert self._sockets is not None, 'The connection has not been created'

        if not data:
            return True

        divided_data = defaultdict(dict)
        for (key, value) in data.iteritems():
            divided_data[self._ring.get_node(key)][key] = value

        msgs = defaultdict(str)
        for (host, host_data) in divided_data.iteritems():
            for (n, (key, value)) in enumerate(host_data.iteritems()):
                host = self._ring.get_node(key)
                packed_value = self._pack(value)
                if n != len(host_data) - 1:
                    msgs[host] += struct.pack(STRUCT_SET % (len(key), len(packed_value)),
                                              REQUEST_MAGIC,
                                              COMMAND_SETQ,
                                              len(key), 8, 0, 0, len(key) + len(packed_value) + 8, 0, 0, 0,
                                              lifetime, key, packed_value)
                else:
                    msgs[host] += struct.pack(STRUCT_SET % (len(key), len(packed_value)),
                                              REQUEST_MAGIC,
                                              COMMAND_SET,
                                              len(key), 8, 0, 0, len(key) + len(packed_value) + 8, 0, 0, 0,
                                              lifetime, key, packed_value)

        failed_hosts = []
        for (host, msg) in msgs.iteritems():
            try:
                self._sockets[host].sendall(msg)
            except:
                logging.exception('An error has occurred while sending a request to memcached')
                failed_hosts.append(host)
                self.reconnect(host)
                continue

        retval = True
        for host in msgs.iterkeys():
            if host in failed_hosts:
                continue

            sock = self._sockets[host]
            opcode = -1

            while opcode != COMMAND_SET:
                try:
                    resp = self._get_response(sock)
                except:
                    logging.exception('An error has occurred while receiving a response from memcached')
                    retval = False
                    self.reconnect(host)
                    break

                opcode = resp['opcode']
                if resp['status'] != STATUS_SUCCESS:
                    retval = False

        return retval

    def _pack(self, value):
        return msgpack.packb(value, encoding='utf-8')

    def _unpack(self, value):
        return msgpack.unpackb(value, encoding='utf-8')

    def _get_response(self, sock):
        header = self._read(HEADER_SIZE, sock)
        (magic, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas) = struct.unpack(STRUCT_HEADER, header)

        assert magic == RESPONSE_MAGIC

        extra_content = None
        if bodylen:
            extra_content = self._read(bodylen, sock)

        return dict(status=status,
                    opcode=opcode,
                    keylen=keylen,
                    bodylen=bodylen,
                    content=extra_content)

    def _read(self, size, sock):
        value = ''
        while len(value) < size:
            data = sock.recv(size - len(value))
            if not data:
                raise IOError('Connection closed')
            value += data

        return value


class MemcachePoolConnection(MemcacheConnection, gsocketpool.Connection):
    def __init__(self, hosts, timeout=5):
        MemcacheConnection.__init__(self, hosts, timeout, lazy=False)
