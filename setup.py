# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='gmemcache',
    version='0.0.1',
    description='A gevent-compatible Memcached Python client with binary protocol and consistent hashing support',
    long_description=open('README.rst').read(),
    author='Studio Ousia',
    author_email='admin@ousia.jp',
    url='http://github.com/studio-ousia/gmemcache/',
    packages=find_packages(),
    license=open('LICENSE').read(),
    include_package_data=True,
    keywords=[],
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
    install_requires=[
        'gsocketpool',
        'msgpack-python',
        'hash_ring',
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    test_suite = 'nose.collector'
)
