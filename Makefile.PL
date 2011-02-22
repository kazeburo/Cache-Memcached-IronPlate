use inc::Module::Install;
name 'Cache-Memcached-IronPlate';
all_from 'lib/Cache/Memcached/IronPlate.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
