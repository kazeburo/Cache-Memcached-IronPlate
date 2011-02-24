use inc::Module::Install;
name 'Cache-Memcached-IronPlate';
all_from 'lib/Cache/Memcached/IronPlate.pm';

requires 'List::Util';
requires 'Scalar::Util';
requires 'Storable';
requires 'POSIX';
requires 'Encode';
requires 'overload';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::TCP';
test_requires 'Test::Skip::UnlessExistsExecutable';
test_requires 'Cache::Memcached::Fast';
test_requires 'File::Which';
test_requires 'Proc::Guard';

auto_set_repository;
WriteAll;
