use strict;
use Test::More;

use Test::TCP qw/empty_port wait_port/;
use Test::Skip::UnlessExistsExecutable;
use File::Which qw(which);
use Proc::Guard;

use URI;
use Cache::Memcached::IronPlate;
use Cache::Memcached::Fast;

skip_all_unless_exists 'memcached';

my @memcached;
for ( 1..5 ) {
    my $port = empty_port();
    my $proc = proc_guard( scalar which('memcached'), '-p', $port, '-U', 0, '-l', '127.0.0.1' );
    wait_port($port);
    push @memcached, { proc => $proc, port => $port };
}

my $cache = Cache::Memcached::Fast->new({
    servers => [map { "localhost:" . $_->{port} } @memcached]
});

my $memd = Cache::Memcached::IronPlate->new(
    cache => $cache
);

ok( $memd->set('var1','bar1') );
is( $memd->get('var1'), 'bar1' );

ok( $memd->set("v a\nr\t1",'bar1') );
is( $memd->get("v a\nr\t1"), 'bar1' );
is( $memd->get("v%20a%0Ar%091"), 'bar1' );

ok( $memd->set("spread:dist",'spreadval' ) );
is( $memd->get("spread:dist"), 'spreadval' );
is( $memd->get("spread:dist:1"), 'spreadval', 'sg1' );
is( $memd->get("spread:dist:2"), 'spreadval', 'sg2' );
is( $memd->get("spread:dist:3"), 'spreadval', 'sg3' );

ok( $memd->set("spread:dup",'dupval' ) );
is( $memd->get("spread:dup"), 'dupval' );
is( $memd->get("spread:dup:1"), 'dupval', 'dg1' );
is( $memd->get("spread:dup:2"), 'dupval', 'dg2' );
is( $memd->get("spread:dup:3"), 'dupval', 'dg3' );
ok( $memd->delete("spread:dup:1") );
is( $memd->get("spread:dup"), 'dupval' );
ok( $memd->delete("spread:dup:2") );
ok( ! $memd->get("spread:dup") );
ok( $memd->delete("spread:dup") );
ok( ! $memd->get("spread:dup:3") );
ok( $memd->set("spread:dup",'dupval2' ) );

ok( $memd->set("x"x512,'longkey' ) );
is( $memd->get("x"x512), 'longkey' );

ok( $memd->set("x"x512 . ":dist",'long distribute key' ) );
is( $memd->get("x"x512 . ":dist"), 'long distribute key' );

{
    my $flaged = "\x{3042}"x100;
    ok( $memd->set($flaged ,'long utf8 key' ) );
    is( $memd->get($flaged) ,'long utf8 key' );
}

for my $method ( qw/add replace append prepend incr counter decr/ ) {
    eval {
        $memd->$method("test:dist", 1);
    };
    ok($@);

    eval {
        $memd->$method("test:dup", 1);
    };
    ok($@);
}

is( $memd->counter("counter1", 1, 2), 1 );
is( $memd->counter("counter1", 1, 2), 2 );

is( $memd->counter("counter2"), 1 );
is( $memd->counter("counter2"), 2 );
is( $memd->counter("counter2"), 3 );

my $hashref = { a => "b" };
ok( $memd->add($hashref,'reference' ) );
is( $memd->get($hashref), 'reference' );

my $uri = URI->new("http://www.google.com/");
ok( $memd->add($uri,'stringify object' ) );
is( $memd->get("$uri"), 'stringify object' );


sleep 3;

is_deeply( $memd->get_multi(
    'var1', 
    "v a\nr\t1",
    "spread:dist",
    "spread:dup",
    "counter1",
    "x"x512,
    $hashref,
), {
    'var1' => 'bar1',
    "v a\nr\t1" => 'bar1',
    "spread:dist" => 'spreadval',
    "spread:dup" => 'dupval2',
    "x"x512 => 'longkey',
    $hashref => 'reference',
} );

ok( $memd->delete("spread:dist" ) );
ok( ! $memd->get("spread:dist:1") );
ok( ! $memd->get("spread:dist:2") );
ok( ! $memd->get("spread:dist:3") );
ok( ! $memd->get("spread:dist") );

$memd->can('remove');



subtest 'cache args required' => sub {
     eval { Cache::Memcached::IronPlate->new() } ;
     like($@ ,qr/^cache value should be object/);
};


done_testing();
