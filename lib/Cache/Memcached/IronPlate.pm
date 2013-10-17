package Cache::Memcached::IronPlate;

use strict;
use warnings;
use Carp qw//;
use URI::Escape;
use Digest::MD5;
use Storable;
use Scalar::Util qw/blessed/;
use List::Util qw//;
use POSIX qw//;
use Encode;
use overload;

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my %args = (
        distribution_num => 20,
        duplication_num => 3,
        @_
    );
    Carp::croak('cache value should be object.') unless blessed $args{cache};
#    $args{distribution_id} = int(rand($args{distribution_num})) + 1;
    bless \%args, $class;
}

sub _is_distribution {
    my $key = shift;
    return unless $key;
    $key =~ m/:dist$/;
}

sub _is_duplication {
    my $key = shift;
    return unless $key;
    $key =~ m/:dup$/;
}

sub _safekey {
    my $key = shift;
    Carp::croak 'no key' unless $key;

    if ( ref $key ) {
        if ( blessed($key) && (my $stringify = overload::Method( $key, '""' )) ) {
            $key = $stringify->($key);
        }
        else {
            $key = Digest::MD5::md5_hex( Storable::nfreeze($key) );
        }
    }

    $key = Encode::encode_utf8($key) if Encode::is_utf8($key);
    
    my $suffix='';
    if ( $key =~ m!(.*)(:dist|:dup)$! ) {
        $key = $1;
        $suffix = $2;
    }
    $key = uri_escape($key,"\x00-\x20\x7f-\xff");
    if ( length($key) > 200 ) {
        $key = Digest::MD5::md5_hex($key);
    }
    $key .= $suffix;
    return $key;
}

sub get {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) ) {
        my $rand = int(rand($self->{distribution_num})) + 1;
        $key .= ":" . $rand;
    }
    elsif ( _is_duplication($key) ) {
        return $self->_get_duplicate($key);
    }
    $self->{cache}->get( $key ); 
}

sub _get_duplicate {
    my $self = shift;
    my $key = shift;

    my $check_num = POSIX::ceil( $self->{duplication_num} / 2 );

    my @keys = map { $key . ":$_" } 1..$self->{duplication_num};
    my $result = $self->{cache}->get_multi(@keys);
  
    my %result;
    map { $result{$_}++ } values %$result;
    List::Util::reduce { $result{$a} > $result{$b} ? $a : $b } grep { $result{$_} >= $check_num } keys %result; 
}

sub get_multi {
    my $self = shift;

    my @keys;
    my @duplicate_keys;
    my %safekey;
    foreach my $key ( @_ ) {
        Carp::croak 'undefined key' if ! defined $key;

        my $safekey = _safekey($key);
        my $rand = int(rand($self->{distribution_num})) + 1;
        $safekey .= ":" . $rand if _is_distribution($safekey);
        $safekey{$safekey} = $key;

        if ( _is_duplication($safekey) ) {
            push @duplicate_keys, $safekey;
            next;
        }
        
        push @keys, $safekey;        
    }

    my $memd = $self->{cache};

    my %result;
    while( my @spliced_keys = splice( @keys, 0, 1000 ) ) {
        my $result = $memd->get_multi(@spliced_keys);
        %result = ( %result, %$result );
    }

    my %back_safekey;
    foreach (keys %result ) {
        $back_safekey{ $safekey{$_} } = $result{$_};
    }

    foreach my $duplicate_key ( @duplicate_keys ) {
        my $result = $self->_get_duplicate($duplicate_key);
        $back_safekey{ $safekey{$duplicate_key} } = $result if defined $result;
    }

    \%back_safekey;
}

sub set {
    my $self = shift;
    my $key = _safekey(shift);
    my $memd = $self->{cache};
    if ( _is_distribution($key) ) {
        for ( 1..$self->{distribution_num} ) {
            my $spread_key = $key . ":" . $_;
            $memd->set($spread_key, @_);
        }
        return 1;
    }
    elsif ( _is_duplication($key) ) {
        for ( 1..$self->{duplication_num} ) {
            my $spread_key = $key . ":" . $_;
            $memd->set($spread_key, @_);
        }
        return 1;
    }
    $memd->set( $key, @_ );
}

sub add {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "add"';
    }
    $self->{cache}->add( $key, @_ );
}

sub replace {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "replace"';
    }
    $self->{cache}->replace( $key, @_ );
}

sub append {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "append"';
    }
    $self->{cache}->append( $key, @_ );
}

sub prepend {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "prepend"';
    }
    $self->{cache}->prepend( $key, @_ );
}

sub incr {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "incr"';
    }
    $self->{cache}->incr( $key, @_ );
}

sub counter {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "counter"';
    }
    
    my $memd = $self->{cache};
    my $result = $memd->incr( $key, @_ );
    if ( defined $result && ! $result ) {
        my $init = shift || 1;
        # incr/decr operations are not thread safe 
        # http://code.google.com/p/memcached/issues/detail?id=172
        $memd->add($key, sprintf("%-20d", 0), @_ );
        $result = $memd->incr($key, $init, @_ );
    }
    $result;
}

sub decr {
    my $self = shift;
    my $key = _safekey(shift);
    if ( _is_distribution($key) || _is_duplication($key) ) {
        Carp::croak 'distribution/duplication keys are not suppoted in "decr"';
    }
    $self->{cache}->decr( $key, @_ );
}

sub delete {
    my $self = shift;
    my $key = _safekey(shift);
    my $memd = $self->{cache};
    if ( _is_distribution($key) ) {
        for ( 1..$self->{distribution_num} ) {
            my $spread_key = $key . ":" . $_;
            $memd->delete($spread_key);
        }
        return 1;
    }
    elsif ( _is_duplication($key) ) {
        for ( 1..$self->{duplication_num} ) {
            my $spread_key = $key . ":" . $_;
            $memd->delete($spread_key);
        }
        return 1;
    }
    $memd->delete($key);
}

*remove = \&delete;


1;
__END__

=head1 NAME

Cache::Memcached::IronPlate - Best practices for Cache::Memcached

=head1 SYNOPSIS

  use Cache::Memcached::IronPlate;
  use Cache::Memcached::Fast;

  my $memd = Cache::Memcached::IronPlate->new(
      cache => Cache::Memcached::Fast->new(...).
  );
  $memd->get
  $memd->get_multi
  $memd->set
  $memd->add
  $memd->replace
  $memd->append
  $memd->prepend
  $memd->incr
  $memd->counter
  $memd->decr
  $memd->delete

=head1 DESCRIPTION

Cache::Memcached::IronPlate is best practices for Cache::Memcached(::Fast)

=head1 FEATURES

=over 4

=item Auto key filter

マルチバイトや制御コードがkeyに含まれている場合、それらをURI Escapeして利用します

  $memd->get("key hoge\n") => get("key%20hoge%0A")

キーが250文字以上の場合は、Digest::MD5でhash値を作り利用します。またオブジェクトの場合はSerializeしてkeyとします

=item キャッシュ分散

設定情報など、比較的変化が少なく、多くのページで読まれるキャッシュは自動的に分散をします
分散するkeyには「:dist」を付加します

  $memd->set("mypref:dist")

内部的には、:common:${num} などとさらにキーを追加して、分散されるようにします。
${num}はデフォルト20です。変更するには、インスタンス作成時に distribution_num を設定します

  my $memd = Cache::Memcached::IronPlate->new(
     distribution_num => 30
  );

キャッシュ拡散の機能は、setとget、get_multi、deleteにのみ有効です。他のメソッドに対して:commonが付いたキーを渡すと
例外となります

=item キャッシュ複製

特定のmemcachedサーバに接続ができない状態になるとセッションが作成できず、
特定のユーザのみログインができないなどの状態がおこります。
keyの名前に「:dup」を付与すると、distribution と同じように自動的にキャッシュを複製します。

内部的には、:dup:${num} などとさらにキーを追加して、複製されるようにします。
${num}はデフォルト3です。インスタンス作成時に duplication_num を設定します

  my $memd = Cache::Memcached::IronPlate->new(
     duplication_num => 30
  );

キャッシュ拡散と異なるのは、キャッシュ取得時に複製したデータを全て取得し、duplication_num の過半数
に達した場合のみ、データを返す事です。大きなキャッシュデータの場合は通信量に影響がでるので注意してください

キャッシュ複製の機能は、setとget、get_multi、deleteにのみ有効です。他のメソッドに対して:commonが付いたキーを渡すと
例外となります

=item カウンター

memcached の increment は指定した値がない場合、動作しません。IronPlateのcounterは自動で初期値をaddします。

=back

=head1 METHODS

=over 4

=item get(key[:common])

=item get_multi(key1,key2,key3)..

get_mulitiに1,000個以上のkeyを渡した場合は、内部的に1000個ごとに分割して処理をします
キャッシュ拡散、複製のkeyも使えます。

=item set(key, value, expires)

=item add(key, value, expires)

=item replace(key, value, expires)

=item incr(key, increment)

=item counter(key, increment, expires)

=item delete(key)

deleteのexpiresはmemcached-1.3.2以降でサポートされなくなったので
何かしらの値が後ろに付いていても無視します

=back

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo {at} gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
