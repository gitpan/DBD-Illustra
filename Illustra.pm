################################################################################
#
#   File name: Illustra.pm
#   Project: DBD::Illustra
#   Description: Perl-level DBI driver
#
#   Author: Peter Haworth
#   Date created: 17/07/1998
#
#   sccs version: 1.5    last changed: 08/12/98
#
#   Copyright (c) 1998 Institute of Physics Publishing
#   You may distribute under the terms of the Artistic License,
#   as distributed with Perl, with the exception that it cannot be placed
#   on a CD-ROM or similar media for commercial distribution without the
#   prior approval of the author.
#
################################################################################

use 5.004;
use strict;

{
  package DBD::Illustra;

  use DBI 0.93 ();
  use DynaLoader();
  use Exporter();
  use vars qw(
    $VERSION @ISA
    $err $errstr $drh
  );

  $VERSION='0.01';
  @ISA=qw(DynaLoader Exporter);
  bootstrap DBD::Illustra $VERSION;

  $err=0;		# holds error code for DBI::err
  $errstr='';		# holds error string for DBI::errstr
  undef $drh;		# holds driver handler once initialised

  # ->driver(\%attr)
  # Driver constructor
  sub driver{
    return $drh if $drh;
    my($class,$attr)=@_;
    # XXX What is $attr used for?

    $class.='::dr';

    $drh=DBI::_new_drh($class,{
      Name => 'Illustra',
      Version => $VERSION,
      Err => \$err,
      Errstr => \$errstr,
      Attribution => 'Illustra DBD by Peter Haworth',
    }); # XXX Could add private hash as third arg

    $drh;
  }
}

{
  package DBD::Illustra::dr; # ======= DRIVER ======

  # ->connect($dbname,$user,$auth)
  # Database handler constructor
  sub connect{
    my($drh,$dbname,$user,$auth,$attr)=@_;
    $attr||={};

    # Create 'blank' dbh
    my $dbh=DBI::_new_dbh($drh,{
      Name => $dbname,
      User => $user,
      Pass => $auth,
    }); # XXX Could add private hash as third arg

    # Call XS function to connect to database
    DBD::Illustra::db::_login($dbh,$dbname,$user,$auth)
      or return;

    # Preset AutoCommit mode
    $attr->{AutoCommit}=1 if !defined $attr->{AutoCommit};
    # XXX Do something with this hash

    $dbh;
  }
}

{
  package DBD::Illustra::db; # ====== DATABASE ======

  # ->prepare($statement,@attribs)
  # Statement handler constructor
  sub prepare{
    my($dbh,$statement,@attribs)=@_;

    # Create a 'blank' sth
    my $sth=DBI::_new_sth($dbh,{
      Statement => $statement,
    });

    DBD::Illustra::st::_prepare($sth,$statement,@attribs)
      or return undef;

    $sth;
  }
}

{
  package DBD::Illustra::st; # ====== STATEMENT ======

  # All done in XS
}


# Return true to require
1;

