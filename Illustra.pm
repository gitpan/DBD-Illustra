################################################################################
#
#   File name: Illustra.pm
#   Project: DBD::Illustra
#   Description: Perl-level DBI driver
#
#   Author: Peter Haworth
#   Date created: 17/07/1998
#
#   sccs version: 1.8    last changed: 09/22/98
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

  use DBI 1.0 ();
  use DynaLoader();
  use Exporter();
  use vars qw(
    $VERSION @ISA
    $err $errstr $sqlstate
    $drh
  );

  $VERSION='0.02';
  @ISA=qw(DynaLoader Exporter);
  bootstrap DBD::Illustra $VERSION;

  $err=0;		# holds error code for DBI::err
  $errstr='';		# holds error string for DBI::errstr
  $sqlstate='';		# hold SQL state for DBI::state
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
      State => \$sqlstate,
      Attribution => 'Illustra DBD by Peter Haworth',
    }); # XXX Could add private hash as third arg

    $drh;
  }
}

{
  package DBD::Illustra::dr; # ======= DRIVER ======
  use Symbol;

  # ->connect($dbname,$user,$auth)
  # Database handler constructor
  sub connect{
    my($drh,$dbname,$user,$auth,$attr)=@_;
    # XXX No use for $attr yet

    # Create 'blank' dbh
    my $dbh=DBI::_new_dbh($drh,{
      Name => $dbname,
      User => $user,
#      Pass => $auth,
    }); # XXX Could add private hash as third arg

    # Call XS function to connect to database
    DBD::Illustra::db::_login($dbh,$dbname,$user,$auth)
      or return;

    $dbh;
  }

  my %dbnames; # Holds list of available databases
  sub load_dbnames{
    my($drh)=@_;
    my($fh,$dir)=(Symbol::gensym,Symbol::gensym);

    foreach my $fname ($ENV{MI_SYSPARAMS},"$ENV{MI_HOME}/MiParams"){
      next unless defined $fname;
      next unless open($fh,"< $fname\0");

      while(<$fh>){
	my($key,$value)=split;
	next unless defined($key) && defined($value);
	next unless $key eq 'MI_DATADIR' && $value ne '';
        next unless opendir($dir,"$value/data/base");

	while(defined(my $f=readdir $dir)){
	  (my($db)=$f=~/^(\w+)_\w+\.\w+$/) && -d "$value/data/base/$f"
	    or next;

	  ++$dbnames{$db};
	}
	closedir $dir;
      }
      close $fh;
    }
  }

  sub data_sources{
    my($drh)=@_;

    load_dbnames($drh) unless %dbnames;

    map { "dbi:Illustra:$_" } sort keys %dbnames;
  }
}

{
  package DBD::Illustra::db; # ====== DATABASE ======

  # ->prepare($statement,@attribs)
  # Statement handler constructor
  sub prepare{
    my($dbh,$statement,@attribs)=@_;

    # Make sure the statement has a terminating semicolon
    $statement=~s/\s+$//s;
    $statement=~s/([^;])$/$1;/s;

    # Create a 'blank' sth
    my $sth=DBI::_new_sth($dbh,{
      Statement => $statement,
    });

    DBD::Illustra::st::_prepare($sth,$statement,@attribs)
      or return undef;

    $sth;
  }

  # ->ping
  sub ping{
    my($dbh)=@_;

    my $sth=$dbh->prepare('return 1;')
      or return 0;
    $sth->execute
      or return 0;
    $sth->finish
      or return 0;
    return 1;
  }
}

{
  package DBD::Illustra::st; # ====== STATEMENT ======

  # All done in XS
}


# Return true to require
1;

__END__


=head1 NAME

DBD::Illustra - Access to Illustra Databases

=head1 SYNOPSIS

use DBI;

=head1 DESCRIPTION

This document described DBD::Illustra version 0.02.

You should also read the documentation for DBI as this document qualifies
what is stated there. This document was last updated for the DBI 1.02
specification, and the code requires at least release 1.0 of the DBI.

=head1 USE OF DBD::Illustra

=head2 Loading DBD::Illustra

To use the DBD::Illustra software, you need to load the DBI software.

    use DBI;

Under normal circumstances, you should then connect to your database using the
notation in the section "CONNECTING TO A DATABASE" which calls DBI->connect().

You can find out which databases are available using the function:

    @dbnames=DBI->data_sources('Illustra');

Note that you may be able to connect to other databases not returned by this
method. Also some databases returned by this method may be unavailable due
to access rights or other reasons.

=head2 CONNECTING TO A DATABASE

The DBD::Illustra driver only supports the "new style" form of connect:

    $dbh = DBI->connect("dbi:Illustra:$database",$user,$pass);
    $dbh = DBI->connect("dbi:Illustra:$database",$user,$pass,\%attr);

The $database part of the first argument specifies the name of the database
to connect to. Currently, only databases served by the default server may
be connected.


=head2 DISCONNECTING FROM A DATABASE

You can also disconnect from the database:

    $dbh->disconnect;

This will rollback any uncommitted work. Note that this does not destroy
the database handle. Any statements prepared using this handle are finished
and cannot be used again.

=cut

XXX Put more stuff here!

=head1 AUTHOR

Peter Haworth (pmh@edison.ioppublishing.com)

=head1 SEE ALSO

perl(1), perldoc for DBI
