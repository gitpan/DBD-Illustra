#!/usr/local/bin/perl
################################################################################
#
#   File name: test.pl
#   Project: DBD::Illustra
#   Description: Test script
#
#   Author: Peter Haworth
#   Date created: 17/07/1998
#
#   sccs version: 1.7    last changed: 09/22/98
#
#   Copyright (c) 1998 Institute of Physics Publishing
#   You may distribute under the terms of the Artistic License,
#   as distributed with Perl, with the exception that it cannot be placed
#   on a CD-ROM or similar media for commercial distribution without the
#   prior approval of the author.
#
################################################################################

use DBI;
use strict;

#DBI->trace(4);
 
foreach my $args(
  # Insert connect args here:
  # ['dbi:Illustra:dbname','username','password'],
  (),
){
  print "Connecting to '",join("','",@$args),"'\n";
  if(my $dbh=DBI->connect(@$args)){
    print "dbh: $dbh\n";
    print qq(
      Type: $dbh->{Type}
      Name: $dbh->{Name}
      AC:   $dbh->{AutoCommit}
      PE:   $dbh->{PrintError}
    );
    print join(',',keys %$dbh),"\n";
    for(1,2,3){
      warn "****** Query #$_...\n";
      if(my $sth=$dbh->prepare('select issn from jnlinfo where skey>0 and skey<5;')){{
	$sth->execute
	  or warn "Can't execute" and last;
	my $names=$sth->{NAME};
	warn "Columns: @$names\n";
	while(my $row=$sth->fetchrow_arrayref){
	  warn "Row: @$row\n";
	}
#	$sth->finish;
      }}else{
	warn "Can't prepare\n";
      }
    }
    $dbh->disconnect;
  }else{
    warn "No connect\n";
  }
}


