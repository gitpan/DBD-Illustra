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
#   sccs version: 1.5    last changed: 08/12/98
#
#   Copyright (c) 1998 Institute of Physics Publishing
#   You may distribute under the terms of the Artistic License,
#   as distributed with Perl, with the exception that it cannot be placed
#   on a CD-ROM or similar media for commercial distribution without the
#   prior approval of the author.
#
################################################################################

use blib;
use DBI;
use strict;

DBI->trace(2);

foreach my $args(
  ['dbi:Illustra:xyz','','',''],
  ['dbi:Illustra:journals','miadmin','',''],
){
  print "Connecting to '",join("','",@$args),"'\n";
  if(my $dbh=DBI->connect(@$args)){
    print "dbh: $dbh\n";
    if(my $sth=$dbh->prepare('select issn from jnlinfo where skey>0 and skey<5;')){{
      $sth->execute
	or warn "Can't execute" and last;
      while(my $row=$sth->fetchrow_arrayref){
	print "@$row\n";
      }
#      $sth->finish;
    }}
    $dbh->disconnect;
  }else{
    print "No connect\n";
  }
}


