/*##############################################################################
#
#   File name: dbdimp.h
#   Project: DBD::Illustra
#   Description: Implementation definitions
#
#   Author: Peter Haworth
#   Date created: 17/07/1998
#
#   sccs version: 1.3    last changed: 08/12/98
#
#   Copyright (c) 1998 Institute of Physics Publishing
#   You may distribute under the terms of the Artistic License,
#   as distributed with Perl, with the exception that it cannot be placed
#   on a CD-ROM or similar media for commercial distribution without the
#   prior approval of the author.
#
##############################################################################*/

struct imp_drh_st{
  dbih_drc_t com;	/* DBI driver handle, MUST be first element */

  /* Illustra specific fields */
};

struct imp_dbh_st{
  dbih_dbc_t com;	/* DBI database handle, MUST be first element */

  /* Illustra specific fields */
  MI_CONNECTION *conn;	/* database connection */
};

struct imp_sth_st{
  dbih_stc_t com;	/* DBI statement handle, MUST be first element */

  /* Illustra specific fields */
};


/* Rename functions or avoiding name clashes */
#define dbd_init		ill_init
#define dbd_discon_all		ill_discon_all
#define dbd_describe		ill_describe
#define dbd_bind_ph		ill_bind_ph
#define dbd_db_login		ill_db_login
#define dbd_db_do		ill_db_do
#define dbd_db_commit		ill_db_commit
#define dbd_db_rollback		ill_db_rollback
#define dbd_db_disconnect	ill_db_disconnect
#define dbd_db_destroy		ill_db_destroy
#define dbd_db_STORE_attrib	ill_db_STORE_attrib
#define dbd_db_FETCH_attrib	ill_db_FETCH_attrib
#define dbd_db_STORE_attrib_k	ill_db_STORE_attrib_k
#define dbd_db_FETCH_attrib_k	ill_db_FETCH_attrib_k

#define dbd_st_prepare		ill_st_prepare
#define dbd_st_rows		ill_st_rows
#define dbd_st_execute		ill_st_execute
#define dbd_st_fetch		ill_st_fetch
#define dbd_st_finish		ill_st_finish
#define dbd_st_destroy		ill_st_destroy
#define dbd_st_blob_read	ill_st_blob_read
#define dbd_st_STORE_attrib	ill_st_STORE_attrib
#define dbd_st_FETCH_attrib	ill_st_FETCH_attrib
#define dbd_st_STORE_attrib_k	ill_st_STORE_attrib_k
#define dbd_st_FETCH_attrib_k	ill_st_FETCH_attrib_k

