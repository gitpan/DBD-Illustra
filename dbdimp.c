/*##############################################################################
#
#   File name: dbdimp.c
#   Project: DBD::Illustra
#   Description: Main implementation file
#
#   Author: Peter Haworth
#   Date created: 17/07/1998
#
#   sccs version: 1.9    last changed: 09/22/98
#
#   Copyright (c) 1998 Institute of Physics Publishing
#   You may distribute under the terms of the Artistic License,
#   as distributed with Perl, with the exception that it cannot be placed
#   on a CD-ROM or similar media for commercial distribution without the
#   prior approval of the author.
#
##############################################################################*/


#include "Illustra.h"

DBISTATE_DECLARE;

/* Predeclare stuff defined at the bottom */
static int exec_query(imp_dbh_t *,const char *);
static void kill_query(imp_dbh_t *);

/* Callback handler */
static void MI_PROC_CALLBACK
all_callback(MI_EVENT_TYPE type,MI_CONNECTION *conn,void *cb_data,void *u_data){
  mi_integer elevel;
  char *levelstr;
  char sqlcode[6];
  char errmsg[1024];

  switch(type){
  case MI_Exception:
    /* Some sort of server error */
    switch(elevel=mi_error_level(cb_data)){
    case MI_MESSAGE:
      levelstr="MI_MESSAGE";
      break;
    case MI_EXCEPTION:
      levelstr="MI_EXCEPTION";
      break;
    case MI_FATAL:
      levelstr="MI_FATAL";
      break;
    default:
      levelstr="unknown level";
    }
    mi_errmsg(cb_data,errmsg,1024);
    mi_error_sql_code(cb_data,sqlcode,5);
    Perl_warn("XXX MI_Exception(%s): '%s' %s\n",levelstr,sqlcode,errmsg);
    break;
  case MI_Client_Library_Error:
    /* Internal library error */
    switch(elevel=mi_error_level(cb_data)){
    case MI_LIB_BADARG:
      levelstr="MI_LIB_BADARG";
      break;
    case MI_LIB_USAGE:
      levelstr="MI_LIB_USAGE";
      break;
    case MI_LIB_INTERR:
      levelstr="MI_LIB_INTERR";
      break;
    case MI_LIB_NOIMP:
      levelstr="MI_LIB_NOIMP";
      break;
    case MI_LIB_DROPCONN:
      levelstr="MI_LIB_DROPCONN";
      break;
    default:
      levelstr="uknown level";
    }
    mi_errmsg(cb_data,errmsg,1024);
    Perl_warn("XXX MI_Client_Library_Error(%s): %s\n",levelstr,errmsg);
    break;
  case MI_Alerter_Fire_Msg:
    /* Alerter fired or dropped */
    levelstr=mi_alert_status(cb_data)==MI_ALERTER_DROPPED ? "dropped" : "fired";
    Perl_warn("XXX MI_Alerter_Fire_Msg(%s): %s\n",levelstr,mi_alert_name(cb_data));
    break;
  case MI_Xact_State_Change:{
    mi_integer oldlevel,newlevel;

    switch(elevel=mi_xact_state(cb_data)){
    case MI_XACT_BEGIN:
      levelstr="Started";
      break;
    case MI_XACT_END:
      levelstr="Ended";
      break;
    case MI_XACT_ABORT:
      levelstr="Aborted";
      break;
    default:
      levelstr="unknown";
    }
    mi_xact_levels(cb_data,&oldlevel,&newlevel);
    Perl_warn("XXX MI_Xact_State_Change(%s): Old: %d, New: %d\n",
      levelstr,oldlevel,newlevel
    );
  } break;
  case MI_Delivery_Status_Msg:
  case MI_Query_Interrupt_Ack:
  case MI_Print:
  case MI_Request:
  case MI_Tape_Request:
  default:
    Perl_warn("XXX Caught unknown callback: %d\n",type);
  }
}

/* Called when driver first loaded */
void dbd_init(dbistate_t* dbistate){
  DBIS=dbistate; /* Initialize the DBI macros */

  /* Register the global callback handler.
   * We have to call this before any other mi_ functions
   */
  mi_add_callback(MI_All_Events,all_callback,0);

  /* Disable pointer checks, because they don't seem to work */
  {
    MI_PARAMETER_INFO pinfo;

    mi_get_parameter_info(&pinfo);
    pinfo.callbacks_enabled=1;
    pinfo.pointer_checks_enabled=0;
    mi_set_parameter_info(&pinfo);
  }
}

int dbd_discon_all(SV* drh,imp_drh_t* imp_drh){
  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_discon_all called\n");
  /* XXX This is just copied from DBD::Oracle, like DBI::DBD says... */
  if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
    sv_setiv(DBIc_ERR(imp_drh), (IV)1);
    sv_setpv(DBIc_ERRSTR(imp_drh),
      (char*)"disconnect_all not implemented");
    DBIh_EVENT2(drh, ERROR_event,
      DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
    return FALSE;
  }
  if (perl_destruct_level)
    perl_destruct_level = 0;
  return FALSE;
}

/* Store error codes and messages in either handle */
void do_error(SV* h, int rc, char* what){
  D_imp_xxh(h);
  SV* errstr=DBIc_ERRSTR(imp_xxh);

  sv_setiv(DBIc_ERR(imp_xxh),(IV)rc); /* set err early */
  sv_setpv(errstr,what);
  DBIh_EVENT2(h,ERROR_event,DBIc_ERR(imp_xxh),errstr);

  if(dbis->debug >= 2){
    fprintf(DBILOGFP,"%s error %d recorded: %s\n",
      what,rc,SvPV(errstr,na)
    );
  }
}

/* Connect to a database */
int dbd_db_login(SV *dbh,imp_dbh_t *imp_dbh,char *dbname,char *uid,char *pwd){
  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_db_login called\n");

  /* Connect to database */
  if((imp_dbh->conn=mi_open(dbname,uid,pwd))==NULL){
    return FALSE;
  }

  DBIc_IMPSET_on(imp_dbh);		/* imp_dbh is set up */
  DBIc_ACTIVE_on(imp_dbh);		/* call disconnect before freeing */
  DBIc_set(imp_dbh,DBIcf_AutoCommit,1); /* Default is AutoCommit on */

  imp_dbh->st_active=0;	/* No active statement */

  return TRUE;
}

/* $dbh->commit */
int dbd_db_commit(SV *dbh,imp_dbh_t *imp_dbh){
  if(dbis->debug>=2)
    fprintf(DBILOGFP,"DBD::Illustra::dbd_db_commit\n");

  return exec_query(imp_dbh,"commit work;");
}

/* $dbh->rollback */
int dbd_db_rollback(SV *dbh,imp_dbh_t *imp_dbh){
  if(dbis->debug>=2)
    fprintf(DBILOGFP,"DBD::Illustra::dbd_db_rollback\n");
  
  return exec_query(imp_dbh,"rollback work;");
}

/* Disconnect from database */
int dbd_db_disconnect(SV *dbh,imp_dbh_t *imp_dbh){
  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_db_disconnect called\n");

  /* Tell DBI that we've disconnected */
  DBIc_ACTIVE_off(imp_dbh);

  /* Now actually disconnect */
  if(imp_dbh->conn){
    if(mi_close(imp_dbh->conn)){
      /* XXX Replace 0 here with some meaningful error code */
      do_error(dbh,0,"disconnect error");
      return 0;
    }
  }
  /* Clear the connection so dbd_st_finish won't use it */
  imp_dbh->conn=NULL;

  /* Don't free imp_dbh since a reference still exists */
  /* The DESTROY method will free memory */
  return 1;
}

/* Destroy a database handle */
void dbd_db_destroy(SV *dbh,imp_dbh_t *imp_dbh){
  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_db_destroy called\n");

  /* Make sure we have disconnected */
  if(DBIc_is(imp_dbh,DBIcf_ACTIVE))
    dbd_db_disconnect(dbh,imp_dbh);
  
  /* Tell DBI that there's nothing to free */
  DBIc_IMPSET_off(imp_dbh);
}

/* $dbh->STORE, approximately */
int dbd_db_STORE_attrib(SV *dbh,imp_dbh_t *imp_dbh,SV *keysv,SV *valuesv){
  STRLEN kl;
  char *key=SvPV(keysv,kl);
  SV *cachesv=NULL;
  int on=SvTRUE(valuesv);

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_db_STORE_attrib called\n");

  if(kl==10 && strEQ(key,"AutoCommit")){
    /* There is no API for setting autocommit, so we have to execute */

    if(!DBIc_has(imp_dbh,DBIcf_AutoCommit) != !on){
      exec_query(imp_dbh,on ? "set autocommit on;" : "set autocommit off;");
      DBIc_set(imp_dbh,DBIcf_AutoCommit,on);
    }
  }else{
    return FALSE;
  }
  return TRUE;
}

/* $dbh->FETCH, approximately */
SV *dbd_db_FETCH_attrib(SV *dbh,imp_dbh_t *imp_dbh,SV *keysv){
  STRLEN kl;
  char * key=SvPV(keysv,kl);
  SV *retsv=Nullsv;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_db_FETCH_attrib called\n");

  if(kl==10 && strEQ(key,"AutoCommit"))
    retsv=boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit));
  if(!retsv)
    return Nullsv;
  if(retsv==&sv_undef || retsv==&sv_yes || retsv==&sv_no)
    return retsv;
  return sv_2mortal(retsv);
}

/* $sth->prepare */
int dbd_st_prepare(SV *sth,imp_sth_t *imp_sth,char *statement,SV *attribs){
  D_imp_dbh_from_sth;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_prepare called\n");

  imp_sth->done_desc=0;

  /* XXX we might want to do something more sophisticated here */
  DBIc_IMPSET_on(imp_sth);

  return 1;
}

/* $sth->execute */
int dbd_st_execute(SV *sth,imp_sth_t *imp_sth){
  D_imp_dbh_from_sth;
  SV **statement;
  mi_integer res;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_execute called\n");

  if(dbis->debug >= 2)
    fprintf(DBILOGFP,"    -> dbd_st_execute for %p\n",sth);
  
  if(!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");
  
  statement=hv_fetch((HV*)SvRV(sth),"Statement",9,FALSE);

  /* Make sure there isn't an active query */
  kill_query(imp_dbh);

  /* Execute the statement */
  if(mi_exec(imp_dbh->conn,SvPV(*statement,na),0)){
    /* XXX Use useful rc instead of 0 */
    do_error(sth,0,"Can't execute statement");
    return -2;
  }

  /* Initialise the fetch loop */
  DBIc_NUM_FIELDS(imp_sth)=0;
  while((res=mi_get_result(imp_dbh->conn))!=MI_NO_MORE_RESULTS){
    if(res==MI_ERROR){
      /* XXX rc */
      do_error(sth,0,"Error fetching results");
      return -2;
    }else if(res==MI_ROWS){
      MI_ROW_DESC *rowdesc=mi_get_row_desc_without_row(imp_dbh->conn);
      DBIc_NUM_FIELDS(imp_sth)=mi_column_count(rowdesc);
      break;
    }
  }

  if(!imp_sth->done_desc)
    dbd_describe(sth,imp_sth);
      
  DBIc_ACTIVE_on(imp_sth);
  imp_dbh->st_active=imp_sth;
  return -1;
}

/* INTERNAL function: Build meta data about select */
int dbd_describe(SV *h,imp_sth_t *imp_sth){
  D_imp_dbh_from_sth;
  int num=DBIc_NUM_FIELDS(imp_sth);
  int i;
  STRLEN buflen=0;
  char *buff,*p;
  MI_ROW_DESC *rowdesc=mi_get_row_desc_without_row(imp_dbh->conn);

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_describe called\n");

  /* Only do it once! */
  if(imp_sth->done_desc)
    return 1;
  
  /* Allocate field buffers */
  Newz(42,imp_sth->fbh,num,imp_fbh_t);
  /* Illustra won't tell us how long the column names are, so we
     have to fetch them all first */
  for(i=0;i<num;++i){
    p=imp_sth->fbh[i].name=mi_column_name(rowdesc,i);
    buflen+=strlen(p)+1;
    imp_sth->fbh[i].nullable=mi_column_nullable(rowdesc,i);
    imp_sth->fbh[i].precision=mi_column_bound(rowdesc,i);
    imp_sth->fbh[i].scale=mi_column_parameter(rowdesc,i);
  }
  Newz(42,buff,buflen,char);
  p=buff;
  for(i=0;i<num;++i){
    char *q=imp_sth->fbh[i].name;
    STRLEN len=strlen(q);
    Copy(q,p,len,char);
    imp_sth->fbh[i].name=p;
    p+=len+1;
  }

  imp_sth->name_data=buff;
  imp_sth->done_desc=1;

  return 1;
}

/* $sth->fetch */
AV *dbd_st_fetch(SV *sth,imp_sth_t *imp_sth){
  D_imp_dbh_from_sth;

  AV *av;
  mi_integer error;
  MI_ROW *row;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_fetch called\n");

  if(dbis->debug >= 2)
    fprintf(DBILOGFP,"    -> dbd_st_fetch for %p\n",sth);

  if(!imp_dbh->st_active){
    return Nullav;
  }if((row=mi_next_row(imp_dbh->conn,&error))==NULL){
    if(error){
      /* XXX rc */
      do_error(sth,0,"Error fetching row");
    }else{
      /* XXX check result */
      mi_query_finish(imp_dbh->conn);
    }
    DBIc_ACTIVE_off(imp_sth);
    imp_dbh->st_active=0;
    return Nullav;
  }else{
    int numfields=DBIc_NUM_FIELDS(imp_sth); /* XXX watch out for ragged rows! */
    int ChopBlanks=DBIc_is(imp_sth,DBIcf_ChopBlanks);
    int i;
    av=DBIS->get_fbav(imp_sth);

    for(i=0;i<numfields;i++){
      mi_integer collen;
      char *colval;
      SV *sv=AvARRAY(av)[i]; /* (re)use the SV in the AV */

      switch(mi_value(row,i,&colval,&collen)){
      case MI_ERROR:
	/* XXX rc */
	do_error(sth,0,"Can't fetch column");
      case MI_NULL_VALUE:
	(void)SvOK_off(sv); /* Field is NULL, return undef */
	break;
      case MI_NORMAL_VALUE:
	if(ChopBlanks){
	  while(collen && isspace(colval[collen-1])){
	    --collen;
	  }
	}

	if(dbis->debug >= 2){
	  fprintf(DBILOGFP,"      Storing col %d (%s) in %p\n",i,colval,sv);
	}
	sv_setpvn(sv,colval,(STRLEN)collen);
	break;
      /* XXX Don't forget large objects */
      }
    }
  }

  return av;
}

/* $sth->finish */
int dbd_st_finish(SV *sth,imp_sth_t *imp_sth){
  D_imp_dbh_from_sth;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_finish called\n");

  /* Finish the query (but only if it's "ours") */
  if(imp_dbh->conn && imp_dbh->st_active==imp_sth){
    mi_query_finish(imp_dbh->conn);
    imp_dbh->st_active=0;
  }

  /* Tell DBI that the query is finished */
  DBIc_ACTIVE_off(imp_sth);

  return 1;
}

/* $sth->DESTROY, kind of */
void dbd_st_destroy(SV *sth,imp_sth_t *imp_sth){
  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_destroy called\n");

  /* Make sure the statement is not still in use */
  if(DBIc_is(imp_sth,DBIcf_ACTIVE))
    dbd_st_finish(sth,imp_sth);
  
  if(imp_sth->done_desc){
    Safefree(imp_sth->fbh);
    Safefree(imp_sth->name_data);
  }
  DBIc_IMPSET_off(imp_sth);
}

/* $sth->FETCH, approximately */
SV *dbd_st_FETCH_attrib(SV *sth,imp_sth_t *imp_sth,SV *keysv){
  STRLEN kl;
  char *key=SvPV(keysv,kl);
  int i=DBIc_NUM_FIELDS(imp_sth);
  AV *av;
  SV *retsv=NULL;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"ill_st_FETCH_attrib called\n");

  if(kl==4 && strEQ(key,"NAME")){
    av=newAV();
    retsv=newRV(sv_2mortal((SV*)av));
    while(--i>=0)
      av_store(av,i,newSVpv(imp_sth->fbh[i].name,0));
  }else if(kl==8 && strEQ(key,"NULLABLE")){
    av=newAV();
    retsv=newRV(sv_2mortal((SV*)av));
    while(--i>=0)
      av_store(av,i,newSViv(imp_sth->fbh[i].nullable));
  }else if(kl==9 && strEQ(key,"PRECISION")){
    av=newAV();
    retsv=newRV(sv_2mortal((SV*)av));
    while(--i>=0)
      av_store(av,i,newSViv(imp_sth->fbh[i].precision));
  }else if(kl==5 && strEQ(key,"SCALE")){
    av=newAV();
    retsv=newRV(sv_2mortal((SV*)av));
    while(--i>=0){
      int s=imp_sth->fbh[i].scale;

      av_store(av,i,s ? newSViv(s) : &sv_undef);
    }
  }else{
    return Nullsv;
  }

  return sv_2mortal(retsv);
}

/* INTERNAL function to finish the currently active query if necessary */
static void kill_query(imp_dbh_t *imp_dbh){
  imp_sth_t *active=imp_dbh->st_active;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"kill_query called\n");

  if(active){
    mi_query_finish(imp_dbh->conn);
    DBIc_ACTIVE_off(active);
  }

  imp_dbh->st_active=0;
}

/* Quick and dirty execution of statements */
static int exec_query(imp_dbh_t *imp_dbh,const char *st){
  MI_CONNECTION *conn=imp_dbh->conn;
  mi_integer res;
  int ok=1;

  if(dbis->debug>=4)
    fprintf(DBILOGFP,"exec_query(\"%s\") called\n",st);

  /* Make sure there's no active query */
  kill_query(imp_dbh);

  /* Execute the statement */
  if(mi_exec(conn,st,0)){
    return 0;
  }
  while((res=mi_get_result(conn))!=MI_NO_MORE_RESULTS){
    if(res==MI_ERROR){
      ok=0;
      break;
    }
  }
  mi_query_finish(conn);
  return ok;
}

