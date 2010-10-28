#ifndef __STASH_H
#define __STASH_H

#include <expbuf.h>
#include <expbufpool.h>
#include <linklist.h>
#include <risp.h>
#include <rispbuf.h>

// This version indicates the version of the library so that developers of
// services can ensure that the correct version is installed.
// This version number should be incremented with every change that would
// effect logic.
#define LIBSTASH_VERSION 0x00000700
#define LIBSTASH_VERSION_NAME "v0.07.00"


#if (EXPBUF_VERSION < 0x00010200)
#error "Needs libexpbuf v1.02 or higher"
#endif

#if (LIBLINKLIST_VERSION < 0x00008000)
#error "Needs liblinklist v0.80 or higher"
#endif

#if (RISP_VERSION < 0x00020000)
#error "Needs librisp v2.00.00 or higher"
#endif


// Since we will be using a number of bit masks to check for data status's and
// so on, we should include some macros to make it easier.
#define BIT_TEST(arg,val) (((arg) & (val)) == (val))
#define BIT_SET(arg,val) ((arg) |= (val))
#define BIT_CLEAR(arg,val) ((arg) &= ~(val))
#define BIT_TOGGLE(arg,val) ((arg) ^= (val))



// global constants and other things go here.
#define STASH_DEFAULT_PORT (13600)

// start out with an 1kb buffer.  Whenever it is full, we will double the
// buffer, so this is just a minimum starting point.
#define STASH_DEFAULT_BUFFSIZE (1024)


// NOTE; At first I will try to put all the commands into a single risp list.  This will be the easiest to take 

													/// execute commands (0 to 31)
#define STASH_CMD_NOP              (0)
#define STASH_CMD_CLEAR            (1)
#define STASH_CMD_NEXT_VOLUME      (2)
#define STASH_CMD_AUTO             (3)
													/// flags (32 to 63)
#define STASH_CMD_TRUE             (32)
#define STASH_CMD_FALSE            (33)
#define STASH_CMD_RIGHT_ADDUSER    (34)
#define STASH_CMD_RIGHT_CREATE     (35)
#define STASH_CMD_RIGHT_DROP       (36)
#define STASH_CMD_RIGHT_SET        (37)
#define STASH_CMD_RIGHT_UPDATE     (38)
#define STASH_CMD_RIGHT_DELETE     (39)
#define STASH_CMD_RIGHT_QUERY      (40)
#define STASH_CMD_RIGHT_LOCK       (41)
#define STASH_CMD_STRICT           (42)
#define STASH_CMD_UNIQUE           (43)
#define STASH_CMD_OVERWRITE        (44)
#define STASH_CMD_TRANSIENT        (45)
#define STASH_CMD_NULL             (46)
#define STASH_CMD_SORTASC          (47)
#define STASH_CMD_SORTDESC         (48)
													/// byte integer 8-bit (64 to 95)
													/// integer 16-bit (96 to 127)
#define STASH_CMD_FILE_SEQ         (96)
#define STASH_CMD_NAMESPACE_ID     (97)
#define STASH_CMD_FAILCODE         (98)
													/// large integer 32-bit (128 to 159)
#define STASH_CMD_TRANS_HI         (128)
#define STASH_CMD_TRANS_LO         (129)
#define STASH_CMD_REQUEST_ID       (130)
#define STASH_CMD_INTEGER          (131)
#define STASH_CMD_TABLE_ID         (132)
#define STASH_CMD_ROW_ID           (133)
#define STASH_CMD_ENTRY_ID         (134)
#define STASH_CMD_NAME_ID          (135)
#define STASH_CMD_KEY_ID           (136)
#define STASH_CMD_USER_ID          (137)
#define STASH_CMD_LOCK_ID          (138)
#define STASH_CMD_COUNT            (139)
#define STASH_CMD_EXPIRES          (140)
#define STASH_CMD_DATETIME         (141)

													/// short string (160 to 191)
#define STASH_CMD_USERNAME         (160)
#define STASH_CMD_PASSWORD         (161)
#define STASH_CMD_NAMESPACE        (162)
#define STASH_CMD_TABLE            (163)
#define STASH_CMD_NAME             (164)
#define STASH_CMD_KEY              (165)
#define STASH_CMD_COND_EXISTS      (166)
#define STASH_CMD_SET_EXPIRY       (167)
#define STASH_CMD_DELETE           (168)
#define STASH_CMD_SORTENTRY        (169)
													/// string (192 to 223)
#define STASH_CMD_CREATE_USER      (192)
#define STASH_CMD_DROP_USER        (193)
#define STASH_CMD_SET_PASSWORD     (194)
#define STASH_CMD_GRANT            (195)
#define STASH_CMD_REVOKE           (196)
#define STASH_CMD_CREATE_NAMESPACE (197)
#define STASH_CMD_DROP_NAMESPACE   (198)
#define STASH_CMD_RENAME_TABLE     (199)
#define STASH_CMD_LOGIN            (202)
#define STASH_CMD_LOCK             (203)
#define STASH_CMD_FAILED           (204)
#define STASH_CMD_GETID            (205)
#define STASH_CMD_CREATE_TABLE     (206)
#define STASH_CMD_CREATE_NAME      (207)
#define STASH_CMD_CREATE_KEY       (208)
#define STASH_CMD_SORT             (209)

#define STASH_CMD_COND_NAME        (222)
#define STASH_CMD_COND_EQUALS      (223)

													/// large string (224 to 255)
#define STASH_CMD_REQUEST          (224)
#define STASH_CMD_REPLY            (225)
#define STASH_CMD_QUERY            (226)
#define STASH_CMD_SET              (227)
#define STASH_CMD_UPDATE           (228)
#define STASH_CMD_SYNC             (229)
#define STASH_CMD_ADMIN            (230)
#define STASH_CMD_STRING           (231)
#define STASH_CMD_OPERATION        (233)
#define STASH_CMD_PAYLOAD          (234)
#define STASH_CMD_VALUE            (235)
#define STASH_CMD_ATTRIBUTE        (236)
#define STASH_CMD_CREATE_ROW       (237)
#define STASH_CMD_CONDITION        (238)
#define STASH_CMD_ROW              (239)
#define STASH_CMD_COND_AND         (240)
#define STASH_CMD_COND_OR          (241)
#define STASH_CMD_COND_A           (242)
#define STASH_CMD_COND_B           (243)
#define STASH_CMD_COND_NOT         (244)


// stash error codes.
#define STASH_ERR_OK                 (0)
#define STASH_ERR_USEREXISTS         (1)
#define STASH_ERR_NOTCONNECTED       (2)
#define STASH_ERR_AUTHFAILED         (3)
#define STASH_ERR_INSUFFICIENTRIGHTS (4)
#define STASH_ERR_USERNOTEXIST       (5)
#define STASH_ERR_NSNOTEXIST         (6)
#define STASH_ERR_TABLEEXISTS        (7)
#define STASH_ERR_GENERICFAIL        (8)
#define STASH_ERR_TABLENOTEXIST      (9)
#define STASH_ERR_NOTUNIQUE          (10)
#define STASH_ERR_NOTSTRICT          (11)
#define STASH_ERR_ROWEXISTS          (12)
#define STASH_ERR_KEYNOTEXIST        (13)

#define STASH_TABOPT_UNIQUE          (1)
#define STASH_TABOPT_STRICT          (2)
#define STASH_TABOPT_OVERWRITE       (4)

#define STASH_NAMEOPT_TRANSIENT      (1)

// grant rights -- bitmask
#define STASH_RIGHT_ADDUSER    (1)
#define STASH_RIGHT_CREATE     (2)
#define STASH_RIGHT_DROP       (4)
#define STASH_RIGHT_SET        (8)
#define STASH_RIGHT_UPDATE     (16)
#define STASH_RIGHT_DELETE     (32)
#define STASH_RIGHT_QUERY      (64)
#define STASH_RIGHT_LOCK       (128)



typedef int stash_userid_t;
typedef unsigned int stash_result_t;
typedef int stash_nsid_t;
typedef int stash_tableid_t;
typedef int stash_nameid_t;
typedef int stash_keyid_t;
typedef int stash_rowid_t;
typedef int stash_expiry_t;



typedef struct {
	risp_t *risp;
	risp_t *risp_reply;
	risp_t *risp_failed;
	risp_t *risp_row;
	risp_t *risp_attr;
	
	// linked-list of our connections.  Only the one at the head is likely to be
	// active (although it might not be).  When a connection is dropped or is
	// timed out, it is put at the bottom of the list.
	list_t *connlist;		/// conn_t

	// even though we are only have one request at a time, the replies remain active until the 
	list_t *replypool;		/// stash_reply_t

	expbuf_t *readbuf;
	expbuf_t *buf_set;
	expbuf_t *buf_attr;
	expbuf_t *buf_value;
	expbuf_t *buf_payload;
	expbuf_t *buf_request;

	int next_reqid;

	// authority
	stash_userid_t uid;
	char *username;
	char *password;

	short int internally_created;
	
} stash_t;


#define NULL_KEYID   0
#define NULL_NAMEID  0


// this complicated structure is used for the replies.  
typedef struct {
	stash_t        *stash;
	int             reqid;
	stash_result_t  resultcode;
	risp_command_t  operation;	// 0 indicates reply structure is not currently in use.
	stash_userid_t  uid;
	stash_nsid_t    nsid;
	stash_tableid_t tid;
	stash_keyid_t   kid;
	int             row_count;
	list_t         *rows;		// replyrow_t
	int             curr_row;
} stash_reply_t;

typedef list_t stash_attrlist_t;


#define STASH_VALTYPE_INT   1
#define STASH_VALTYPE_STR   2
#define STASH_VALTYPE_AUTO  3
typedef struct {
	short int valtype;
	union {
		char *str;			// STASH_VALTYPE_STR
		int number;			// STASH_VALTYPE_INT
	} value;
	unsigned int datalen;
} stash_value_t;


#define STASH_CONDTYPE_EQUALS 1
#define STASH_CONDTYPE_NAME   2
#define STASH_CONDTYPE_AND    3
#define STASH_CONDTYPE_OR     4
#define STASH_CONDTYPE_NOT    5
#define STASH_CONDTYPE_EXISTS 6
#define STASH_CONDTYPE_GT     7
typedef struct __stash_cond_t {
	short int condtype;
	stash_keyid_t kid;
	void *key_ptr;
	stash_value_t *value;
	stash_nameid_t nameid;
	char *name;
	void *name_ptr;
	struct __stash_cond_t *ca, *cb;
} stash_cond_t;


// initialise the stash_t structure.  If a NULL is passed in, a new object is 
// created for you, alternatively, you can pass in a pointer to an object you 
// want to control.... normally just pass a NULL and let us take care of it.
stash_t * stash_init(stash_t *stash);


// shutdown connections to the servers.
void    stash_shutdown(stash_t *stash);


// free all the resources used by the stash object.
void    stash_free(stash_t *stash);


// add authorization info to the stash.
stash_result_t stash_authority(stash_t *stash, const char *username, const char *password);
stash_result_t stash_addserver(stash_t *stash, const char *host, int priority);

// set a connection string.  Does not initiate a connection.
void stash_connstr(stash_t *stash, const char *connstr);


// using the host info and authority already specified, connect to the database if not already connected.
stash_result_t stash_connect(stash_t *stash);


const char *stash_err_text(stash_result_t res);


stash_result_t stash_create_username(stash_t *stash, const char *newuser, stash_userid_t *uid);
stash_result_t stash_set_password(stash_t *stash, stash_userid_t uid, const char *username, const char *newpass);

stash_result_t stash_get_namespace_id(stash_t *stash, const char *namespace, stash_nsid_t *nsid);

stash_result_t stash_create_table(stash_t *stash, stash_nsid_t nsid, const char *tablename, int option_map, stash_tableid_t *tid);

// the attrlist is just a linklist, but we call it our own thing to avoid confusion.
stash_attrlist_t * stash_init_alist(stash_t *stash);
void stash_free_alist(stash_t *stash, stash_attrlist_t *alist);



stash_value_t * __value_str(const char *str);
stash_value_t * __value_int(int number);
stash_value_t * __value_auto(void);
stash_value_t * __value_blob(void *ptr, int len);

void stash_set_attr(stash_attrlist_t *alist, stash_keyid_t keyid, stash_value_t *value, stash_expiry_t expires);
void stash_build_value(expbuf_t *buf, stash_value_t *value);
stash_value_t * stash_parse_value(const risp_data_t *data, const risp_length_t length);
void stash_free_value(stash_value_t *value);

stash_reply_t * stash_create_row(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_nameid_t nameid, const char *name, stash_attrlist_t *alist, stash_expiry_t expires);
stash_reply_t * stash_set(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_attrlist_t *alist);
stash_reply_t * stash_expire(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_keyid_t keyid, stash_expiry_t expires);
stash_reply_t * stash_delete(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_keyid_t keyid);

void stash_return_reply(stash_reply_t *reply);

stash_keyid_t stash_get_key_id(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, const char *keyname);

stash_result_t stash_grant(stash_t *stash, stash_userid_t uid, stash_nsid_t nsid, stash_tableid_t tid, unsigned short rights);


stash_cond_t * __cond_key_equals(stash_keyid_t kid, stash_value_t *value);
stash_cond_t * __cond_key_gt(stash_keyid_t kid, stash_value_t *value);
stash_cond_t * __cond_key_exists(stash_keyid_t kid);
stash_cond_t * __cond_name(stash_nameid_t nameid, const char *name);
stash_cond_t * __cond_and(stash_cond_t *aa, stash_cond_t *bb);
stash_cond_t * __cond_or(stash_cond_t *aa, stash_cond_t *bb);
stash_cond_t * __cond_not(stash_cond_t *aa);
void stash_cond_free(stash_cond_t *cond);

typedef struct {
	stash_nsid_t nsid;
	stash_tableid_t tid;
	int limit;
	stash_cond_t *condition;
} stash_query_t;

stash_query_t * stash_query_new(stash_nsid_t nsid, stash_tableid_t tid);
void stash_query_free(stash_query_t *query);
void stash_query_condition(stash_query_t *query, stash_cond_t *condition);
void stash_query_limit(stash_query_t *query, int limit);
stash_reply_t * stash_query_execute(stash_t *stash, stash_query_t *query);

// the stash_query function is deprecated, and may not be supported in future versions.
stash_reply_t * stash_query(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, int limit, stash_cond_t *condition);

stash_result_t stash_get_user_id(stash_t *stash, const char *username, stash_userid_t *uid);
stash_result_t stash_get_table_id(stash_t *stash, stash_nsid_t nsid, const char *tablename, stash_userid_t *tid);

int stash_nextrow(stash_reply_t *reply);
const char * stash_getstr(stash_reply_t *reply, stash_keyid_t key);
int stash_getint(stash_reply_t *reply, stash_keyid_t key);
stash_rowid_t stash_rowid(stash_reply_t *reply);
int stash_getlength(stash_reply_t *reply, stash_keyid_t key);

void stash_reply_reset(stash_reply_t *reply); 
void stash_sort(stash_reply_t *reply, stash_keyid_t key);

#endif
