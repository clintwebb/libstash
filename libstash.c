//-----------------------------------------------------------------------------
// libstash
// library interface to a stash service.   The default version uses ablocking 
// socket calls.  All operations block until the operation is complete.

#include "stash.h"

#include <arpa/inet.h>
#include <assert.h>
#include <netdb.h>
#include <netinet/in.h>
#include <rispbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>


#if (LIBSTASH_VERSION != 0x00000700)
#error "Incorrect stash.h header version."
#endif

#if (EXPBUF_VERSION < 0x00010230) 
#error "Requires libexpbuf v1.02.30 or higher"
#endif


#if (LIBLINKLIST_VERSION < 0x00009010)
#error "Requires liblinklist v0.90.10 or higher."
#endif


// when sorting the replies from a query, we use qsort which will not know which key to sort on.
static stash_keyid_t stash_sortkey = 0;


typedef struct {
	int handle;		// socket handle to the connected controller.
	char active;
	char closing;
	char shutdown;
	risp_t *risp;
	
	char *host;
	int port;
	
	expbuf_t *inbuf, *outbuf, *readbuf;
} conn_t;





// attribute pair.
typedef struct {
	stash_keyid_t keyid;
	stash_value_t *value;
	stash_expiry_t expires;
} attr_t;


typedef struct {
	int identifier;
	int count;
	stash_rowid_t rid;
	stash_nameid_t nid;
	list_t *attrlist;		// attr_t
	stash_reply_t *reply;
	short int done;
} replyrow_t;



static void cmdReplyReqID(stash_reply_t *reply, risp_int_t value)
{
	assert(reply);
	reply->reqid = value;
}

static void cmdReplyUserID(stash_reply_t *reply, risp_int_t value)
{
	assert(reply);
	assert(value > 0);
	
	assert(reply->uid == 0);
	reply->uid = value;
}

static void cmdReplyNamespaceID(stash_reply_t *reply, risp_int_t value)
{
	assert(reply);
	assert(value > 0);
	
	assert(reply->nsid == 0);
	reply->nsid = value;
}

static void cmdReplyTableID(stash_reply_t *reply, risp_int_t value)
{
	assert(reply);
	assert(value > 0);
	
	assert(reply->tid == 0);
	reply->tid = value;
}

static void cmdReplyKeyID(stash_reply_t *reply, risp_int_t value)
{
	assert(reply);
	assert(value > 0);
	
	assert(reply->kid == 0);
	reply->kid = value;
}

static void cmdReplyCount(stash_reply_t *reply, risp_int_t value)
{
	assert(reply && value >= 0);
	
	assert(reply->row_count == 0);
	reply->row_count = value;
}


static void cmdReplyRow(stash_reply_t *reply, const risp_length_t length, const risp_data_t *data)
{
	int cnt;
	replyrow_t *row;
	risp_length_t processed;
	
	assert(reply && length > 0 && data);
	assert(reply->stash);

	// create a row object.
	row = calloc(1, sizeof(*row));
	row->identifier = 0x1234;
	row->attrlist = ll_init(NULL);
	row->reply = reply;
	
	// risp_row should not have anything in it, as it is purely a callback 
	// process.  Therefore, we do not need to clear it.
	
	// we have the contents of the row, we need to parse them.
	assert(reply->stash->risp_row);
	processed = risp_process(reply->stash->risp_row, row, length, data);
	assert(processed == length);
	
	// add the row to the reply list.
	assert(reply->rows);
	ll_push_tail(reply->rows, row);
	
#ifndef NDEBUG
	// check for any values we didn't handle.
	for (cnt=0; cnt<256; cnt++) {
		if (reply->stash->risp_row->commands[cnt].handler == NULL) {
			if (reply->stash->risp_row->commands[cnt].set) {
				printf("cmdReplyRow: Unexpected row param: %d\n", cnt);
			}
		}
	}
#endif
}




static void cmdRowCount(replyrow_t *row, risp_int_t value)
{
	assert(row && value >= 0);
	assert(row->count == 0);
	row->count = value;
}


static void cmdRowNameID(replyrow_t *row, risp_int_t value)
{
	assert(row && value > 0);
	assert(row->nid == 0);
	row->nid = value;
}


static void cmdRowRowID(replyrow_t *row, risp_int_t value)
{
	assert(row && value > 0);
	assert(row->rid == 0);
	row->rid = value;
}


static void cmdRowAttribute(replyrow_t *row, const risp_length_t length, const risp_data_t *data)
{
	int cnt;
	attr_t *attr;
	risp_length_t processed;
	
	assert(row && length > 0 && data);
	assert(row->reply);
	assert(row->reply->stash);
	
	// create a row object.
	attr = calloc(1, sizeof(*attr));
	assert(attr);
	
	// risp_row should not have anything in it, as it is purely a callback 
	// process.  Therefore, we do not need to clear it.
	
	// we have the contents of the row, we need to parse them.
	assert(row->reply->stash->risp_attr);
	processed = risp_process(row->reply->stash->risp_attr, attr, length, data);
	assert(processed == length);
	
	// add the row to the reply list.
	assert(row->attrlist);
	ll_push_tail(row->attrlist, attr);
	
	// check for any values we didn't handle.
#ifndef NDEBUG
	for (cnt=0; cnt<256; cnt++) {
		if (row->reply->stash->risp_attr->commands[cnt].handler == NULL) {
			if (row->reply->stash->risp_attr->commands[cnt].set) {
				printf("cmdReplyRow: Unexpected row param: %d\n", cnt);
			}
		}
	}
#endif
}


static void cmdAttrKeyID(attr_t *attr, risp_int_t value)
{
	assert(attr && value > 0);
	
	assert(attr->keyid == 0);
	attr->keyid = value;
}

static void cmdAttrValue(attr_t *attr, const risp_length_t length, const risp_data_t *data)
{
	assert(attr && length > 0 && data);

	assert(attr->value == NULL);
	attr->value = stash_parse_value(data, length);
	assert(attr->value);
}




//-----------------------------------------------------------------------------
// initialise the stash_t structure.  If a NULL is passed in, a new object is 
// created for you, alternatively, you can pass in a pointer to an object you 
// want to control.... normally just pass a NULL and let us take care of it.
stash_t * stash_init(stash_t *stash)
{
	stash_t *s;
	
	if (stash) {
		s = stash;
		s->internally_created = 0;
	}
	else {
		s = calloc(1, sizeof(stash_t));
		s->internally_created = 1;
	}
	
	s->risp = risp_init(NULL);
	assert(s->risp != NULL);
//	risp_add_command(control->risp, HTTP_CMD_CLEAR,        &cmdClear);
//	risp_add_command(control->risp, HTTP_CMD_FILE,         &cmdFile);
//	risp_add_command(control->risp, HTTP_CMD_CONTENT_TYPE, &cmdContentType);
//	risp_add_command(control->risp, HTTP_CMD_REPLY,        &cmdReply);

	s->risp_reply = risp_init(NULL);
	assert(s->risp_reply);
	risp_add_command(s->risp_reply, STASH_CMD_REQUEST_ID,   &cmdReplyReqID);
	risp_add_command(s->risp_reply, STASH_CMD_USER_ID,      &cmdReplyUserID);
	risp_add_command(s->risp_reply, STASH_CMD_NAMESPACE_ID, &cmdReplyNamespaceID);
	risp_add_command(s->risp_reply, STASH_CMD_TABLE_ID,     &cmdReplyTableID);
	risp_add_command(s->risp_reply, STASH_CMD_KEY_ID,       &cmdReplyKeyID);
	risp_add_command(s->risp_reply, STASH_CMD_ROW,          &cmdReplyRow);
	risp_add_command(s->risp_reply, STASH_CMD_COUNT,        &cmdReplyCount);
	
	s->risp_failed = risp_init(NULL);
	assert(s->risp_failed);
	
	s->risp_row = risp_init(NULL);
	assert(s->risp_row);
	risp_add_command(s->risp_row, STASH_CMD_COUNT,          &cmdRowCount);
	risp_add_command(s->risp_row, STASH_CMD_ATTRIBUTE,      &cmdRowAttribute);
	risp_add_command(s->risp_row, STASH_CMD_NAME_ID,        &cmdRowNameID);
	risp_add_command(s->risp_row, STASH_CMD_ROW_ID,         &cmdRowRowID);
	
	s->risp_attr = risp_init(NULL);
	assert(s->risp_attr);
	risp_add_command(s->risp_attr, STASH_CMD_KEY_ID,        &cmdAttrKeyID);
	risp_add_command(s->risp_attr, STASH_CMD_VALUE,         &cmdAttrValue);
	
	
	
	// linked-list of our connections.  Only the one at the head is likely to be
	// active (although it might not be).  When a connection is dropped or is
	// timed out, it is put at the bottom of the list.
	s->connlist = ll_init(NULL);		/// stash_conn_t
	assert(s->connlist);
	
	// the replies will be kept in this list.  the unused one will be at the 
	// head, the used ones will be at the tail.
	s->replypool = ll_init(NULL);
	assert(s->replypool);
	
	
	s->readbuf = expbuf_init(NULL, 1024);
	assert(s->readbuf);
	
	s->buf_set = expbuf_init(NULL, 32);
	assert(s->buf_set);
	
	s->buf_attr = expbuf_init(NULL, 32);
	assert(s->buf_attr);
	
	s->buf_value = expbuf_init(NULL, 32);
	assert(s->buf_value);
	
	s->buf_payload = expbuf_init(NULL, 64);
	assert(s->buf_payload);
	
	s->buf_request = expbuf_init(NULL, 128);
	assert(s->buf_request);
	
	s->uid = 0;
	s->username = NULL;
	s->password = NULL;
	
	s->next_reqid = 1;
	
	return(s);
}


// free internal resources.
static void reply_free(stash_reply_t *reply)
{
	assert(reply->rows);
	assert(ll_count(reply->rows) == 0);
	reply->rows = ll_free(reply->rows);
	assert(reply->rows == NULL);

	free(reply);
}

static void conn_free(conn_t *conn)
{
	assert(conn->host);
	free(conn->host);
	
	if (conn->risp) {
		// need to shutdown a risp object for the connection?
		assert(0);
	}
	
	assert(conn->inbuf);
	assert(conn->outbuf);
	assert(conn->readbuf);
	
	conn->inbuf = expbuf_free(conn->inbuf);
	assert(conn->inbuf == NULL);
	
	conn->outbuf = expbuf_free(conn->outbuf);
	assert(conn->outbuf == NULL);
	
	conn->readbuf = expbuf_free(conn->readbuf);
	assert(conn->readbuf == NULL);
	
	free(conn);
}

//-----------------------------------------------------------------------------
// free all the resources used by the stash object.
void stash_free(stash_t *stash)
{
	conn_t *conn;
	stash_reply_t *reply;
	
	assert(stash);
	
	
	assert(stash->readbuf);
	assert(BUF_LENGTH(stash->readbuf) == 0);
	stash->readbuf = expbuf_free(stash->readbuf);
	assert(stash->readbuf == NULL);
	
	assert(stash->buf_set);
	assert(BUF_LENGTH(stash->buf_set) == 0);
	stash->buf_set = expbuf_free(stash->buf_set);
	assert(stash->buf_set == NULL);
	
	assert(stash->buf_attr);
	assert(BUF_LENGTH(stash->buf_attr) == 0);
	stash->buf_attr = expbuf_free(stash->buf_attr);
	assert(stash->buf_attr == NULL);
	
	assert(stash->buf_value);
	assert(BUF_LENGTH(stash->buf_value) == 0);
	stash->buf_value = expbuf_free(stash->buf_value);
	assert(stash->buf_value == NULL);
	
	assert(stash->buf_payload);
	assert(BUF_LENGTH(stash->buf_payload) == 0);
	stash->buf_payload = expbuf_free(stash->buf_payload);
	assert(stash->buf_payload == NULL);
	
	assert(stash->buf_request);
	assert(BUF_LENGTH(stash->buf_request) == 0);
	stash->buf_request = expbuf_free(stash->buf_request);
	assert(stash->buf_request == NULL);
	
	assert(stash->connlist);
	while ((conn = ll_pop_head(stash->connlist))) {
		conn_free(conn);
	}
	stash->connlist = ll_free(stash->connlist);
	assert(stash->connlist == NULL);
	
	assert(stash->replypool);
	while ((reply = ll_pop_head(stash->replypool)))
	{
		reply_free(reply);
	}
	stash->replypool = ll_free(stash->replypool);
	assert(stash->replypool == NULL);
	
	
	assert(stash->risp);
	risp_shutdown(stash->risp);
	stash->risp = NULL;
	
	assert(stash->risp_reply);
	risp_shutdown(stash->risp_reply);
	stash->risp_reply = NULL;
	
	assert(stash->risp_failed);
	risp_shutdown(stash->risp_failed);
	stash->risp_failed = NULL;
	
	assert(stash->risp_row);
	risp_shutdown(stash->risp_row);
	stash->risp_row = NULL;
	
	assert(stash->risp_attr);
	risp_shutdown(stash->risp_attr);
	stash->risp_attr = NULL;

	if (stash->username) { free(stash->username); stash->username = NULL; }
	if (stash->password) { free(stash->password); stash->password = NULL; }
	
	if (stash->internally_created > 0) {
		free(stash);
	}
}



stash_result_t stash_authority(stash_t *stash, const char *username, const char *password)
{
	assert(stash);
	assert(username);
	assert(password);
	
	// check the authority list to see if this username is already in there.  If so, overwrite with new password.
	// if not there, then add a new entry.
	stash->username = strdup(username);
	stash->password = strdup(password);

	return(STASH_ERR_OK);
}


// add a server to the list.
stash_result_t stash_addserver(stash_t *stash, const char *host, int priority)
{
	conn_t *conn;
	char *copy;
	char *first;
	char *next;
	
	assert(stash);
	assert(host);
	
	conn = calloc(1, sizeof(conn_t));
	assert(conn);

	conn->handle = -1;		// socket handle to the connected controller.
	conn->active = 0;
	conn->closing = 0;
	conn->shutdown = 0;
	conn->risp = NULL;
	
	conn->inbuf = expbuf_init(NULL, 0);
	conn->outbuf = expbuf_init(NULL, 0);
	conn->readbuf = expbuf_init(NULL, 0);
	
	// parse the host string, to remove the port part.
	copy = strdup(host);
	assert(copy);
	next = copy;
	first = strsep(&next, ":");
	assert(first == copy);
	
	if (next == NULL) {
		// no port was supplied.
		conn->port = STASH_DEFAULT_PORT;
		conn->host = strdup(host);
	}
	else {
		conn->port = atoi(next);
		conn->host = strdup(first);
	}
	
	free (copy);

	// add the conn to the list.
	assert(stash->connlist);
	ll_push_head(stash->connlist, conn);

	return(STASH_ERR_OK);
}


// format of the string is: username/password@server:port,server:port,server:port
// port is optional, the rest are required.
// there can be any number of server:port entries.
void stash_connstr(stash_t *stash, const char *connstr)
{
	char *ptr;
	char buffer[1024];
	int blok;
	char *user;
	
	assert(stash && connstr);
	
	// we will set ptr to be NULL when we are finished looping.
	blok = 0;
	ptr = (char *) connstr;
	assert(*ptr);
	
	user = NULL;
	while (ptr) {
		
		if (*ptr == '/') {
			assert(user == NULL);
			assert(blok > 0);
			buffer[blok] = 0;
			user = strdup(buffer);
			blok = 0;
		}
		else if (*ptr == '@') {
			assert(user);
			assert(blok > 0);
			buffer[blok] = 0;
			blok = 0;
			
			stash_authority(stash, user, buffer);
			free(user);
			user = NULL;
		}
		else if (*ptr == ',' || *ptr == 0) {
			assert(blok > 0);
			buffer[blok] = 0;
			
			stash_addserver(stash, buffer, 10);
			blok = 0;
		}
		else {
			assert(blok >= 0 && blok < 1024);
			buffer[blok] = *ptr;
			blok ++;
			assert(blok > 0 && blok < 1024);
		}
		
		// if we've reached the end of the string, set ptr to NULL so we break out of the loop.  Otherwise, go to the next character.
		if (*ptr == 0) { ptr = NULL; }
		else { ptr ++; }
	}
}



const char * stash_err_text(stash_result_t res)
{
	const char *text = NULL;
	
	assert(res >= 0);
	
	switch(res) {
		case STASH_ERR_OK:
			text = "No error";
			break;
			
		case STASH_ERR_NOTCONNECTED:
			text = "Not connected";
			break;
			
		case STASH_ERR_USEREXISTS:
			text = "Username exists";
			break;
			
		case STASH_ERR_AUTHFAILED:
			text = "Authorization failed";
			break;
			
		case STASH_ERR_INSUFFICIENTRIGHTS:
			text = "Insufficient Rights for this operation";
			break;
			
		case STASH_ERR_TABLEEXISTS:
			text = "Table name already exists";
			break;
			
		default:
			text = "Unknown error code";
			break;
	}
	
	assert(text);
	return (text);
}


static int sock_resolve(const char *szAddr, int iPort, struct sockaddr_in *pSin)
{
	unsigned long ulAddress;
	struct hostent *hp;
	
	assert(szAddr != NULL && szAddr[0] != '\0' && iPort > 0);
	assert(pSin != NULL);
	
	// First, assign the family and port.
	pSin->sin_family = AF_INET;
	pSin->sin_port = htons(iPort);
	
	// Look up by standard notation (xxx.xxx.xxx.xxx) first.
	ulAddress = inet_addr(szAddr);
	if ( ulAddress != (unsigned long)(-1) )  {
		// Success. Assign, and we're done.  Since it was an actual IP address, then we dont doany DNS lookup for that, so we cant do any check ing for any other address type (such as MX).
		pSin->sin_addr.s_addr = ulAddress;
		return 0;
	}
	
	
	// If that didn't work, try to resolve host name by DNS.
	hp = gethostbyname(szAddr);
	if( hp == NULL ) {
		// Didn't work. We can't resolve the address.
		return -1;
	}
	
	// Otherwise, copy over the converted address and return success.
	memcpy( &(pSin->sin_addr.s_addr), &(hp->h_addr[0]), hp->h_length);
	return 0;
}



int sock_connect(char *szHost, int nPort)
{
	int nSocket = -1;
	struct sockaddr_in sin;
	
	assert(szHost != NULL);
	assert(nPort > 0);
	
	if (sock_resolve(szHost,nPort,&sin) >= 0) {
		// CJW: Create the socket
		nSocket = socket(AF_INET,SOCK_STREAM,0);
		if (nSocket >= 0) {
			// CJW: Connect to the server
			if (connect(nSocket, (struct sockaddr*)&sin, sizeof(struct sockaddr)) < 0) {
				// the connect failed.
				close(nSocket);
				nSocket = -1;
			}
		}
	}
	
	return(nSocket);
}







static void reply_clear(stash_reply_t *reply)
{
	assert(reply);
	
	assert(reply->stash);
	reply->resultcode = STASH_ERR_OK;
	reply->operation = 0;
	reply->uid = 0;
	reply->nsid = 0;
	reply->tid = 0;
	reply->kid = 0;
	reply->row_count = 0;
	reply->curr_row = -1;
	
	assert(reply->rows);
	assert(ll_count(reply->rows) == 0);
}


// even though the library can only have one outstanding communication with the 
// server at a time, we can have multiple replies going at the same time....
static stash_reply_t * getreply(stash_t *stash)
{
	stash_reply_t *reply;
	
	assert(stash->replypool);
	reply = ll_get_head(stash->replypool);
	if (reply == NULL || reply->operation == 0) {
		reply = calloc(1, sizeof(stash_reply_t));
		reply->stash = stash;
		reply->rows = ll_init(NULL);
		assert(reply->rows);
		
		reply_clear(reply);
	}
	
	return(reply);
}


//-----------------------------------------------------------------------------
// given a loaded risp object, parse out the details into a reply.  This is a 
// complicated function because it needs to be able to parse out all the 
// different replies possible.
static stash_reply_t * parsereply(stash_t *stash, risp_t *risp)
{
	stash_reply_t *reply;
	risp_length_t datalen;
	risp_data_t *data;
	risp_length_t processed;
	int cnt;
	
	assert(stash && risp);
	
	reply = getreply(stash);
	assert(reply);
	
	if (risp_isset(risp, STASH_CMD_FAILED)) {
		datalen = risp_getlength(risp, STASH_CMD_FAILED);
		data = risp_getdata(risp, STASH_CMD_FAILED);
		
		assert(datalen > 0);
		assert(data);
		
		// need to get the failcode parsed out.
		assert(stash->risp_failed);
		processed = risp_process(stash->risp_failed, NULL, datalen, data);
		assert(processed == datalen);
		assert(risp_isset(stash->risp_failed, STASH_CMD_FAILCODE));
		reply->resultcode = risp_getvalue(stash->risp_failed, STASH_CMD_FAILCODE);
		assert(reply->resultcode > 0);
	}
	else if (risp_isset(risp, STASH_CMD_REPLY)) {
		datalen = risp_getlength(risp, STASH_CMD_REPLY);
		data = risp_getdata(risp, STASH_CMD_REPLY);
		
		assert(stash->risp_reply);
		assert(datalen > 0);
		assert(data);
		processed = risp_process(stash->risp_reply, reply, datalen, data);
		assert(processed == datalen);
		
#ifndef NDEBUG
		// check for unprocessed params.
		for (cnt=0; cnt<256; cnt++) {
			if (stash->risp_reply->commands[cnt].handler == NULL) {
				if (stash->risp_reply->commands[cnt].set) {
					printf("parsereply: Unexpected reply param: %d\n", cnt);
				}
			}
		}
#endif
	}
	else {
		// we've got something back that wasn't expected.
		assert(0);
	}
	
	return(reply);
}


//-----------------------------------------------------------------------------
// TODO: This function needs a lot of work.   It should be worked a little bit 
//       better.   Not sure exactly of the best way, just know that what we 
//       have already doesn't look very good.
static stash_reply_t * send_request(stash_t *stash, risp_command_t cmd, expbuf_t *data)
{
	stash_reply_t *reply = NULL;
	conn_t *conn;
	ssize_t sent;
	risp_t *risp;
	
	assert(stash && cmd > 0 && data);
	assert(stash->next_reqid > 0);

	assert(stash->buf_payload);
	assert(BUF_LENGTH(stash->buf_payload) == 0);
	
	assert(stash->buf_request);
	assert(BUF_LENGTH(stash->buf_request) == 0);
	
	// build the operation that will contain the data, based on the command range.
	rispbuf_addInt(stash->buf_payload, STASH_CMD_REQUEST_ID, stash->next_reqid);
	rispbuf_addBuffer(stash->buf_payload, cmd, data);
	rispbuf_addBuffer(stash->buf_request, STASH_CMD_REQUEST, stash->buf_payload);
	
	stash->next_reqid++;
	assert(stash->next_reqid > 0);
	
	// clear the buffer we dont need anymore.
	expbuf_clear(stash->buf_payload);
	
	// ensure we are connected.
	assert(stash->connlist);
	conn = ll_get_head(stash->connlist);
	assert(conn);
	if (conn->active == 0) {
		// we dont have an active connection... we need to try and make one.
		assert(0);
	}
	assert(conn->closing == 0);
	assert(conn->shutdown == 0);
	assert(conn->handle > 0);
	
	// send the data
	reply = NULL;
	while (BUF_LENGTH(stash->buf_request) > 0) {
		sent = send(conn->handle, BUF_DATA(stash->buf_request), BUF_LENGTH(stash->buf_request), 0);
		assert(sent != 0);
		assert(sent <= BUF_LENGTH(stash->buf_request));
		if (sent < 0) {
			// connection to the server has closed....
			expbuf_clear(stash->buf_request);
			
			conn->handle = -1;
			conn->active = 0;
			ll_move_tail(stash->connlist, conn);
		}
		else {
			expbuf_purge(stash->buf_request, sent);
		}
	}
	
	// now that we have sent everything (or tried to), we can clear the request buffer.
	expbuf_clear(stash->buf_request);

	if (conn->active == 0) {
		// we lost connection.  make sure we will return a NULL.
		assert(reply == NULL);
	}
	else {
		
		int done;
		int avail;
		risp_length_t processed;
		
		risp = risp_init(NULL);
	
		// read data from the socket
		// while not everything has been received (because we cant process it with risp), read more data.
		assert(stash->readbuf);
		assert(BUF_MAX(stash->readbuf) > 0);
		assert(BUF_LENGTH(stash->readbuf) == 0);
		done = 0;
		while (done == 0) {
			
			avail = BUF_MAX(stash->readbuf) - BUF_LENGTH(stash->readbuf);
			assert(avail > 0);
			sent = recv(conn->handle, BUF_DATA(stash->readbuf)+BUF_LENGTH(stash->readbuf), avail, 0);
			if (sent <= 0) {
				// socket has shutdown
				assert(0);
			}
			else {
				assert(sent <= avail);
				BUF_LENGTH(stash->readbuf) += sent;
				
				// now that we have more data, attempt to parse it into risp.  
				// If it succeeds, then we have all we need to get.
				processed = risp_process(risp, NULL, BUF_LENGTH(stash->readbuf), BUF_DATA(stash->readbuf));
				if (processed > 0) {
					assert(processed == BUF_LENGTH(stash->readbuf));
					done = 1;
				}
				else {
					assert(processed == 0);
					assert(done == 0);
					
					// if we dont have more room in the buffer, then increase its size by 1k.
					if ((BUF_MAX(stash->readbuf) - BUF_LENGTH(stash->readbuf)) < 1024) {
						expbuf_shrink(stash->readbuf, 1024);
					}
				}
			}
		}
		
		expbuf_clear(stash->readbuf);
		
		if (processed <= 0) {
			// failed to receive reply.
			assert(reply == NULL);
		}
		else {
			// when we have everything, get a fresh reply structure.
			assert(risp);
			reply = parsereply(stash, risp);
			assert(reply);
		}
			
		// we dont need the RISP object anymore... we can remove it.
		risp_shutdown(risp);
		risp = NULL;
	}
	
	assert(BUF_LENGTH(stash->readbuf) == 0);
	
	return(reply);
}




// do nothing if we are already connected.  If we are not connected, then go 
// through the list for the best one and connect to it.  Since we are setup 
// for blocking acticity, we will wait until the connect succeeds or fails.
stash_result_t stash_connect(stash_t *stash)
{
	stash_result_t res = STASH_ERR_OK;
	conn_t *conn;
	stash_reply_t *reply = NULL;
	
	// TODO: need to go thru the list for the best candidate.
	
	assert(stash);
	if (stash->username == NULL || stash->password == NULL) {
		res = STASH_ERR_NOTCONNECTED;
	}
	else {
		
		assert(stash->connlist);
		conn = ll_get_head(stash->connlist);
		assert(conn);
		
		assert(conn->handle < 0);
		assert(conn->active == 0);
		assert(conn->closing == 0);
		assert(conn->shutdown == 0);
		
		assert(conn->inbuf);
		assert(conn->outbuf);
		assert(conn->readbuf);
		
		assert(conn->host);
		assert(conn->port > 0);
		if (conn->handle < 0) {
			conn->handle = sock_connect(conn->host, conn->port);
			assert(res == STASH_ERR_OK);
		}
		
		if (conn->handle < 0) {
			res = STASH_ERR_NOTCONNECTED;
			assert(conn->active == 0);
		}
		else {
			assert(res == STASH_ERR_OK);
			conn->active = 1;
			
			// if we have authority (which we should), we need to send off a login.
			assert(stash->username && stash->password);
			assert(stash->uid == 0);
			
			// get a buffer and bui
			assert(stash->buf_set);
			assert(BUF_LENGTH(stash->buf_set) == 0);
			rispbuf_addStr(stash->buf_set, STASH_CMD_USERNAME, strlen(stash->username), stash->username);
			rispbuf_addStr(stash->buf_set, STASH_CMD_PASSWORD, strlen(stash->password), stash->password);
			
			// send the request and receive the reply.
			reply = send_request(stash, STASH_CMD_LOGIN, stash->buf_set);
			
			expbuf_clear(stash->buf_set);
			
			// process the reply and store the results in the data pointers that was provided.
			if (reply->resultcode == STASH_ERR_OK) {
				assert(reply->uid > 0);
				stash->uid = reply->uid;
				assert(conn->active == 1);
				assert(res == STASH_ERR_OK);
			}
			else {
				res = reply->resultcode;
				conn->active = 0;
			}
			
			reply_free(reply);
			
			
		}
		
		
		
		assert(conn->inbuf);
		assert(conn->outbuf);
		assert(conn->readbuf);
		assert(BUF_LENGTH(conn->inbuf) == 0);
		assert(BUF_LENGTH(conn->outbuf) == 0);
		assert(BUF_LENGTH(conn->readbuf) == 0);
	}
	
	return(res);
}




// This function will need to build the request to create a username, send it over the active connection (if there is one)
stash_result_t stash_create_username(stash_t *stash, const char *newuser, stash_userid_t *uid)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash);
	assert(newuser);
	assert(uid);

	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	rispbuf_addStr(data, STASH_CMD_USERNAME, strlen(newuser), newuser);

	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_CREATE_USER, data);

	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	if (res == STASH_ERR_OK) {
		assert(reply->uid > 0);
		*uid = reply->uid;
	}
	
	stash_return_reply(reply);
	
	return(res);
}


stash_result_t stash_set_password(stash_t *stash, stash_userid_t uid, const char *username, const char *newpass)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash);
	assert(uid > 0 || username);
	assert(newpass);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	if (uid > 0) {
		rispbuf_addInt(data, STASH_CMD_USER_ID, uid);
	}
	else {
		assert(username);
		rispbuf_addStr(data, STASH_CMD_USERNAME, strlen(username), username);
	}
	rispbuf_addStr(data, STASH_CMD_PASSWORD, strlen(newpass), newpass);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_SET_PASSWORD, data);

	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	
	stash_return_reply(reply);
	
	return(res);
}


stash_result_t stash_get_namespace_id(stash_t *stash, const char *namespace, stash_nsid_t *nsid)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash);
	assert(namespace);
	assert(nsid);

	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	
	rispbuf_addStr(data, STASH_CMD_NAMESPACE, strlen(namespace), namespace);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_GETID, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	if (res == STASH_ERR_OK) {
		*nsid = reply->nsid;
	}
	reply_free(reply);
	
	return(res);
}


stash_result_t stash_create_table(stash_t *stash, stash_nsid_t nsid, const char *tablename, int option_map, stash_tableid_t *tid)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash);
	assert(nsid > 0);
	assert(tablename);
	assert(tid);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	
	rispbuf_addInt(data, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addStr(data, STASH_CMD_TABLE, strlen(tablename), tablename);

	if (option_map & STASH_TABOPT_STRICT) rispbuf_addCmd(data, STASH_CMD_STRICT);
	if (option_map & STASH_TABOPT_UNIQUE) rispbuf_addCmd(data, STASH_CMD_UNIQUE);
	if (option_map & STASH_TABOPT_OVERWRITE) rispbuf_addCmd(data, STASH_CMD_OVERWRITE);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_CREATE_TABLE, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	if (res == STASH_ERR_OK) {
		assert(reply->tid > 0);
		*tid = reply->tid;
		assert(*tid > 0);
	}
	reply_free(reply);
	
	return(res);
}


// we pass in the stash object, because at some point we will probably want to pool these list objects, because we will probably be creating a lot of them.
stash_attrlist_t * stash_init_alist(stash_t *stash)
{
	stash_attrlist_t *list;
	
	assert(stash);
	assert(sizeof(stash_attrlist_t) == sizeof(list_t));
	
	list = ll_init(NULL);
	assert(list);
	return (list);
}



void stash_free_alist(stash_t *stash, stash_attrlist_t *alist)
{
	attr_t *attr;
	
	assert(stash);
	assert(alist);
	
	// need to go thru the list and free all the elements.
	while (( attr = ll_pop_head(alist))) {
		// free the attribute.
		assert(attr->keyid >= 0);
		assert(attr->value);

		stash_free_value(attr->value);
		free(attr);
	}
	
	alist = ll_free(alist);
	assert(alist == NULL);
}


void stash_set_attr(stash_attrlist_t *alist, stash_keyid_t keyid, stash_value_t *value, stash_expiry_t expires)
{
	attr_t *attr;
	
	assert(alist && keyid > 0 && value && expires >= 0);
	
	attr = calloc(1, sizeof(attr_t));
	assert(attr);
	
	attr->keyid = keyid;
	attr->value = value;
	attr->expires = expires;
	
	ll_push_tail(alist, attr);
}



stash_value_t * __value_str(const char *str)
{
	stash_value_t *val;
	
	assert(str);
	
	val = calloc(1, sizeof(*val));
	assert(val);
	
	val->valtype = STASH_VALTYPE_STR;
	val->value.str = strdup(str);
	val->datalen = strlen(str);
	assert(val->datalen > 0);
	
	return(val);
}

stash_value_t * __value_blob(void *ptr, int len)
{
	stash_value_t *val;
	
	assert(ptr && len >= 0);
	
	assert(sizeof(size_t) == sizeof(len));
	
	val = calloc(1, sizeof(*val));
	assert(val);
	
	val->valtype = STASH_VALTYPE_STR;
	val->value.str = malloc(len);
	assert(val->value.str);
	memcpy(val->value.str, ptr, len);
	val->datalen = len;
	assert(val->datalen > 0);
	
	return(val);
}


stash_value_t * __value_int(int number)
{
	stash_value_t *val;
	
	val = calloc(1, sizeof(*val));
	assert(val);
	
	val->valtype = STASH_VALTYPE_INT;
	val->value.number = number;
	
	return(val);
}

stash_value_t * __value_auto(void)
{
	stash_value_t *val;
	val = calloc(1, sizeof(*val));
	assert(val);
	val->valtype = STASH_VALTYPE_AUTO;
	return(val);
}


// PERF: use a pool of values so that we dont need to keep malloc'ing new ones.
// PERF: use a pool of same sized data buffers.
// PERF: use a single RISP instance instead of creating a new one each time.
stash_value_t * stash_parse_value(const risp_data_t *data, const risp_length_t length)
{
	stash_value_t *value;
	risp_t *rr;
	risp_length_t processed;
	char *str;
	
	assert(data && length > 0);
	
	value = calloc(1, sizeof(*value));
	assert(value);
	
	rr = risp_init(NULL);
	assert(rr);
	
	processed = risp_process(rr, NULL, length, data);
// 	printf("stash_parse_value:  processed=%d, length=%d\n", processed, length);
	assert(processed == length);
	
	if (risp_isset(rr, STASH_CMD_INTEGER)) {
		value->datalen = 0;
		value->valtype = STASH_VALTYPE_INT;
		value->value.number = risp_getvalue(rr, STASH_CMD_INTEGER);
	}
	else if (risp_isset(rr, STASH_CMD_STRING)) {
		value->datalen = risp_getlength(rr, STASH_CMD_STRING);
		value->valtype = STASH_VALTYPE_STR;
		if (value->datalen > 0) {
			str = risp_getstring(rr, STASH_CMD_STRING);
			value->value.str = strdup(str);
		}
		else {
			value->value.str = NULL;
		}
	}
	else if (risp_isset(rr, STASH_CMD_AUTO)) {
		value->datalen = 0;
		value->valtype = STASH_VALTYPE_AUTO;
		value->value.number = 0;
	}
	else {
		assert(0);
	}
	
	rr = risp_shutdown(rr);
	assert(rr == NULL);
	
	return(value);
}

// release the contents of the value object.
void stash_free_value(stash_value_t *value)
{
	assert(value);
	
	if (value->valtype == STASH_VALTYPE_STR) {
		if (value->datalen > 0) {
			assert(value->value.str);
			free(value->value.str);
		}
		else {
			assert(value->value.str == NULL);
		}
	}
	free(value);
}


void stash_build_value(expbuf_t *buf, stash_value_t *value)
{
	assert(buf && value);
	
	assert(BUF_LENGTH(buf) == 0);
	
	switch (value->valtype) {
		
		case STASH_VALTYPE_INT:
			rispbuf_addInt(buf, STASH_CMD_INTEGER, value->value.number);
			break;

		case STASH_VALTYPE_STR:
			assert(value->datalen >= 0);
			if (value->datalen == 0) {
				rispbuf_addCmd(buf, STASH_CMD_NULL);
			}
			else {
				assert(value->value.str);
				rispbuf_addStr(buf, STASH_CMD_STRING, value->datalen, value->value.str);
			}
			break;
			
		case STASH_VALTYPE_AUTO:
			rispbuf_addCmd(buf, STASH_CMD_AUTO);
// 			printf("stash_build_value: AUTO\n");
			break;
	
		default:
			assert(0);
		// 				STASH_CMD_AUTO            [optional]
		// 				STASH_CMD_DATETIME <str>  [optional]
		// 				STASH_CMD_DATE <int32>    [optional]
		// 				STASH_CMD_TIME <int32>    [optional]
		// 				STASH_CMD_HASHMAP <>      [optional]
		// 					STASH_CMD_KEY 
		// 					STASH_CMD_VALUE <>
		// 						...
			break;
	}
	
// 	printf("value len - %d\n", BUF_LENGTH(buf));
}


//-----------------------------------------------------------------------------
// This is a pretty important function.  It needs to add a row into a table, 
// and set the initial attributes.
stash_reply_t *
	stash_create_row(
		stash_t *stash, 
		stash_nsid_t nsid, 
		stash_tableid_t tid, 
		stash_nameid_t nameid, 
		const char *name, 
		stash_attrlist_t *alist,
		stash_expiry_t expires)
{
	stash_reply_t *reply;
	attr_t *attr;
	
	assert(stash);
	assert(nsid > 0);
	assert(tid > 0);
	assert((nameid == 0 && name) || (nameid > 0 && name == NULL));
	assert((alist == NULL) || (alist && ll_count(alist)));
	assert(expires >= 0);
	
	// get a buffer and bui
	assert(stash->buf_set);
	assert(BUF_LENGTH(stash->buf_set) == 0);

	assert(stash->buf_attr);
	assert(BUF_LENGTH(stash->buf_attr) == 0);
	
	assert(stash->buf_value);
	assert(BUF_LENGTH(stash->buf_value) == 0);
	
	
	rispbuf_addInt(stash->buf_set, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_TABLE_ID, tid);
	if (nameid > 0) {
		rispbuf_addInt(stash->buf_set, STASH_CMD_NAME_ID, nameid);
	}
	else {
		assert(name);
		rispbuf_addStr(stash->buf_set, STASH_CMD_NAME, strlen(name), name);
	}
	
	
	if (alist) {
		assert(ll_count(alist) > 0);
		ll_start(alist);
		while ((attr = ll_next(alist))) {
			
			assert(attr->keyid > 0);
			assert(attr->value);
			assert(BUF_LENGTH(stash->buf_attr) == 0);
			
			rispbuf_addInt(stash->buf_attr, STASH_CMD_KEY_ID, attr->keyid);
			
			stash_build_value(stash->buf_value, attr->value);
			assert(BUF_LENGTH(stash->buf_value) > 0);
			rispbuf_addBuffer(stash->buf_attr, STASH_CMD_VALUE, stash->buf_value);
			expbuf_clear(stash->buf_value);
			
			if (attr->expires > 0) {
				rispbuf_addInt(stash->buf_attr, STASH_CMD_EXPIRES, attr->expires);
			}
			
			rispbuf_addBuffer(stash->buf_set, STASH_CMD_ATTRIBUTE, stash->buf_attr);
			expbuf_clear(stash->buf_attr);
		}
		ll_finish(alist);
	}
	
	if (expires > 0) {
		rispbuf_addInt(stash->buf_set, STASH_CMD_EXPIRES, expires);
	}
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_SET, stash->buf_set);
	
	expbuf_clear(stash->buf_set);
	expbuf_clear(stash->buf_value);
	expbuf_clear(stash->buf_attr);
	
	assert(reply);
	return(reply);
}


//-----------------------------------------------------------------------------
// This is a pretty important function.  It needs to add a row into a table, 
// and set the initial attributes.
stash_reply_t * stash_set(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_attrlist_t *alist)
{
	stash_reply_t *reply;
	attr_t *attr;

	assert(stash && nsid > 0 && tid > 0 && rowid > 0);
	assert(alist && ll_count(alist));

	// get a buffer and bui
	assert(stash->buf_set);
	assert(BUF_LENGTH(stash->buf_set) == 0);

	assert(stash->buf_attr);
	assert(BUF_LENGTH(stash->buf_attr) == 0);

	assert(stash->buf_value);
	assert(BUF_LENGTH(stash->buf_value) == 0);

	rispbuf_addInt(stash->buf_set, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_TABLE_ID, tid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_ROW_ID, rowid);

	if (alist) {
		assert(ll_count(alist) > 0);
		ll_start(alist);
		while ((attr = ll_next(alist))) {
			
			assert(attr->keyid > 0);
			assert(attr->value);
			assert(BUF_LENGTH(stash->buf_attr) == 0);
			
			rispbuf_addInt(stash->buf_attr, STASH_CMD_KEY_ID, attr->keyid);
			
			stash_build_value(stash->buf_value, attr->value);
			assert(BUF_LENGTH(stash->buf_value) > 0);
			rispbuf_addBuffer(stash->buf_attr, STASH_CMD_VALUE, stash->buf_value);
			expbuf_clear(stash->buf_value);
			
			if (attr->expires > 0) {
				rispbuf_addInt(stash->buf_attr, STASH_CMD_EXPIRES, attr->expires);
			}
			
			rispbuf_addBuffer(stash->buf_set, STASH_CMD_ATTRIBUTE, stash->buf_attr);
			expbuf_clear(stash->buf_attr);
		}
		ll_finish(alist);
	}

	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_SET, stash->buf_set);
	
	expbuf_clear(stash->buf_set);
	expbuf_clear(stash->buf_value);
	expbuf_clear(stash->buf_attr);
	
	assert(reply);
	return(reply);
}



static void free_row(replyrow_t *row)
{
	attr_t *attr;
	
	assert(row);	
	assert(row->attrlist);
	
	while ((attr = ll_pop_head(row->attrlist))) {
		assert(attr->keyid > 0);
		assert(attr->value);
		
		stash_free_value(attr->value);
	}
	row->attrlist = ll_free(row->attrlist);
	assert(row->attrlist == NULL);
}

void stash_return_reply(stash_reply_t *reply)
{
	replyrow_t *row;
	
	assert(reply);
	assert(reply->stash);
	
	assert(reply->rows);
	while((row = ll_pop_head(reply->rows))) {
		free_row(row);
		free(row);
	}
	
	// put the reply back on the reply pool.
	reply_clear(reply);
	assert(reply->stash->replypool);
	ll_push_head(reply->stash->replypool, reply);
}


stash_keyid_t stash_get_key_id(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, const char *keyname)
{
	stash_keyid_t kid = 0;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash && nsid > 0 && tid > 0 && keyname);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	
	rispbuf_addInt(data, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addInt(data, STASH_CMD_TABLE_ID, tid);
	rispbuf_addStr(data, STASH_CMD_KEY, strlen(keyname), keyname);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_GETID, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	if (reply->resultcode == STASH_ERR_OK) {
		kid = reply->kid;
	}
	stash_return_reply(reply);
	
	return(kid);
}


stash_result_t stash_grant(stash_t *stash, stash_userid_t uid, stash_nsid_t nsid, stash_tableid_t tid, unsigned short option_map)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash);
	assert(option_map > 0);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	
	if (uid > 0)  rispbuf_addInt(data, STASH_CMD_USER_ID, uid);
	if (nsid > 0) rispbuf_addInt(data, STASH_CMD_NAMESPACE_ID, nsid);
	if (tid > 0)  rispbuf_addInt(data, STASH_CMD_TABLE_ID, tid);
	
	if (option_map & STASH_RIGHT_ADDUSER) rispbuf_addCmd(data, STASH_CMD_RIGHT_ADDUSER);
	if (option_map & STASH_RIGHT_CREATE)  rispbuf_addCmd(data, STASH_CMD_RIGHT_CREATE);
	if (option_map & STASH_RIGHT_DROP)    rispbuf_addCmd(data, STASH_CMD_RIGHT_DROP);
	if (option_map & STASH_RIGHT_SET)     rispbuf_addCmd(data, STASH_CMD_RIGHT_SET);
	if (option_map & STASH_RIGHT_UPDATE)  rispbuf_addCmd(data, STASH_CMD_RIGHT_UPDATE);
	if (option_map & STASH_RIGHT_DELETE)  rispbuf_addCmd(data, STASH_CMD_RIGHT_DELETE);
	if (option_map & STASH_RIGHT_QUERY)   rispbuf_addCmd(data, STASH_CMD_RIGHT_QUERY);
	if (option_map & STASH_RIGHT_LOCK)    rispbuf_addCmd(data, STASH_CMD_RIGHT_LOCK);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_GRANT, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	reply_free(reply);
	
	return(res);
}


stash_cond_t * __cond_key_equals(stash_keyid_t kid, stash_value_t *value)
{
	stash_cond_t *cond;
	
	assert(kid > 0 && value);
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_EQUALS;
	cond->kid = kid;
	cond->value = value;
	
	return(cond);
}

stash_cond_t * __cond_key_gt(stash_keyid_t kid, stash_value_t *value)
{
	stash_cond_t *cond;
	
	assert(kid > 0 && value);
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_GT;
	cond->kid = kid;
	cond->value = value;
	
	return(cond);
}



stash_cond_t * __cond_key_exists(stash_keyid_t kid)
{
	stash_cond_t *cond;
	
	assert(kid > 0);
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	cond->condtype = STASH_CONDTYPE_EXISTS;
	cond->kid = kid;
	assert(cond->value == NULL);
	
	return(cond);
}


stash_cond_t * __cond_name(stash_nameid_t nameid, const char *name)
{
	stash_cond_t *cond;
	
	assert((nameid == 0 && name) || (nameid > 0 && name==NULL));
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_NAME;
	if (nameid > 0) { 
		cond->nameid = nameid; 
		assert(cond->name == NULL); 
	}
	else { 
		cond->name = strdup(name);
		assert(cond->nameid == 0);
	}
	
	return(cond);
}

stash_cond_t * __cond_and(stash_cond_t *aa, stash_cond_t *bb)
{
	stash_cond_t *cond;
	assert(aa && bb);
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_AND;
	cond->ca = aa;
	cond->cb = bb;
	
	return(cond);
}

stash_cond_t * __cond_or(stash_cond_t *aa, stash_cond_t *bb)
{
	stash_cond_t *cond;
	assert(aa && bb);
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_OR;
	cond->ca = aa;
	cond->cb = bb;
	
	return(cond);
}

stash_cond_t * __cond_not(stash_cond_t *aa)
{
	stash_cond_t *cond;
	assert(aa);
	
	cond = calloc(1, sizeof(*cond));
	assert(cond);
	
	cond->condtype = STASH_CONDTYPE_NOT;
	cond->ca = aa;
	assert(cond->cb == NULL);
	
	return(cond);
}


// Free a compound condition.  Recursively free the condition structure.
void stash_cond_free(stash_cond_t *cond)
{
	assert(cond);
	
	switch(cond->condtype) {
		case STASH_CONDTYPE_EQUALS:
			assert(cond->kid > 0);
			assert(cond->value);
			stash_free_value(cond->value);
			break;
			
		case STASH_CONDTYPE_NAME:
			if (cond->name) free(cond->name);
			break;
			
		case STASH_CONDTYPE_AND:
		case STASH_CONDTYPE_OR:
			assert(cond->ca);
			assert(cond->cb);
			stash_cond_free(cond->ca);
			stash_cond_free(cond->cb);
			break;
			
		default:
			assert(0);
			break;
	}
	
	free(cond);
}



// this function is used to build the condition operations into a buffer.  If the conditions are nested, then it will call itself recursively to add them.
static void build_condition(expbuf_t *buffer, stash_cond_t *condition) 
{
	expbuf_t *buf;
	expbuf_t *buf_value;
	assert(buffer && condition);

	buf = expbuf_init(NULL, 64);
	
	if (condition->condtype == STASH_CONDTYPE_EQUALS) {
		
		assert(condition->kid > 0);
		rispbuf_addInt(buf, STASH_CMD_KEY_ID, condition->kid);
		
		assert(condition->value);
		buf_value = expbuf_init(NULL, 0);
		stash_build_value(buf_value, condition->value);
		rispbuf_addBuffer(buf, STASH_CMD_VALUE, buf_value);
		buf_value = expbuf_free(buf_value);
		assert(buf_value == NULL);
		
		rispbuf_addBuffer(buffer, STASH_CMD_COND_EQUALS, buf);
		
	}
	else if (condition->condtype == STASH_CONDTYPE_NAME) {
		
		if (condition->nameid > 0) {
			assert(condition->name == NULL);
			rispbuf_addInt(buf, STASH_CMD_NAME_ID, condition->nameid);
		}
		else {
			assert(condition->nameid == 0);
			assert(condition->name);
			rispbuf_addStr(buf, STASH_CMD_NAME, strlen(condition->name), condition->name);
		}

		rispbuf_addBuffer(buffer, STASH_CMD_COND_NAME, buf);
	}
	else if (condition->condtype == STASH_CONDTYPE_AND || condition->condtype == STASH_CONDTYPE_OR) {
		
		buf_value = expbuf_init(NULL, 0);
		
		// build the cond-a
		assert(condition->ca);
		build_condition(buf_value, condition->ca);
		rispbuf_addBuffer(buf, STASH_CMD_COND_A, buf_value);
		expbuf_clear(buf_value);
		
		// build the cond-b
		assert(condition->cb);
		build_condition(buf_value, condition->cb);
		rispbuf_addBuffer(buf, STASH_CMD_COND_B, buf_value);
		
		
		buf_value = expbuf_free(buf_value);
		assert(buf_value == NULL);
		
		if (condition->condtype == STASH_CONDTYPE_AND) {
			rispbuf_addBuffer(buffer, STASH_CMD_COND_AND, buf);
		}
		else {
			rispbuf_addBuffer(buffer, STASH_CMD_COND_OR, buf);
		}
	}
	else if (condition->condtype == STASH_CONDTYPE_NOT) {
		// build the cond-a
		assert(condition->ca);
		build_condition(buf, condition->ca);
		rispbuf_addBuffer(buffer, STASH_CMD_COND_NOT, buf);
	}
	else if (condition->condtype == STASH_CONDTYPE_EXISTS) {
		
		assert(condition->kid > 0);
		assert(condition->value == NULL);
		rispbuf_addInt(buf, STASH_CMD_KEY_ID, condition->kid);
		rispbuf_addBuffer(buffer, STASH_CMD_COND_EXISTS, buf);
		
	}
	else {
		assert(0);
	}
	
	buf = expbuf_free(buf);
	assert(buf == NULL);
}


// create a query object that will be used to build a query and then execute it.  
// Object will need to be free'd with stash_query_free().
stash_query_t * stash_query_new(stash_nsid_t nsid, stash_tableid_t tid)
{
	stash_query_t *query;
	
	assert(nsid > 0 && tid > 0);
	query = calloc(1, sizeof(*query));
	assert(query);
	
	query->nsid = nsid;
	query->tid = tid;
	assert(query->limit == 0);
	assert(query->condition == NULL);
	
	return(query);
}

// free the query object that was created with stash_query_new().
// 
// NOTE: Freeing the query does not free or cleanup any conditions that were 
//       supplied.  Conditions must be freed manually.
void stash_query_free(stash_query_t *query)
{
	assert(query);
	
	// eventually there might be things inside the object that need to be freed as well, but not yet.
	
	free(query);
}

// set the condition for the query.  If an existing condition is there, then 
// it is merely replaced.  No cleanup of the condition is attempted.
void stash_query_condition(stash_query_t *query, stash_cond_t *condition)
{
	assert(query && condition);
	query->condition = condition;
}

void stash_query_limit(stash_query_t *query, int limit)
{
	assert(query && limit >= 0);
	query->limit = limit;
}

stash_reply_t * stash_query_execute(stash_t *stash, stash_query_t *query)
{
	stash_reply_t *reply;
	expbuf_t *buf_cond = NULL;
	expbuf_t *buf_query = NULL;
	
	assert(stash && query);
	assert(query->nsid > 0 && query->tid > 0 && query->limit >= 0);
	
	buf_query = expbuf_init(NULL, 0);
	
	// build the rest of the message.
	rispbuf_addInt(buf_query, STASH_CMD_NAMESPACE_ID, query->nsid);
	rispbuf_addInt(buf_query, STASH_CMD_TABLE_ID, query->tid);
	
	if (query->condition) {
		buf_cond = expbuf_init(NULL, 0);
		
		// build the condition string.
		build_condition(buf_cond, query->condition);
		assert(BUF_LENGTH(buf_cond) > 0);
		
		// add the condition buffer to the main one.
		rispbuf_addBuffer(buf_query, STASH_CMD_CONDITION, buf_cond);
		
		buf_cond = expbuf_free(buf_cond);
		assert(buf_cond == NULL);
	}
	
	// send it.
	reply = send_request(stash, STASH_CMD_QUERY, buf_query);
	
	buf_query = expbuf_free(buf_query);
	assert(buf_query == NULL);
	
	// return the reply;
	return(reply);
}


// This function is retained for compatibility reasons.  It builds a query 
// based on the limited parameters, and executes it.  It returns the reply.
stash_reply_t * stash_query(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, int limit, stash_cond_t *condition)
{
	stash_reply_t *reply;
	stash_query_t *query;
	
	assert(stash && nsid > 0 && tid > 0 && limit >= 0);
	query = stash_query_new(nsid, tid);
	assert(query);
	
	stash_query_limit(query, limit);
	if (condition) {
		stash_query_condition(query, condition);
	}
	
	reply = stash_query_execute(stash, query);
	assert(reply);
	
	stash_query_free(query);
	
	return(reply);
}


stash_result_t stash_get_user_id(stash_t *stash, const char *username, stash_userid_t *uid)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash && username && uid);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 0);
	assert(data);
	
	rispbuf_addStr(data, STASH_CMD_USERNAME, strlen(username), username);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_GETID, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was provided.
	res = reply->resultcode;
	if (res == STASH_ERR_OK) {
		*uid = reply->uid;
	}
	reply_free(reply);
	
	return(res);
}


stash_result_t stash_get_table_id(stash_t *stash, stash_nsid_t nsid, const char *tablename, stash_userid_t *tid)
{
	stash_result_t res;
	stash_reply_t *reply;
	expbuf_t *data;
	
	assert(stash && nsid > 0 && tablename && tid);
	
	// get a buffer and bui
	data = expbuf_init(NULL, 128);
	assert(data);
	
	rispbuf_addInt(data, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addStr(data, STASH_CMD_TABLE, strlen(tablename), tablename);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_GETID, data);
	
	data = expbuf_free(data);
	assert(data == NULL);
	
	// process the reply and store the results in the data pointers that was 
	// provided.
	res = reply->resultcode;
	if (res == STASH_ERR_OK) {
		*tid = reply->tid;
	}
	reply_free(reply);
	
	return(res);
}


//-----------------------------------------------------------------------------
// given a fully fleshed out reply object.  Go to the next row in the list.  
// If there are no rows, then return 0.  If there is an available row, then 
// return the rowid (non-zero).
int stash_nextrow(stash_reply_t *reply)
{
	replyrow_t *row;
	int rowid = 0;
	
	assert(reply);
	assert(reply->curr_row <= reply->row_count);

//  	printf("stash_nextrow(): curr_row=%d, row_count=%d\n", reply->curr_row, reply->row_count);
	
	if (reply->curr_row == reply->row_count) {
		// we've reached the end of the list.  We increment the curr_row so that other functions can determine that the end of the list was reached and that further values cant be obtained unless the reply object is rewound.
		reply->curr_row ++;
		assert(reply->curr_row > reply->row_count);
		assert(rowid == 0);
	}
	else if (reply->curr_row < 0) {
		// this is the first one, 
		if (reply->row_count > 0) {
			assert(reply->rows);
			row = ll_get_head(reply->rows);
			assert(row);
			assert(row->nid > 0);
			assert(row->rid > 0);
			rowid = row->rid;
			row->done = 1;
			
			reply->curr_row = 1;
		}
		else {
			assert(rowid == 0);
		}
	}
	else {
		assert(reply->curr_row > 0);
		assert(reply->row_count > 0);
		
		// this is not the first one, so we need to move the entry at the top of the list to the bottom.
		// get the entry from the top.
		
		assert(reply->rows);
		row = ll_pop_head(reply->rows);
		assert(row);
		assert(row->done == 1);
		ll_push_tail(reply->rows, row);
		
		// now look at the row at the top of the list and mark it so that we know that we've seen this row.
		row = ll_get_head(reply->rows);
		assert(row);
		assert(row->done == 0);
		row->done = 1;
		assert(row->nid > 0);
		assert(row->rid > 0);
		rowid = row->rid;
		
		reply->curr_row ++;
		assert(reply->curr_row > 0 && reply->curr_row <= reply->row_count);
	}
	
	return(rowid);
}

// if the attribute contains a string value, then return it.  Otherwise return NULL.
const char * stash_getstr(stash_reply_t *reply, stash_keyid_t key)
{
	const char *value = NULL;
	replyrow_t *row;
	attr_t *attr;

	assert(reply && key > 0);
	assert(reply->row_count > 0);
	assert(reply->curr_row > 0 && reply->curr_row <= reply->row_count);
	
	assert(reply->rows);
	row = ll_get_head(reply->rows);
	assert(row->rid > 0);
	assert(row->done == 1);
	assert(row->attrlist);
	
	ll_start(row->attrlist);
	attr = ll_next(row->attrlist);
	while (attr) {
		assert(value == NULL);
		assert(attr->keyid > 0);
		if (attr->keyid == key) {
			assert(attr->value);
			if (attr->value->valtype == STASH_VALTYPE_STR) {
				value = attr->value->value.str;
			}
			
			// we've found what we are looking for, no need to keep looking.
			attr = NULL;
		}
		else {
			assert(value == NULL);
			attr = ll_next(row->attrlist);
		}
	}
	ll_finish(row->attrlist);
	
	return(value);
}

int stash_getlength(stash_reply_t *reply, stash_keyid_t key)
{
	int length = 0;
	replyrow_t *row;
	attr_t *attr;
	
	assert(reply && key > 0);
	assert(reply->row_count > 0);
	assert(reply->curr_row > 0 && reply->curr_row <= reply->row_count);
	
	assert(reply->rows);
	row = ll_get_head(reply->rows);
	assert(row->rid > 0);
	assert(row->done == 1);
	assert(row->attrlist);
	
	ll_start(row->attrlist);
	attr = ll_next(row->attrlist);
	while (attr) {
		assert(length == 0);
		assert(attr->keyid > 0);
		if (attr->keyid == key) {
			assert(attr->value);
			if (attr->value->valtype == STASH_VALTYPE_STR) {
				length = attr->value->datalen;
			}
			
			// we've found what we are looking for, no need to keep looking.
			attr = NULL;
		}
		else {
			assert(length == 0);
			attr = ll_next(row->attrlist);
		}
	}
	ll_finish(row->attrlist);
	
	return(length);
}


int stash_getint(stash_reply_t *reply, stash_keyid_t key)
{
	int value = 0;
	replyrow_t *row;
	attr_t *attr;
	
	assert(reply && key > 0);
	assert(reply->row_count > 0);
	assert(reply->curr_row > 0 && reply->curr_row <= reply->row_count);
	
	assert(reply->rows);
	row = ll_get_head(reply->rows);
	assert(row->rid > 0);
	assert(row->done == 1);
	assert(row->attrlist);
	
	ll_start(row->attrlist);
	attr = ll_next(row->attrlist);
	while (attr) {
		assert(value == 0);
		assert(attr->keyid > 0);
		if (attr->keyid == key) {
			assert(attr->value);
			if (attr->value->valtype == STASH_VALTYPE_INT) {
				value = attr->value->value.number;
			}
			
			// we've found what we are looking for, no need to keep looking.
			attr = NULL;
		}
		else {
			assert(value == 0);
			attr = ll_next(row->attrlist);
		}
	}
	ll_finish(row->attrlist);
	
	return(value);
}


// returns the rowid for the current row in the reply.
stash_rowid_t stash_rowid(stash_reply_t *reply)
{
	replyrow_t *row;
	
	assert(reply);
	assert(reply->row_count > 0);
	assert(reply->curr_row > 0 && reply->curr_row <= reply->row_count);
	
	assert(reply->rows);
	row = ll_get_head(reply->rows);
	assert(row->rid > 0);
	
	return(row->rid);
}




stash_reply_t * stash_expire(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_keyid_t keyid, stash_expiry_t expires)
{
	stash_reply_t *reply;
	
	assert(stash && nsid > 0 && tid > 0 && rowid);
	assert(keyid >= 0);
	assert(expires >= 0);
	
	// get a buffer and bui
	assert(stash->buf_set);
	assert(BUF_LENGTH(stash->buf_set) == 0);
	
	rispbuf_addInt(stash->buf_set, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_TABLE_ID, tid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_ROW_ID, rowid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_KEY_ID, keyid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_EXPIRES, expires);
	
	// send the request and receive the reply.
	reply = send_request(stash, STASH_CMD_SET_EXPIRY, stash->buf_set);
	expbuf_clear(stash->buf_set);
	assert(reply);
	return(reply);
}


stash_reply_t * stash_delete(stash_t *stash, stash_nsid_t nsid, stash_tableid_t tid, stash_rowid_t rowid, stash_keyid_t keyid)
{
	stash_reply_t *reply;
	
	assert(stash && nsid > 0 && tid > 0 && rowid);
	assert(keyid >= 0);
	
	// get a buffer and bui
	assert(stash->buf_set);
	assert(BUF_LENGTH(stash->buf_set) == 0);
	
	rispbuf_addInt(stash->buf_set, STASH_CMD_NAMESPACE_ID, nsid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_TABLE_ID, tid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_ROW_ID, rowid);
	rispbuf_addInt(stash->buf_set, STASH_CMD_KEY_ID, keyid);
	
	// send the request and receive the reply.
	printf("sending DELETE cmd\n");
	reply = send_request(stash, STASH_CMD_DELETE, stash->buf_set);
	expbuf_clear(stash->buf_set);
	assert(reply);
	return(reply);
}

static stash_value_t * getvalue(const replyrow_t *row, stash_keyid_t key)
{
	stash_value_t *val = NULL;
	attr_t *tmp;
	
	
	assert(row && key > 0);
	assert(row->identifier == 0x1234);
	assert(row->attrlist);
	assert(row->attrlist->loop == NULL);
	ll_start(row->attrlist);
	while (val == NULL && (tmp = ll_next(row->attrlist))) {
		if (tmp->keyid == key) {
			assert(tmp->value);
			val = tmp->value;
		}
	}
	ll_finish(row->attrlist);
	
	return(val);
}

// a and b point to elements in the list... and is not the pointer that the element has in it.  Therefore, it needs to be dereferenced a bit.
static int sortfn(const void *a, const void *b) 
{
	stash_value_t *va, *vb;
// 	replyrow_t *ra, *rb;
	
	replyrow_t * const *ra = a;
	replyrow_t * const *rb = b;
	
	assert(a && b);
	assert(stash_sortkey > 0);
	
// 	printf("a=%lu, b=%lu\n", (*ra), (*rb));

	
	assert((*ra)->identifier == 0x1234);
	assert((*rb)->identifier == 0x1234);
	
	va = getvalue((*ra), stash_sortkey);
	vb = getvalue((*rb), stash_sortkey);
	if (va == NULL && vb == NULL) { return(0); }
	else if (va && vb == NULL) { return(1); }
	else if (va == NULL && vb) { return(-1); }
	else {
		assert(va && vb);
		
		if (va->valtype == STASH_VALTYPE_INT && vb->valtype == STASH_VALTYPE_INT) {
			return(va->value.number - vb->value.number);
		}
		else if (va->valtype == STASH_VALTYPE_STR && vb->valtype == STASH_VALTYPE_STR) {
			return(strncmp(va->value.str, vb->value.str, vb->datalen < va->datalen ? vb->datalen : va->datalen));
		}
		else {
			// need to compare other type combinations.
			assert(0);
		}
	}

	return(0);
}

// reset the reply so that it can be iterated from the start again.  Normally used after resorting
void stash_reply_reset(stash_reply_t *reply) 
{
	assert(reply);
	
	reply->curr_row = 0;
	
	// incomplete.  Need to go trhough the list of rows, and reset the 'done' flag.
	assert(0);
}

// this function will take the reply array, and sort it based on the keyID supplied.  If rows do not contain this key, then they are moved to the bottom.
void stash_sort(stash_reply_t *reply, stash_keyid_t key)
{
	int total, i;
	replyrow_t **list;
	
	assert(reply && key > 0);

	if (reply->rows) {
		total = ll_count(reply->rows);
		assert(total > 0);
		
		// create an array (of pointers) big enough to hold all the rows.
		list = calloc(total, sizeof(replyrow_t *));
		assert(list);
		
		// pull out the rows from the list, into the array.
		for (i=0; i<total; i++) {
			assert(list[i] == NULL);
			list[i] = ll_pop_head(reply->rows);
			assert(list[i]);
			assert(list[i]->identifier == 0x1234);
		}

		// the original list should now be empty.
		assert(ll_count(reply->rows) == 0);

		// sort the rows
		assert(stash_sortkey == 0);
		stash_sortkey = key;
		
		qsort(list, total, sizeof(replyrow_t *), sortfn);
		assert(stash_sortkey == key);
		stash_sortkey = 0;
		assert(stash_sortkey == 0);

		// put the rows back into the list.
		for (i=0; i<total; i++) {
			assert(list[i]);
			
			// reset the 'done' marker because it has been sorted, and we start iterating through the list again.
			list[i]->done = 0;
			
			ll_push_tail(reply->rows, list[i]);
		}
		
		// free the array.
		free(list);

		// reset the 'current row' to indicate that it should start at the begining.
		reply->curr_row = -1;
	}
	else {
		assert(reply->curr_row == 0);
		
	}
}




