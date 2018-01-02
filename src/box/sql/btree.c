/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * This file implements an external (disk-based) database using BTrees.
 * See the header comment on "btreeInt.h" for additional information.
 * Including a description of file format and an overview of operation.
 */
#include "btreeInt.h"
#include "tarantoolInt.h"


/*
 * Clear the current cursor position.
 */
void
sqlite3BtreeClearCursor(BtCursor * pCur)
{
	sqlite3_free(pCur->pKey);
	pCur->pKey = 0;
	pCur->eState = CURSOR_INVALID;
}

/*
 * Determine whether or not a cursor has moved from the position where
 * it was last placed, or has been invalidated for any other reason.
 * Cursors can move when the row they are pointing at is deleted out
 * from under them, for example.  Cursor might also move if a btree
 * is rebalanced.
 *
 * Calling this routine with a NULL cursor pointer returns false.
 *
 * Use the separate sqlite3BtreeCursorRestore() routine to restore a cursor
 * back to where it ought to be if this routine returns true.
 */
int
sqlite3BtreeCursorHasMoved(BtCursor * pCur)
{
	return pCur->eState != CURSOR_VALID;
}

/*
 * Provide flag hints to the cursor.
 */
void
sqlite3BtreeCursorHintFlags(BtCursor * pCur, unsigned x)
{
	assert(x == BTREE_SEEK_EQ || x == BTREE_BULKLOAD || x == 0);
	pCur->hints = x;
}

/*
 * Open a database file.
 *
 * zFilename is the name of the database file.  If zFilename is NULL
 * then an ephemeral database is created.  The ephemeral database might
 * be exclusively in memory, or it might use a disk-based memory cache.
 * Either way, the ephemeral database will be automatically deleted
 * when sqlite3BtreeClose() is called.
 *
 * If zFilename is ":memory:" then an in-memory database is created
 * that is automatically destroyed when it is closed.
 *
 * The "flags" parameter is a bitmask that might contain bits like
 * BTREE_OMIT_JOURNAL and/or BTREE_MEMORY.
 *
 * If the database is already opened in the same database connection
 * and we are in shared cache mode, then the open will fail with an
 * SQLITE_CONSTRAINT error.  We cannot allow two or more BtShared
 * objects in the same database connection since doing so will lead
 * to problems with locking.
 */
int
sqlite3BtreeOpen(sqlite3_vfs * pVfs,	/* VFS to use for this b-tree */
		 const char *zFilename,	/* Name of the file containing the BTree database */
		 sqlite3 * db,	/* Associated database handle */
		 Btree ** ppBtree,	/* Pointer to new Btree object written here */
		 int flags,	/* Options */
		 int vfsFlags	/* Flags passed through to sqlite3_vfs.xOpen() */
    ) {
	Btree *p;                /* Handle to return */
	(void)vfsFlags;
	(void)zFilename;

	assert(db != 0);
	assert(pVfs != 0);
	assert((flags & 0xff) == flags);        /* flags fit in 8 bits */

	/* Only a BTREE_SINGLE database can be BTREE_UNORDERED */
	assert((flags & BTREE_UNORDERED) == 0 || (flags & BTREE_SINGLE) != 0);

	p = sqlite3MallocZero(sizeof(Btree));
	if (!p) {
		return SQLITE_NOMEM_BKPT;
	}
	p->inTrans = TRANS_NONE;
	p->db = db;

	*ppBtree = p;

	return SQLITE_OK;
}

/*
 * Attempt to start a new transaction. A write-transaction
 * is started if the second argument is nonzero, otherwise a read-
 * transaction.  If the second argument is 2 or more and exclusive
 * transaction is started, meaning that no other process is allowed
 * to access the database.  A preexisting transaction may not be
 * upgraded to exclusive by calling this routine a second time - the
 * exclusivity flag only works for a new transaction.
 *
 * A write-transaction must be started before attempting any
 * changes to the database.  None of the following routines
 * will work unless a transaction is started first:
 *
 *      sqlite3BtreeCreateTable()
 *      sqlite3BtreeCreateIndex()
 *      sqlite3BtreeClearTable()
 *      sqlite3BtreeDropTable()
 *      sqlite3BtreeInsert()
 *      sqlite3BtreeDelete()
 *      sqlite3BtreeUpdateMeta()
 *
 * If an initial attempt to acquire the lock fails because of lock contention
 * and the database was previously unlocked, then invoke the busy handler
 * if there is one.  But if there was previously a read-lock, do not
 * invoke the busy handler - just return SQLITE_BUSY.  SQLITE_BUSY is
 * returned when there is already a read-lock in order to avoid a deadlock.
 *
 * Suppose there are two processes A and B.  A has a read lock and B has
 * a reserved lock.  B tries to promote to exclusive but is blocked because
 * of A's read lock.  A tries to promote to reserved but is blocked by B.
 * One or the other of the two processes must give way or there can be
 * no progress.  By returning SQLITE_BUSY and not invoking the busy callback
 * when A already has a read lock, we encourage A to give up and let B
 * proceed.
 */
int
sqlite3BtreeBeginTrans(Btree * p, int nSavepoint, int wrflag)
{
	(void)nSavepoint;

	p->inTrans = (wrflag ? TRANS_WRITE : TRANS_READ);

	return SQLITE_OK;
}

/*
 * This function is called from both BtreeCommitPhaseTwo() and BtreeRollback()
 * at the conclusion of a transaction.
 */
static void
btreeEndTransaction(Btree * p)
{
	sqlite3 *db = p->db;

	if (p->inTrans > TRANS_NONE && db->nVdbeRead > 1) {
		p->inTrans = TRANS_READ;
	} else {
		p->inTrans = TRANS_NONE;
	}
}

/*
 * Rollback the transaction in progress.
 *
 * If tripCode is not SQLITE_OK then cursors will be invalidated (tripped).
 * Only write cursors are tripped if writeOnly is true but all cursors are
 * tripped if writeOnly is false.  Any attempt to use
 * a tripped cursor will result in an error.
 *
 * This will release the write lock on the database file.  If there
 * are no active cursors, it also releases the read lock.
 */
int
sqlite3BtreeRollback(Btree * p, int tripCode, int writeOnly)
{
	assert(writeOnly == 1 || writeOnly == 0);
	assert(tripCode == SQLITE_ABORT_ROLLBACK || tripCode == SQLITE_OK);

	btreeEndTransaction(p);
	return SQLITE_OK;
}

/*
 * The second argument to this function, op, is always SAVEPOINT_ROLLBACK
 * or SAVEPOINT_RELEASE. This function either releases or rolls back the
 * savepoint identified by parameter iSavepoint, depending on the value
 * of op.
 *
 * Normally, iSavepoint is greater than or equal to zero. However, if op is
 * SAVEPOINT_ROLLBACK, then iSavepoint may also be -1. In this case the
 * contents of the entire transaction are rolled back. This is different
 * from a normal transaction rollback, as no locks are released and the
 * transaction remains open.
 */
int
sqlite3BtreeSavepoint(Btree * p, int op, int iSavepoint)
{
	(void)p;
	(void)op;
	(void)iSavepoint;

	return SQLITE_OK;
}

/*
 * Create a new cursor for the BTree whose root is on the page
 * iTable. If a read-only cursor is requested, it is assumed that
 * the caller already has at least a read-only transaction open
 * on the database already. If a write-cursor is requested, then
 * the caller is assumed to have an open write transaction.
 *
 * If the BTREE_WRCSR bit of wrFlag is clear, then the cursor can only
 * be used for reading.  If the BTREE_WRCSR bit is set, then the cursor
 * can be used for reading or for writing if other conditions for writing
 * are also met.  These are the conditions that must be met in order
 * for writing to be allowed:
 *
 * 1:  The cursor must have been opened with wrFlag containing BTREE_WRCSR
 *
 * 2:  Other database connections that share the same pager cache
 *     but which are not in the READ_UNCOMMITTED state may not have
 *     cursors open with wrFlag==0 on the same table.  Otherwise
 *     the changes made by this write cursor would be visible to
 *     the read cursors in the other database connection.
 *
 * 3:  The database must be writable (not on read-only media)
 *
 * 4:  There must be an active transaction.
 *
 * The BTREE_FORDELETE bit of wrFlag may optionally be set if BTREE_WRCSR
 * is set.  If FORDELETE is set, that is a hint to the implementation that
 * this cursor will only be used to seek to and delete entries of an index
 * as part of a larger DELETE statement.  The FORDELETE hint is not used by
 * this implementation.  But in a hypothetical alternative storage engine
 * in which index entries are automatically deleted when corresponding table
 * rows are deleted, the FORDELETE flag is a hint that all SEEK and DELETE
 * operations on this cursor can be no-ops and all READ operations can
 * return a null row (2-bytes: 0x01 0x00).
 *
 * No checking is done to make sure that page iTable really is the
 * root page of a b-tree.  If it is not, then the cursor acquired
 * will not work correctly.
 *
 * It is assumed that the sqlite3BtreeCursorZero() has been called
 * on pCur to initialize the memory space prior to invoking this routine.
 */
static int
btreeCursor(Btree * p,		/* The btree */
	    int iTable,		/* Root page of table to open */
	    int wrFlag,		/* 1 to write. 0 read-only */
	    struct KeyInfo *pKeyInfo,	/* First arg to comparison function */
	    BtCursor * pCur	/* Space for new cursor */
    )
{
	assert(wrFlag == 0
	       || wrFlag == BTREE_WRCSR
	       || wrFlag == (BTREE_WRCSR | BTREE_FORDELETE)
	    );

	assert(p->inTrans > TRANS_NONE);

	/* Now that no other errors can occur, finish filling in the BtCursor
	 * variables and link the cursor into the BtShared list.
	 */
	pCur->pgnoRoot = (Pgno) iTable;
	pCur->pKeyInfo = pKeyInfo;
	pCur->pBtree = p;
	pCur->eState = CURSOR_INVALID;
	return SQLITE_OK;
}

int
sqlite3BtreeCursor(Btree * p,	/* The btree */
		   int iTable,	/* Root page of table to open */
		   int wrFlag,	/* 1 to write. 0 read-only */
		   struct KeyInfo *pKeyInfo,	/* First arg to xCompare() */
		   BtCursor * pCur	/* Write new cursor here */
    )
{
	int rc;
	if (iTable < 1) {
		rc = SQLITE_CORRUPT_BKPT;
	} else {
		rc = btreeCursor(p, iTable, wrFlag, pKeyInfo, pCur);
		if (p->db->mdb.pBt == p) {
			/* Main database backed by Tarantool.
			 * Ephemeral tables are not, in which case if-stmt is false
			 */
			pCur->curFlags |= BTCF_TaCursor;
			pCur->pTaCursor = 0;	/* sqlite3BtreeCursorZero didn't touch it */
		}
	}
	return rc;
}

int
sqlite3BtreeCursorEphemeral(Btree* p, int iTable, int wrFlag,
			    struct KeyInfo *pKeyInfo, BtCursor * pCur)
{
	btreeCursor(p, iTable, wrFlag, pKeyInfo, pCur);
	pCur->curFlags |= BTCF_TEphemCursor;
	pCur->pTaCursor = 0;

	return SQLITE_OK;
}

/*
 * Return the size of a BtCursor object in bytes.
 *
 * This interfaces is needed so that users of cursors can preallocate
 * sufficient storage to hold a cursor.  The BtCursor object is opaque
 * to users so they cannot do the sizeof() themselves - they must call
 * this routine.
 */
int
sqlite3BtreeCursorSize(void)
{
	return ROUND8(sizeof(BtCursor));
}

/*
 * Initialize memory that will be converted into a BtCursor object.
 *
 * The simple approach here would be to memset() the entire object
 * to zero.  But it turns out that the apPage[] and aiIdx[] arrays
 * do not need to be zeroed and they are large, so we can save a lot
 * of run-time by skipping the initialization of those elements.
 */
void
sqlite3BtreeCursorZero(BtCursor * p)
{
	memset(p, 0, offsetof(BtCursor, hints));
}

/*
 * Close a cursor.  The read lock on the database file is released
 * when the last cursor is closed.
 */
int
sqlite3BtreeCloseCursor(BtCursor * pCur)
{
	Btree *pBtree = pCur->pBtree;
	if (pBtree) {
		sqlite3BtreeClearCursor(pCur);
	}

		if (pCur->curFlags & BTCF_TaCursor) {
			tarantoolSqlite3CloseCursor(pCur);
		} else if (pCur->curFlags & BTCF_TEphemCursor) {
			tarantoolSqlite3EphemeralDrop(pCur);
			tarantoolSqlite3CloseCursor(pCur);
		}

	return SQLITE_OK;
}

#ifndef NDEBUG			/* The next routine used only within assert() statements */
/*
 * Return true if the given BtCursor is valid.  A valid cursor is one
 * that is currently pointing to a row in a (non-empty) table.
 * This is a verification routine is used only within assert() statements.
 */
int
sqlite3BtreeCursorIsValid(BtCursor * pCur)
{
	return pCur && pCur->eState == CURSOR_VALID;
}
#endif				/* NDEBUG */
int
sqlite3BtreeCursorIsValidNN(BtCursor * pCur)
{
	assert(pCur != 0);
	return pCur->eState == CURSOR_VALID;
}

/*
 * Return the number of bytes of payload for the entry that pCur is
 * currently pointing to.  For table btrees, this will be the amount
 * of data.  For index btrees, this will be the size of the key.
 *
 * The caller must guarantee that the cursor is pointing to a non-NULL
 * valid entry.  In other words, the calling procedure must guarantee
 * that the cursor has Cursor.eState==CURSOR_VALID.
 */
u32
sqlite3BtreePayloadSize(BtCursor * pCur)
{
	assert(pCur->eState == CURSOR_VALID);
	if (pCur->curFlags & BTCF_TaCursor ||
	    pCur->curFlags & BTCF_TEphemCursor) {
		u32 sz;
		tarantoolSqlite3PayloadFetch(pCur, &sz);
		return sz;
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}


/*
 * This function is used to read or overwrite payload information
 * for the entry that the pCur cursor is pointing to. The eOp
 * argument is interpreted as follows:
 *
 *   0: The operation is a read. Populate the overflow cache.
 *   1: The operation is a write. Populate the overflow cache.
 *   2: The operation is a read. Do not populate the overflow cache.
 *
 * A total of "amt" bytes are read or written beginning at "offset".
 * Data is read to or from the buffer pBuf.
 *
 * The content being read or written might appear on the main page
 * or be scattered out on multiple overflow pages.
 *
 * If the current cursor entry uses one or more overflow pages and the
 * eOp argument is not 2, this function may allocate space for and lazily
 * populates the overflow page-list cache array (BtCursor.aOverflow).
 * Subsequent calls use this cache to make seeking to the supplied offset
 * more efficient.
 *
 * Once an overflow page-list cache has been allocated, it may be
 * invalidated if some other cursor writes to the same table, or if
 * the cursor is moved to a different row.
 *
 * Creating a table (may require moving an overflow page).
 */
static int
accessPayload(BtCursor * pCur,	/* Cursor pointing to entry to read from */
	      u32 offset,	/* Begin reading this far into payload */
	      u32 amt,		/* Read this many bytes */
	      unsigned char *pBuf,	/* Write the bytes into this buffer */
	      int eOp		/* zero to read. non-zero to write. */
    )
{
	(void)eOp;
	if (pCur->curFlags & BTCF_TaCursor ||
	    pCur->curFlags & BTCF_TEphemCursor) {
		const void *pPayload;
		u32 sz;
		pPayload = tarantoolSqlite3PayloadFetch(pCur, &sz);
		if ((uptr) (offset + amt) > sz)
			return SQLITE_CORRUPT_BKPT;
		memcpy(pBuf, pPayload + offset, amt);
		return SQLITE_OK;
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

/*
 * Read part of the payload for the row at which that cursor pCur is currently
 * pointing.  "amt" bytes will be transferred into pBuf[].  The transfer
 * begins at "offset".
 *
 * pCur can be pointing to either a table or an index b-tree.
 * If pointing to a table btree, then the content section is read.  If
 * pCur is pointing to an index b-tree then the key section is read.
 *
 * For sqlite3BtreePayload(), the caller must ensure that pCur is pointing
 * to a valid row in the table.  For sqlite3BtreePayloadChecked(), the
 * cursor might be invalid or might need to be restored before being read.
 *
 * Return SQLITE_OK on success or an error code if anything goes
 * wrong.  An error is returned if "offset+amt" is larger than
 * the available payload.
 */
int
sqlite3BtreePayload(BtCursor * pCur, u32 offset, u32 amt, void *pBuf)
{
	assert(pCur->eState == CURSOR_VALID);
	assert((pCur->curFlags & BTCF_TaCursor) ||
	       (pCur->curFlags & BTCF_TEphemCursor));
	return accessPayload(pCur, offset, amt, (unsigned char *)pBuf, 0);
}


/*
 * For the entry that cursor pCur is point to, return as
 * many bytes of the key or data as are available on the local
 * b-tree page.  Write the number of available bytes into *pAmt.
 *
 * The pointer returned is ephemeral.  The key/data may move
 * or be destroyed on the next call to any Btree routine,
 * including calls from other threads against the same cache.
 * Hence, a mutex on the BtShared should be held prior to calling
 * this routine.
 *
 * These routines is used to get quick access to key and data
 * in the common case where no overflow pages are used.
 */
const void *
sqlite3BtreePayloadFetch(BtCursor * pCur, u32 * pAmt)
{
	if (pCur->curFlags & BTCF_TaCursor ||
	    pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3PayloadFetch(pCur, pAmt);
	}
	unreachable();
	return 0;
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
 * on success.  Set *pRes to 0 if the cursor actually points to something
 * or set *pRes to 1 if the table is empty.
 */
int
sqlite3BtreeFirst(BtCursor * pCur, int *pRes)
{

	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3First(pCur, pRes);
	}
	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralFirst(pCur, pRes);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
 * on success.  Set *pRes to 0 if the cursor actually points to something
 * or set *pRes to 1 if the table is empty.
 */
int
sqlite3BtreeLast(BtCursor * pCur, int *pRes)
{
	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3Last(pCur, pRes);
	}

	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralLast(pCur, pRes);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

/* Move the cursor so that it points to an entry near the key
 * specified by pIdxKey or intKey.   Return a success code.
 *
 * For INTKEY tables, the intKey parameter is used.  pIdxKey
 * must be NULL.  For index tables, pIdxKey is used and intKey
 * is ignored.
 *
 * If an exact match is not found, then the cursor is always
 * left pointing at a leaf page which would hold the entry if it
 * were present.  The cursor might point to an entry that comes
 * before or after the key.
 *
 * An integer is written into *pRes which is the result of
 * comparing the key with the entry to which the cursor is
 * pointing.  The meaning of the integer written into
 * *pRes is as follows:
 *
 *     *pRes<0      The cursor is left pointing at an entry that
 *                  is smaller than intKey/pIdxKey or if the table is empty
 *                  and the cursor is therefore left point to nothing.
 *
 *     *pRes==0     The cursor is left pointing at an entry that
 *                  exactly matches intKey/pIdxKey.
 *
 *     *pRes>0      The cursor is left pointing at an entry that
 *                  is larger than intKey/pIdxKey.
 *
 * For index tables, the pIdxKey->eqSeen field is set to 1 if there
 * exists an entry in the table that exactly matches pIdxKey.
 */
int
sqlite3BtreeMovetoUnpacked(BtCursor * pCur,	/* The cursor to be moved */
			   UnpackedRecord * pIdxKey,	/* Unpacked index key */
			   i64 intKey,	/* The table key */
			   int biasRight,	/* If true, bias the search to the high end */
			   int *pRes	/* Write search results here */
    )
{
	(void)biasRight;
	(void)intKey;

	assert(pRes);
	assert((pIdxKey == 0) == (pCur->pKeyInfo == 0));
	assert(pCur->eState != CURSOR_VALID
	       || (pIdxKey == 0) == (pCur->curIntKey != 0));

	if (pCur->curFlags & BTCF_TaCursor) {
		assert(pIdxKey);
		/*
		 * Note: pIdxKey/intKey are mutually-exclusive and all Tarantool
		 * tables are WITHOUT ROWID, hence no intKey parameter.
		 * BiasRight is a hint used during binary search; ignore it for now.
		 */
		return tarantoolSqlite3MovetoUnpacked(pCur, pIdxKey, pRes);
	}

	if (pCur->curFlags & BTCF_TEphemCursor) {
		assert(pIdxKey);
		return tarantoolSqlite3MovetoUnpackedEphemeral(pCur, pIdxKey, pRes);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

/*
 * Return TRUE if the cursor is not pointing at an entry of the table.
 *
 * TRUE will be returned after a call to sqlite3BtreeNext() moves
 * past the last entry in the table or sqlite3BtreePrev() moves past
 * the first entry.  TRUE is also returned if the table is empty.
 */
int
sqlite3BtreeEof(BtCursor * pCur)
{
	/* TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table entries
	 * have been deleted? This API will need to change to return an error code
	 * as well as the boolean result value.
	 */
	return (CURSOR_VALID != pCur->eState);
}

int
sqlite3BtreeNext(BtCursor * pCur, int *pRes)
{
	assert(pRes != 0);
	assert(*pRes == 0 || *pRes == 1);
	assert(pCur->skipNext == 0 || pCur->eState != CURSOR_VALID);
	*pRes = 0;
	if (pCur->curFlags & BTCF_TaCursor ||
	    pCur->curFlags & BTCF_TEphemCursor) {
		if (pCur->curFlags & BTCF_TEphemCursor)
			return tarantoolSqlite3EphemeralNext(pCur, pRes);
		return tarantoolSqlite3Next(pCur, pRes);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

int
sqlite3BtreePrevious(BtCursor * pCur, int *pRes)
{
	assert(pRes != 0);
	assert(*pRes == 0 || *pRes == 1);
	assert(pCur->skipNext == 0 || pCur->eState != CURSOR_VALID);
	*pRes = 0;
	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3Previous(pCur, pRes);
	}
	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralPrevious(pCur, pRes);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}


int
sqlite3BtreeInsert(BtCursor * pCur,	/* Insert data into the table of this cursor */
		   const BtreePayload * pX,	/* Content of the row to be inserted */
		   int appendBias,	/* True if this is likely an append */
		   int seekResult	/* Result of prior MovetoUnpacked() call */
    )
{
	(void)appendBias;
	(void)seekResult;

	if (pCur->eState == CURSOR_FAULT) {
		assert(pCur->skipNext != SQLITE_OK);
		return pCur->skipNext;
	}

	/* Assert that the caller has been consistent. If this cursor was opened
	 * expecting an index b-tree, then the caller should be inserting blob
	 * keys with no associated data. If the cursor was opened expecting an
	 * intkey table, the caller should be inserting integer keys with a
	 * blob of associated data.
	 */
	assert((pX->pKey == 0) == (pCur->pKeyInfo == 0));

	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3Insert(pCur, pX);
	}

	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralInsert(pCur, pX);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

int
sqlite3BtreeDelete(BtCursor * pCur, u8 flags)
{
	assert(pCur->eState == CURSOR_VALID);

	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3Delete(pCur, flags);
	}

	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralDelete(pCur);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}

/*
 * Delete all information from the single table that pCur is open on.
 *
 * This routine only work for pCur on an ephemeral table.
 */
int
sqlite3BtreeClearTableOfCursor(BtCursor * pCur)
{
	if (pCur->curFlags & BTCF_TEphemCursor)
		return tarantoolSqlite3EphemeralClearTable(pCur);
	return SQLITE_OK;
}

/*
 * Write meta-information back into the database.  Meta[0] is
 * read-only and may not be written.
*/

#ifndef SQLITE_OMIT_BTREECOUNT
/*
 * The first argument, pCur, is a cursor opened on some b-tree. Count the
 * number of entries in the b-tree and write the result to *pnEntry.
 *
 * SQLITE_OK is returned if the operation is successfully executed.
 * Otherwise, if an error is encountered (i.e. an IO error or database
 * corruption) an SQLite error code is returned.
 */
int
sqlite3BtreeCount(BtCursor * pCur, i64 * pnEntry)
{
	if (pCur->curFlags & BTCF_TaCursor) {
		return tarantoolSqlite3Count(pCur, pnEntry);
	}

	if (pCur->curFlags & BTCF_TEphemCursor) {
		return tarantoolSqlite3EphemeralCount(pCur, pnEntry);
	}
	unreachable();
	return SQLITE_TARANTOOL_ERROR;
}
#endif

/*
 * Return non-zero if a transaction is active.
 */
int
sqlite3BtreeIsInTrans(Btree * p)
{
	return (p && (p->inTrans == TRANS_WRITE));
}

/*
 * Return non-zero if a read (or write) transaction is active.
 */
int
sqlite3BtreeIsInReadTrans(Btree * p)
{
	assert(p);
	return p->inTrans != TRANS_NONE;
}

/*
 * Return true if the cursor has a hint specified.  This routine is
 * only used from within assert() statements
 */
int
sqlite3BtreeCursorHasHint(BtCursor * pCsr, unsigned int mask)
{
	return (pCsr->hints & mask) != 0;
}
