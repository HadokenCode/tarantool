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

#include "sqliteInt.h"

/* A Btree handle
 *
 * A database connection contains a pointer to an instance of
 * this object for every database file that it has open.  This structure
 * is opaque to the database connection.  The database connection cannot
 * see the internals of this structure and only deals with pointers to
 * this structure.
 *
 * For some database files, the same underlying database cache might be
 * shared between multiple connections.  In that case, each connection
 * has it own instance of this object.  But each instance of this object
 * points to the same BtShared object.  The database cache and the
 * schema associated with the database file are all contained within
 * the BtShared object.
 *
 * All fields in this structure are accessed under sqlite3.mutex.
 * The pBt pointer itself may not be changed while there exists cursors
 * in the referenced BtShared that point back to this Btree since those
 * cursors have to go through this Btree to find their BtShared and
 * they often do so without holding sqlite3.mutex.
 */
struct Btree {
	sqlite3 *db;		/* The database connection holding this btree */
	u8 inTrans;		/* TRANS_NONE, TRANS_READ or TRANS_WRITE */
	Btree *pNext;		/* List of other sharable Btrees from the same db */
	Btree *pPrev;		/* Back pointer of the same list */
};

/*
 * Btree.inTrans may take one of the following values.
 *
 * If the shared-data extension is enabled, there may be multiple users
 * of the Btree structure. At most one of these may open a write transaction,
 * but any number may have active read transactions.
 */
#define TRANS_NONE  0
#define TRANS_READ  1
#define TRANS_WRITE 2

/*
 * A cursor is a pointer to a particular entry within a particular
 * b-tree within a database file.
 *
 * The entry is identified by its MemPage and the index in
 * MemPage.aCell[] of the entry.
 *
 * A single database file can be shared by two more database connections,
 * but cursors cannot be shared.  Each cursor is associated with a
 * particular database connection identified BtCursor.pBtree.db.
 *
 * Fields in this structure are accessed under the BtShared.mutex
 * found at self->pBt->mutex.
 *
 * skipNext meaning:
 *    eState==SKIPNEXT && skipNext>0:  Next sqlite3BtreeNext() is no-op.
 *    eState==SKIPNEXT && skipNext<0:  Next sqlite3BtreePrevious() is no-op.
 *    eState==FAULT:                   Cursor fault with skipNext as error code.
 */
struct BtCursor {
	Btree *pBtree;		/* The Btree to which this cursor belongs */
	BtCursor *pNext;	/* Forms a linked list of all cursors */
	i64 nKey;		/* Size of pKey, or last integer key */
	void *pKey;		/* Saved key that was cursor last known position */
	Pgno pgnoRoot;		/* The root page of this tree */
	int skipNext;		/* Prev() is noop if negative. Next() is noop if positive.
				 * Error code if eState==CURSOR_FAULT
				 */
	u8 curFlags;		/* zero or more BTCF_* flags defined below */
	u8 eState;		/* One of the CURSOR_XXX constants (see below) */
	u8 hints;		/* As configured by CursorSetHints() */
	/* All fields above are zeroed when the cursor is allocated.  See
	 * sqlite3BtreeCursorZero().  Fields that follow must be manually
	 * initialized.
	 */
	u8 curIntKey;		/* Value of apPage[0]->intKey */
	struct KeyInfo *pKeyInfo;	/* Argument passed to comparison function */
	void *pTaCursor;	/* Tarantool cursor */
};

/*
 * Legal values for BtCursor.curFlags
 */
#define BTCF_TaCursor     0x80	/* Tarantool cursor, pTaCursor valid */
#define BTCF_TEphemCursor 0x40	/* Tarantool cursor to ephemeral table  */

/*
 * Potential values for BtCursor.eState.
 *
 * CURSOR_INVALID:
 *   Cursor does not point to a valid entry. This can happen (for example)
 *   because the table is empty or because BtreeCursorFirst() has not been
 *   called.
 *
 * CURSOR_VALID:
 *   Cursor points to a valid entry. getPayload() etc. may be called.
 *
 * CURSOR_SKIPNEXT:
 *   Cursor is valid except that the Cursor.skipNext field is non-zero
 *   indicating that the next sqlite3BtreeNext() or sqlite3BtreePrevious()
 *   operation should be a no-op.
 *
 * CURSOR_REQUIRESEEK:
 *   The table that this cursor was opened on still exists, but has been
 *   modified since the cursor was last used. The cursor position is saved
 *   in variables BtCursor.pKey and BtCursor.nKey. When a cursor is in
 *   this state, restoreCursorPosition() can be called to attempt to
 *   seek the cursor to the saved position.
 *
 * CURSOR_FAULT:
 *   An unrecoverable error (an I/O error or a malloc failure) has occurred
 *   on a different connection that shares the BtShared cache with this
 *   cursor.  The error has left the cache in an inconsistent state.
 *   Do nothing else with this cursor.  Any attempt to use the cursor
 *   should return the error code stored in BtCursor.skipNext
 */
#define CURSOR_INVALID           0
#define CURSOR_VALID             1
#define CURSOR_SKIPNEXT          2
#define CURSOR_REQUIRESEEK       3
#define CURSOR_FAULT             4

/*
 * Routines to read or write a two- and four-byte big-endian integer values.
 */
#define get2byte(x)   ((x)[0]<<8 | (x)[1])
#define put2byte(p,v) ((p)[0] = (u8)((v)>>8), (p)[1] = (u8)(v))
#define get4byte sqlite3Get4byte
#define put4byte sqlite3Put4byte

/*
 * get2byteAligned(), unlike get2byte(), requires that its argument point to a
 * two-byte aligned address.  get2bytea() is only used for accessing the
 * cell addresses in a btree header.
 */
#if SQLITE_BYTEORDER==4321
#define get2byteAligned(x)  (*(u16*)(x))
#elif SQLITE_BYTEORDER==1234 && !defined(SQLITE_DISABLE_INTRINSIC) \
    && GCC_VERSION>=4008000
#define get2byteAligned(x)  __builtin_bswap16(*(u16*)(x))
#elif SQLITE_BYTEORDER==1234 && !defined(SQLITE_DISABLE_INTRINSIC) \
    && defined(_MSC_VER) && _MSC_VER>=1300
#define get2byteAligned(x)  _byteswap_ushort(*(u16*)(x))
#else
#define get2byteAligned(x)  ((x)[0]<<8 | (x)[1])
#endif
