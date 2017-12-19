/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "schema_def.h"
#include <wchar.h>
#include <wctype.h>
#include <unicode/ucnv_err.h>
#include <unicode/ucnv.h>
#include <unicode/uchar.h>

static const char *object_type_strs[] = {
	/* [SC_UKNNOWN]         = */ "unknown",
	/* [SC_UNIVERSE]        = */ "universe",
	/* [SC_SPACE]           = */ "space",
	/* [SC_FUNCTION]        = */ "function",
	/* [SC_USER]            = */ "user",
	/* [SC_ROLE]            = */ "role",
	/* [SC_SEQUENCE]        = */ "sequence",
	/* [SC_COLLATION]       = */ "collation",
};

/* ICU returns this character in case of unknown symbol */
#define REPLACEMENT_CHARACTER (0xFFFD)

static UConverter* utf8conv = NULL;

enum schema_object_type
schema_object_type(const char *name)
{
	/**
	 * There may be other places in which we look object type by
	 * name, and they are case-sensitive, so be case-sensitive
	 * here too.
	 */
	int n_strs = sizeof(object_type_strs)/sizeof(*object_type_strs);
	int index = strindex(object_type_strs, name, n_strs);
	return (enum schema_object_type) (index == n_strs ? 0 : index);
}

const char *
schema_object_name(enum schema_object_type type)
{
	return object_type_strs[type];
}

bool
identifier_characters_is_valid(const char *str, uint32_t str_len)
{
	assert(utf8conv);
	if (str_len == 0)
		return false;
	const char * end = str + str_len;
	UChar32 c;
	UErrorCode status = U_ZERO_ERROR ;
	ucnv_reset(utf8conv);
	while(str < end){
		c = ucnv_getNextUChar(utf8conv, &str, end, &status);
		int8_t type = u_charType(c);
		if (U_FAILURE(status))
			return false;
		if (c == REPLACEMENT_CHARACTER ||
			type == U_UNASSIGNED ||
			type == U_LINE_SEPARATOR ||
			type == U_CONTROL_CHAR ||
			type == U_PARAGRAPH_SEPARATOR)
			return false;
	}
	return true;
}

int
init_identifier_check(){
	assert(utf8conv == NULL);
	UErrorCode status = U_ZERO_ERROR ;
	utf8conv = ucnv_open("utf8", &status);
	if (U_FAILURE(status))
		return -1;
	return 0;
}

void
destroy_identifier_check(){
	assert(utf8conv);
	ucnv_close(utf8conv);
	utf8conv = NULL;
}
