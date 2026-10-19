/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "url_encode.h"

#include <stdio.h>
#include <string.h>

void url_encode(const char *s, char *t, int length)
{
	if (s) {
		while (*s && length > 1) {
			/* A plain char is negative for bytes above 127 where it is signed, so compare and format the byte unsigned. */
			unsigned char c = *s;
			if (c <= 32 || c == '%' || c == '\\' || c == '<' || c == '>' || c == '\'' || c == '\"' || c > 122) {
				if (length > 3) {
					/* Always two digits, so that a control character such as a newline does not become a space. */
					snprintf(t, length, "%%%02X", c);
					t += 3;
					length -= 3;
					s++;
				} else {
					break;
				}
			} else {
				*t++ = *s++;
				length--;
			}
		}
	}
	*t = 0;
}

void url_decode(const char *s, char *t, int length)
{
	while (*s && length > 1) {
		if (*s == '%') {
			unsigned int x;
			sscanf(s + 1, "%2x", &x);
			*t++ = x;
			s += 3;
		} else {
			*t++ = *s++;
		}
		length--;
	}
	*t = 0;
}

/* vim: set noexpandtab tabstop=8: */
