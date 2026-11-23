/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_print.h"
#include "jx_parse.h"

#include <assert.h>
#include <ctype.h>
#include <stdint.h>
#include <string.h>

/* Powers of ten that fit in 64 bits, for formatting numbers without printf. */
static const uint64_t jx_print_pow10[20] = {1ULL,
		10ULL,
		100ULL,
		1000ULL,
		10000ULL,
		100000ULL,
		1000000ULL,
		10000000ULL,
		100000000ULL,
		1000000000ULL,
		10000000000ULL,
		100000000000ULL,
		1000000000000ULL,
		10000000000000ULL,
		100000000000000ULL,
		1000000000000000ULL,
		10000000000000000ULL,
		100000000000000000ULL,
		1000000000000000000ULL,
		10000000000000000000ULL};

void jx_print_integer(jx_int_t value, buffer_t *b)
{
	char text[24];
	char *end = text + sizeof(text);
	char *p = end;
	uint64_t magnitude = value < 0 ? -(uint64_t)value : (uint64_t)value;

	do {
		*--p = '0' + magnitude % 10;
		magnitude /= 10;
	} while (magnitude);

	if (value < 0) {
		*--p = '-';
	}

	buffer_putlstring(b, p, end - p);
}

/* Format a double exactly as printf("%.16g") does, for finite values whose magnitude is zero or within [1e-7, 1e16),
 * whose decimal exponent is at most 15, so that 16 digits fit in 64 bits. Returns the length written to text (at least
 * 32 bytes), or -1 for any other value, which the caller formats with printf. The value is M / 2^s exactly, so the 16
 * significant digits are the exact product M * 10^p / 2^s rounded half to even, as glibc rounds in the default rounding
 * mode, and M * 10^p fits in 128 bits for p up to 22. Compilers without 128-bit integers always use printf. */
#ifndef __SIZEOF_INT128__
static int jx_print_double_text(double value, char *text)
{
	return -1;
}
#else
static int jx_print_double_text(double value, char *text)
{
	uint64_t bits;
	memcpy(&bits, &value, sizeof(bits));

	int negative = (int)(bits >> 63);
	int exponent_bits = (int)((bits >> 52) & 0x7ff);
	uint64_t fraction = bits & ((1ULL << 52) - 1);
	char *p = text;

	if (negative) {
		*p++ = '-';
	}

	if (exponent_bits == 0 && fraction == 0) {
		*p++ = '0';
		return p - text;
	}

	double magnitude = negative ? -value : value;
	if (exponent_bits == 0x7ff || !(magnitude >= 1e-7 && magnitude < 1e16)) {
		return -1;
	}

	uint64_t mantissa = fraction | (1ULL << 52);
	int shift = 1075 - exponent_bits;

	/* Estimate the decimal exponent, then correct it from the rounded digits, because %g takes the exponent after
	 * rounding to 16 digits. */
	int e = 15;
	while (e > 0 && magnitude < (double)jx_print_pow10[e]) {
		e--;
	}
	if (e == 0 && magnitude < 1) {
		e = -1;
		double bound = 0.1;
		while (e > -7 && magnitude < bound) {
			e--;
			bound /= 10;
		}
	}

	uint64_t digits = 0;
	int attempt;
	for (attempt = 0; attempt < 4; attempt++) {
		if (e < -7 || e > 15) {
			return -1;
		}
		int p10 = 15 - e;
		unsigned __int128 product = (unsigned __int128)mantissa * jx_print_pow10[p10 > 19 ? 19 : p10];
		if (p10 > 19) {
			product *= jx_print_pow10[p10 - 19];
		}
		unsigned __int128 n;
		if (shift <= 0) {
			n = product << -shift;
		} else {
			n = product >> shift;
			unsigned __int128 remainder = product - (n << shift);
			unsigned __int128 half = (unsigned __int128)1 << (shift - 1);
			if (remainder > half || (remainder == half && (n & 1))) {
				n++;
			}
		}
		if (n >= jx_print_pow10[16]) {
			e++;
		} else if (n < jx_print_pow10[15]) {
			e--;
		} else {
			digits = (uint64_t)n;
			break;
		}
	}
	if (attempt == 4) {
		return -1;
	}

	char d[16];
	int i;
	for (i = 15; i >= 0; i--) {
		d[i] = '0' + digits % 10;
		digits /= 10;
	}

	/* %g drops trailing zeros, and the decimal point when no fraction remains. */
	int significant = 16;
	while (significant > 1 && d[significant - 1] == '0') {
		significant--;
	}

	if (e >= 0) {
		memcpy(p, d, e + 1);
		p += e + 1;
		if (significant > e + 1) {
			*p++ = '.';
			memcpy(p, d + e + 1, significant - e - 1);
			p += significant - e - 1;
		}
	} else if (e >= -4) {
		*p++ = '0';
		*p++ = '.';
		for (i = 0; i < -e - 1; i++) {
			*p++ = '0';
		}
		memcpy(p, d, significant);
		p += significant;
	} else {
		/* %g uses the exponent style below 1e-4, with at least two digits of exponent. */
		*p++ = d[0];
		if (significant > 1) {
			*p++ = '.';
			memcpy(p, d + 1, significant - 1);
			p += significant - 1;
		}
		*p++ = 'e';
		*p++ = '-';
		*p++ = '0';
		*p++ = '0' - e;
	}

	return p - text;
}
#endif

void jx_print_double(double value, buffer_t *b)
{
	char text[32];
	int length = jx_print_double_text(value, text);

	if (length < 0) {
		buffer_printf(b, "%.16g", value);
	} else {
		buffer_putlstring(b, text, length);
	}
}

void jx_comprehension_print(struct jx_comprehension *comp, buffer_t *b)
{
	if (!comp)
		return;

	buffer_putstring(b, " for ");
	buffer_putstring(b, comp->variable);
	buffer_putstring(b, " in ");
	jx_print_buffer(comp->elements, b);
	if (comp->condition) {
		buffer_putstring(b, " if ");
		jx_print_buffer(comp->condition, b);
	}

	jx_comprehension_print(comp->next, b);
}

static void jx_pair_print(struct jx_pair *pair, buffer_t *b)
{
	if (!pair)
		return;

	jx_print_buffer(pair->key, b);
	buffer_putstring(b, ":");
	jx_print_buffer(pair->value, b);
	jx_comprehension_print(pair->comp, b);
	if (pair->next) {
		buffer_putstring(b, ",");
		jx_pair_print(pair->next, b);
	}
}

static void jx_item_print(struct jx_item *item, buffer_t *b)
{
	if (!item)
		return;

	jx_print_buffer(item->value, b);
	jx_comprehension_print(item->comp, b);

	if (item->next) {
		buffer_putstring(b, ",");
		jx_item_print(item->next, b);
	}
}

const char *jx_operator_string(jx_operator_t type)
{
	switch (type) {
	case JX_OP_EQ:
		return "==";
	case JX_OP_NE:
		return "!=";
	case JX_OP_LT:
		return "<";
	case JX_OP_LE:
		return "<=";
	case JX_OP_GT:
		return ">";
	case JX_OP_GE:
		return ">=";
	case JX_OP_ADD:
		return "+";
	case JX_OP_SUB:
		return "-";
	case JX_OP_MUL:
		return "*";
	case JX_OP_DIV:
		return "/";
	case JX_OP_MOD:
		return "%";
	case JX_OP_AND:
		return " and ";
	case JX_OP_OR:
		return " or ";
	case JX_OP_NOT:
		return " not ";
	// note that the closing bracket/paren is in jx_print_subexpr
	case JX_OP_LOOKUP:
		return "[";
	case JX_OP_CALL:
		return "(";
	case JX_OP_DOT:
		return ".";
	case JX_OP_SLICE:
		return ":";
	default:
		return "???";
	}
}

const char *jx_type_string(jx_type_t type)
{
	switch (type) {
	case JX_NULL:
		return "null";
	case JX_BOOLEAN:
		return "boolean";
	case JX_INTEGER:
		return "integer";
	case JX_DOUBLE:
		return "float";
	case JX_STRING:
		return "string";
	case JX_SYMBOL:
		return "symbol";
	case JX_ARRAY:
		return "array";
	case JX_OBJECT:
		return "object";
	case JX_OPERATOR:
		return "operator";
	case JX_ERROR:
		return "error";
	default:
		return "???";
	}
}

void jx_escape_string(const char *s, buffer_t *b)
{
	if (!s)
		return;

	buffer_putstring(b, "\"");
	while (*s) {
		/* Copy a run of characters that need no escaping at once, because formatting one character at a time
		 * dominated the cost of printing. */
		const char *run = s;
		while (*s && *s != '\"' && *s != '\\' && isprint(*s)) {
			s++;
		}
		if (s > run) {
			buffer_putlstring(b, run, s - run);
			continue;
		}
		switch (*s) {
		case '\"':
			buffer_putstring(b, "\\\"");
			break;
		case '\\':
			buffer_putstring(b, "\\\\");
			break;
		case '\b':
			buffer_putstring(b, "\\b");
			break;
		case '\f':
			buffer_putstring(b, "\\f");
			break;
		case '\n':
			buffer_putstring(b, "\\n");
			break;
		case '\r':
			buffer_putstring(b, "\\r");
			break;
		case '\t':
			buffer_putstring(b, "\\t");
			break;
		default:
			/* Printable characters were copied above. */
			buffer_printf(b, "\\u%04x", (int)*s);
			break;
		}
		s++;
	}
	buffer_putstring(b, "\"");
}

void jx_print_subexpr(struct jx *j, jx_operator_t parent, buffer_t *b)
{
	if (!j)
		return;

	int do_parens = 0;

	if (j->type == JX_OPERATOR && jx_operator_precedence(parent) > jx_operator_precedence(j->u.oper.type)) {
		do_parens = 1;
	} else {
		do_parens = 0;
	}

	if (do_parens)
		buffer_putstring(b, "(");
	jx_print_buffer(j, b);
	if (do_parens)
		buffer_putstring(b, ")");
}

void jx_print_args(struct jx *j, buffer_t *b)
{
	if (!jx_istype(j, JX_ARRAY))
		return;
	jx_item_print(j->u.items, b);
}

void jx_print_buffer(struct jx *j, buffer_t *b)
{
	if (!j)
		return;

	switch (j->type) {
	case JX_NULL:
		buffer_putstring(b, "null");
		break;
	case JX_DOUBLE:
		jx_print_double(j->u.double_value, b);
		break;
	case JX_BOOLEAN:
		buffer_printf(b, "%s", j->u.boolean_value ? "true" : "false");
		break;
	case JX_INTEGER:
		jx_print_integer(j->u.integer_value, b);
		break;
	case JX_SYMBOL:
		buffer_printf(b, "%s", j->u.symbol_name);
		break;
	case JX_STRING:
		jx_escape_string(j->u.string_value, b);
		break;
	case JX_ARRAY:
		buffer_putstring(b, "[");
		jx_item_print(j->u.items, b);
		buffer_putstring(b, "]");
		break;
	case JX_OBJECT:
		buffer_putstring(b, "{");
		jx_pair_print(j->u.pairs, b);
		buffer_putstring(b, "}");
		break;
	case JX_OPERATOR:
		jx_print_subexpr(j->u.oper.left, j->u.oper.type, b);
		buffer_putstring(b, jx_operator_string(j->u.oper.type));
		if (j->u.oper.type == JX_OP_CALL) {
			jx_item_print(j->u.oper.right->u.items, b);
			buffer_putstring(b, ")");
		} else {
			jx_print_subexpr(j->u.oper.right, j->u.oper.type, b);
		}
		if (j->u.oper.type == JX_OP_LOOKUP)
			buffer_putstring(b, "]");
		break;
	case JX_ERROR:
		buffer_putstring(b, "error(");
		jx_print_buffer(j->u.err, b);
		buffer_putstring(b, ")");
		break;
	}
}

void jx_print_stream(struct jx *j, FILE *file)
{
	buffer_t buffer;
	buffer_init(&buffer);
	jx_print_buffer(j, &buffer);
	fprintf(file, "%s", buffer_tostring(&buffer));
	buffer_free(&buffer);
}

void jx_print_link(struct jx *j, struct link *l, time_t stoptime)
{

	buffer_t buffer;
	buffer_init(&buffer);
	jx_print_buffer(j, &buffer);

	size_t len;
	const char *str = buffer_tolstring(&buffer, &len);

	link_printf(l, stoptime, "%s", str);
	buffer_free(&buffer);
}

char *jx_print_string(struct jx *j)
{

	buffer_t buffer;
	char *str;
	buffer_init(&buffer);
	jx_print_buffer(j, &buffer);
	buffer_dup(&buffer, &str);
	buffer_free(&buffer);
	return str;
}

static char *unquoted_string(struct jx *j)
{
	char *str;
	if (j->type == JX_STRING) {
		str = strdup(j->u.string_value);
	} else {
		str = jx_print_string(j);
	}
	return str;
}

/*
Export a JX object as environment variables in bash format.
*/

void jx_print_shell(struct jx *j, FILE *stream)
{
	struct jx_pair *p;
	for (p = j->u.pairs; p; p = p->next) {
		char *str = unquoted_string(p->value);
		fprintf(stream, "export %s=%s\n", p->key->u.string_value, str);
		free(str);
	}
}
