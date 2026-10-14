/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file vine_protocol.h
This file describes a handful of constants that are necessary for
a common implementation of the protocol between the manager,
worker, and catalog, but should not be visible to the public user API.
*/

#ifndef VINE_PROTOCOL_H
#define VINE_PROTOCOL_H

#define VINE_PROTOCOL_VERSION 13

#define VINE_LINE_MAX 4096       /**< Maximum length of a vine message line. */

/* Default response timeout X in seconds: a healthy party makes visible progress within this time. The manager tunes it
 * with response-timeout and sends it to each worker that accepts it. */
#define VINE_DEFAULT_RESPONSE_TIMEOUT 15

/* The tolerance k, a policy: a wait on a party fails when the party makes no progress for T = k X. */
#define VINE_TIMEOUT_TOLERANCE 2

/* Microseconds of work in one round of the worker's main loop and of message handling in one step of the manager's
 * vine_wait(), a constant that keeps both well within X. */
#define VINE_ROUND_BUDGET 100000

#endif
