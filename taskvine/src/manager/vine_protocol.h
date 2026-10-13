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

#define VINE_PROTOCOL_VERSION 14

#define VINE_LINE_MAX 4096       /**< Maximum length of a vine message line. */

/* Default response timeout X in seconds: a healthy worker or data peer makes visible progress within this time, and
 * every wait on a peer fails when the peer makes no progress for twice as long. The manager tunes it with
 * response-timeout and sends it to each worker, which bounds every round of its main loop by it. */
#define VINE_DEFAULT_RESPONSE_TIMEOUT 15

#endif
