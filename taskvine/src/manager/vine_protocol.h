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

/* Default response time X in seconds of a healthy manager or worker. A link whose peer makes no progress for T = 2X
 * fails. The manager tunes it with response-timeout, the worker with --response-timeout. */
#define VINE_DEFAULT_RESPONSE_TIMEOUT 15

#endif
