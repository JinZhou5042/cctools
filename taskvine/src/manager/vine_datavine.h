/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_DATAVINE_H
#define VINE_DATAVINE_H

struct vine_manager;

int vine_datavine_configure(struct vine_manager *q, const char *endpoint, const char *token);
int vine_datavine_resolve_transfer(struct vine_manager *q, const char *data_id,
		const char *destination_worker_id, const char *excluded_worker_id,
		const char *transfer_id, char **source_worker_id);
int vine_datavine_release_transfer(struct vine_manager *q, const char *transfer_id, int success);

#endif
