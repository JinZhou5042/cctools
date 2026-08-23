/* Worker-side pull of one immutable DataVine content object. */
#ifndef VINE_DATAVINE_TRANSFER_H
#define VINE_DATAVINE_TRANSFER_H

int vine_datavine_transfer_get(const char *source, const char *destination,
		char **error_message);

#endif
