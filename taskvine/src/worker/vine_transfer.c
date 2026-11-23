/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_transfer.h"
#include "vine_protocol.h"
#include "vine_worker.h"

#include "debug.h"
#include "host_disk_info.h"
#include "link.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"
#include "url_encode.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
This module implements the streaming directory transfer,
making it efficient to move large directory trees without
multiple round trips needed for remote procedure calls.

Each file, directory, or symlink is represented by a single
header line giving the name, length, and mode of the entry.
Files and symlinks are followed by the raw contents of the file
or link, respectively, while directories are followed by more
lines containing the contents of the directory, until an "end"
is received.

For example, the following directory tree:

- mydir
-- 1.txt
-- 2.txt
-- mysubdir
--- a.txt
--- b.txt
-- z.jpb

Is represented as follows:

dir mydir
file 1.txt 35291 0600
  (35291 bytes of 1.txt)
file 2.txt 502 0666
  (502 bytes of 2.txt)
dir mysubdir
file a.txt 321 0600
  (321 bytes of a.txt)
file b.txt 456 0600
  (456 bytes of a.txt)
end
file z.jpg 40001 0644
  (40001 bytes of z.jpg)
end

*/

/*
Queue a cached object of any type to be sent. Return zero if the writer refused it.
*/

int vine_transfer_put_any(struct link_writer *lw, struct vine_cache *cache, const char *filename, vine_transfer_mode_t xfer_mode)
{
	char *cached_path = vine_cache_data_path(cache, filename);
	int result;

	if (path_within_dir(cached_path, workspace->cache_dir)) {
		result = vine_transfer_queue_item(lw, cached_path, path_basename(filename), xfer_mode);
	} else {
		debug(D_VINE, "refusing to send %s: not within %s", cached_path, workspace->cache_dir);
		result = EACCES;
	}
	if (result > 0) {
		result = vine_transfer_queue_error(lw, path_basename(filename), result) ? 0 : -1;
	}

	free(cached_path);
	return result == 0;
}

int vine_transfer_request_any(struct link *lnk, const char *request_path, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message)
{
	debug(D_VINE, "tx: get %s", request_path);
	struct link_writer *lw = link_writer_create(lnk);
	if (!lw) {
		return 0;
	}
	char *request = string_format("get %s\n", request_path);
	int sent = link_writer_push(lw, request, strlen(request)) && link_writer_send_sync(lw, timestamp_monotonic() + timeout * 1000000LL, 0);
	free(request);
	link_writer_delete(lw);
	if (!sent) {
		return 0;
	}
	return vine_transfer_get_any(lnk, dirname, totalsize, mode, mtime, timeout, error_message);
}
