
#include "budget.h"
#include "create_dir.h"
#include "debug.h"
#include "random.h"
#include "stringtools.h"
#include "timestamp.h"
#include "unlink_recursive.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "trash.h"

static char *trashdir = 0;

/* Non-zero while the trash may hold entries that no complete pass of trash_empty_budget() has tried to delete. */
static int trash_pending = 0;

/* The directory inside the trash where trash_empty_budget() sets aside entries that it could not delete, so that a
 * later pass does not try them again before newer entries. trash_empty() tries them again. */
#define TRASH_KEPT ".kept"

void trash_setup(const char *dir)
{
	if (trashdir) {
		if (!strcmp(dir, trashdir)) {
			/* same directory, no problem */
		} else {
			notice(D_NOTICE, "Trash directory already setup to %s. Ignoring setup for %s.", trashdir, dir);
		}
		return;
	}

	trashdir = strdup(dir);
	create_dir(trashdir, 0700);
	random_init();
}

/*
A direct unlink of a file may fail, in particular if the file
is being executed by a process.  To avoid the problem of unlinkable
files, we instead move a file to a random name within the trash directory,
and then attempt to delete it there.  If that fails, it's ok b/c we will
get it later on the next attempt to empty the trash.
*/

#define TRASH_COOKIE_LENGTH 8

void trash_move(const char *filename)
{
	/* Generate a unique random name */
	char cookie[TRASH_COOKIE_LENGTH];
	string_cookie(cookie, TRASH_COOKIE_LENGTH);

	char *trashname = string_format("%s/%s.%" PRIu64, trashdir, cookie, timestamp_get());

	/* Move the file to the trash directory. If the original file doesn't exist, then return success. The rename
	 * itself tells, so a file costs one system call, which matters on a slow disk. */
	int result = rename(filename, trashname);
	if (result != 0 && errno != ENOENT) {
		fatal("failed to move file (%s) to trash location (%s): %s", filename, trashname, strerror(errno));
	}
	if (result == 0) {
		debug(D_DEBUG, "trashing file %s", filename);
		trash_pending = 1;
	}

	free(trashname);
}

void trash_file(const char *filename)
{
	trash_move(filename);
	trash_empty();
}

void trash_empty()
{
	int result = unlink_dir_contents(trashdir);
	if (result != 0) {
		warn(D_ERROR, "unable to delete all items in trash directory (%s), will try again later.", trashdir);
	}
	trash_pending = 0;
}

int trash_empty_budget(const struct budget *b)
{
	if (!trash_pending || !trashdir) {
		return 1;
	}

	int dirfd = open(trashdir, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOCTTY);
	if (dirfd < 0) {
		warn(D_ERROR, "unable to open trash directory (%s): %s", trashdir, strerror(errno));
		trash_pending = 0;
		return 1;
	}
	DIR *dir = fdopendir(dirfd);
	if (!dir) {
		close(dirfd);
		trash_pending = 0;
		return 1;
	}

	int complete = 1;
	struct dirent *d;
	while ((d = readdir(dir))) {
		if (!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..") || !strcmp(d->d_name, TRASH_KEPT)) {
			continue;
		}
		int r = unlinkat_recursive_budget(dirfd, d->d_name, b);
		if (r < 0) {
			/* What could not be deleted is set aside, so that later passes reach newer entries instead of trying it
			 * again first, and it is reported once. */
			char *kept = string_format("%s/%s", TRASH_KEPT, d->d_name);
			if (mkdirat(dirfd, TRASH_KEPT, 0700) < 0 && errno != EEXIST) {
				warn(D_ERROR, "unable to create %s/%s: %s", trashdir, TRASH_KEPT, strerror(errno));
			}
			if (renameat(dirfd, d->d_name, dirfd, kept) < 0) {
				warn(D_ERROR, "unable to delete or set aside %s/%s: %s", trashdir, d->d_name, strerror(errno));
			} else {
				warn(D_ERROR, "unable to delete %s/%s, set it aside in %s/%s", trashdir, d->d_name, trashdir, TRASH_KEPT);
			}
			free(kept);
		}
		if (r == 0 || budget_expired(b)) {
			complete = 0;
			break;
		}
	}
	closedir(dir);

	if (complete) {
		trash_pending = 0;
	}
	return complete;
}

/* vim: set noexpandtab tabstop=8: */
