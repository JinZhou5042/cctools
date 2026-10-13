
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
	struct stat info;

	/* If the original file doesn't exist, then return success. */
	int result = stat(filename, &info);
	if (result < 0 && errno == ENOENT)
		return;

	/* Generate a unique random name */
	char cookie[TRASH_COOKIE_LENGTH];
	string_cookie(cookie, TRASH_COOKIE_LENGTH);

	char *trashname = string_format("%s/%s.%" PRIu64, trashdir, cookie, timestamp_get());
	debug(D_DEBUG, "trashing file %s", filename);

	/* Move the file to the trash directory. */
	result = rename(filename, trashname);
	if (result != 0) {
		fatal("failed to move file (%s) to trash location (%s): %s", filename, trashname, strerror(errno));
	}
	trash_pending = 1;

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

/*
Delete path, relative to dirfd, and everything below it, checking the budget after each entry. Return 1 if path is
gone, 0 if the budget expired first, and -1 if something could not be deleted. A later call resumes by walking what is
left, so a partly deleted tree needs no saved state.
*/

static int unlinkat_budget(int dirfd, const char *path, const struct budget *b)
{
	if (unlinkat(dirfd, path, 0) == 0 || errno == ENOENT) {
		return 1;
	}
	if (errno != EISDIR && errno != EPERM && errno != ENOTEMPTY) {
		warn(D_ERROR, "couldn't delete %s: %s\n", path, strerror(errno));
		return -1;
	}

	/* Another process may empty the trash at the same time, so an entry that vanished is deleted. */
	int subdirfd = openat(dirfd, path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOCTTY | O_NOFOLLOW, 0);
	if (subdirfd < 0) {
		return errno == ENOENT ? 1 : -1;
	}
	DIR *dir = fdopendir(subdirfd);
	if (!dir) {
		close(subdirfd);
		return -1;
	}

	int result = 1;
	struct dirent *d;
	while ((d = readdir(dir))) {
		if (!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) {
			continue;
		}
		int r = unlinkat_budget(subdirfd, d->d_name, b);
		if (r == 0 || budget_expired(b)) {
			result = 0;
			break;
		}
		if (r < 0) {
			/* Keep going, so as to delete as much as possible. */
			result = -1;
		}
	}
	closedir(dir);

	if (result == 0) {
		return 0;
	}

	/* After an interior failure this fails too. */
	if (unlinkat(dirfd, path, AT_REMOVEDIR) < 0 && errno != ENOENT) {
		warn(D_ERROR, "couldn't delete directory %s: %s\n", path, strerror(errno));
		return -1;
	}
	return result;
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
	int failed = 0;
	struct dirent *d;
	while ((d = readdir(dir))) {
		if (!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) {
			continue;
		}
		int r = unlinkat_budget(dirfd, d->d_name, b);
		if (r < 0) {
			failed = 1;
		}
		if (r == 0 || budget_expired(b)) {
			complete = 0;
			break;
		}
	}
	closedir(dir);

	if (complete) {
		/* Entries that could not be deleted are tried again after the next file is moved here, as trash_file() does. */
		if (failed) {
			warn(D_ERROR, "unable to delete all items in trash directory (%s), will try again later.", trashdir);
		}
		trash_pending = 0;
	}
	return complete;
}

/* vim: set noexpandtab tabstop=8: */
