/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_transfer_server.h"
#include "vine_protocol.h"
#include "vine_transfer.h"
#include "vine_worker.h"

#include "change_process_title.h"
#include "debug.h"
#include "link.h"
#include "link_auth.h"
#include "process.h"
#include "url_encode.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* The server link from which connections are accepted */
static struct link *transfer_link = 0;

/* Pid of process handling peer transfers. */
pid_t transfer_server_pid = 0;

/* Specific port for the transfer server to listen on.  Zero means choose any available. */
int vine_transfer_server_port = 0;

/* Answer one get request on an authenticated connection. The reply may take as long as the requester keeps taking
 * data within the idle timeout. */

static void vine_transfer_serve_request(struct link *lnk, struct vine_cache *cache, time_t command_stoptime)
{
	char line[VINE_LINE_MAX];
	char filename_encoded[VINE_LINE_MAX];
	char filename[VINE_LINE_MAX];

	if (link_readline(lnk, line, sizeof(line), command_stoptime)) {
		link_set_idle_timeout(lnk, vine_worker_options_idle_timeout(options));
		if (sscanf(line, "get %s", filename_encoded) == 1) {
			url_decode(filename_encoded, filename, sizeof(filename));
			vine_transfer_put_any(lnk, cache, filename, VINE_TRANSFER_MODE_ANY, LINK_FOREVER);
		} else {
			debug(D_VINE, "invalid peer transfer message: %s\n", line);
		}
	}
}

/* Handle a single request for a transfer request from a peer, which sends its request right after it connects and
 * then makes progress within the idle timeout. */

static void vine_transfer_handler(struct link *lnk, struct vine_cache *cache)
{
	change_process_title("vine_worker [transfer]");

	int timeout = vine_worker_options_idle_timeout(options);
	link_set_idle_timeout(lnk, timeout);

	if (options->password) {
		if (!link_auth_password(lnk, options->password, time(0) + timeout)) {
			debug(D_VINE, "transfer server: could not authenticate peer worker via password!");
			return;
		}
	}

	vine_transfer_serve_request(lnk, cache, time(0) + timeout);
}

static void vine_transfer_process(struct vine_cache *cache, pid_t worker_pid)
{
	static int child_count = 0;

	/*
	1. Perform a non-blocking check for any child processes that have exited, this runs very fast.
	2. If the number of child processes has reached the maximum allowed, perform a blocking wait for a child process to exit.
	3. Once arrives here, the server is safe to accept a new connection, as the child_count is less than the maximum allowed.
	4. Upon accepting a connection, fork a new child process to handle it; if timeout, simply continue.
	5. lnk should be closed in the parent process to prevent file descriptor exhaustion.
	*/
	while (1) {
		/* A worker that is killed cannot stop the server, so the server exits when its worker is gone. */
		if (getppid() != worker_pid) {
			debug(D_VINE, "Transfer Server: the worker is gone, exiting");
			_exit(0);
		}

		/* Do a non-blocking wait for any exited children. */
		while (waitpid(-1, NULL, WNOHANG) > 0) {
			child_count--;
		}
		/* If the child count is larger than the maximum allowed, do blocking wait until it is safe to accept a new connection. */
		while (child_count >= VINE_TRANSFER_PROC_MAX_CHILD) {
			debug(D_VINE, "Transfer Server: waiting on exited child. Reached %d", child_count);
			if (waitpid(-1, NULL, 0) > 0) {
				child_count--;
			}
		}

		/* The server is safe to accept a new connection. */
		struct link *lnk = link_accept(transfer_link, time(0) + 10);

		if (lnk) {
			pid_t p = fork();
			if (p == 0) {
				vine_transfer_handler(lnk, cache);
				link_close(lnk);
				_exit(0);
			} else if (p > 0) {
				/* Increment the child count when a new child is successfully forked. */
				child_count++;
				/* Also close the link in the parent process, otherwise the opened file descriptors will not be closed.
				 * This caused a problem where incoming transfers were all failing due to the file descriptor limit per process being reached. */
				link_close(lnk);
			} else {
				/* If fork fails, also close the link. */
				link_close(lnk);
			}
		} else {
			/* If lnk is NULL, it means link_accept failed to accept a connection.
			 * This could be due to a timeout or other transient issues. */
			continue;
		}
	}
}

int vine_transfer_server_start(struct vine_cache *cache, struct link *manager, int port_min, int port_max)
{
	transfer_link = link_serve_range(port_min, port_max);

	if (!transfer_link) {
		return 0;
	}

	pid_t worker_pid = getpid();
	transfer_server_pid = fork();
	if (transfer_server_pid == 0) {
		/* Close only the inherited descriptor without flushing or shutting down the Worker TLS session. */
		close(link_fd(manager));
		change_process_title("vine_worker [transfer server]");
		vine_transfer_process(cache, worker_pid);
		_exit(0);
	} else if (transfer_server_pid > 0) {
		char addr[LINK_ADDRESS_MAX];
		int port;
		vine_transfer_server_address(addr, &port);
		debug(D_VINE, "started transfer server pid %d listening on %s:%d", transfer_server_pid, addr, port);
		// in parent, keep going
	} else {
		link_close(transfer_link);
		transfer_link = NULL;
		return 0;
	}
	return 1;
}

/*
A connect-back sender connects to the manager's data port within the idle timeout T, which allows for the kernel
retrying a dropped connection. The manager sends its get request once an executor takes the connection, which may wait
in its listener backlog while every request slot is busy, so the request may take another T. This matches the
manager, which gives a waiting receive 2T.
*/
int vine_transfer_server_connect_back(struct vine_cache *cache, struct link *manager, uint64_t token)
{
	char ip[LINK_ADDRESS_MAX];
	int port;
	if (!vine_cache_get_manager_transfer(cache, ip, &port)) {
		debug(D_VINE, "connect back: the manager has not sent its data port");
		return 0;
	}
	/* The intermediate process exits at once, so the worker reaps it here and init reaps the sender. */
	pid_t pid = fork();
	if (pid < 0) {
		debug(D_VINE, "connect back: could not fork: %s", strerror(errno));
		return 0;
	}
	if (pid == 0) {
		if (fork() == 0) {
			/* Close only the inherited descriptor without flushing or shutting down the Worker TLS session. */
			close(link_fd(manager));
			change_process_title("vine_worker [connect back]");
			int timeout = vine_worker_options_idle_timeout(options);
			struct link *lnk = link_connect(ip, port, time(0) + timeout);
			if (lnk) {
				/* Until the request arrives, the manager's backlog may add another T to its silence. */
				link_set_idle_timeout(lnk, 2 * timeout);
				if ((!options->password || link_auth_password(lnk, options->password, time(0) + timeout)) &&
						link_printf(lnk, LINK_FOREVER, "connect-back %" PRIu64 "\n", token) >= 0) {
					vine_transfer_serve_request(lnk, cache, time(0) + 2 * timeout);
				}
				link_close(lnk);
			} else {
				debug(D_VINE, "connect back: could not connect to %s:%d: %s", ip, port, strerror(errno));
			}
		}
		_exit(0);
	}
	waitpid(pid, NULL, 0);
	return 1;
}

int vine_transfer_server_running()
{
	return transfer_link != NULL;
}

void vine_transfer_server_stop()
{
	if (!transfer_link) {
		return;
	}
	int status;

	debug(D_VINE, "stopping transfer server pid %d", transfer_server_pid);

	link_close(transfer_link);
	transfer_link = NULL;
	if (transfer_server_pid > 0) {
		kill(transfer_server_pid, SIGKILL);
		waitpid(transfer_server_pid, &status, 0);
		transfer_server_pid = 0;
	}
}

void vine_transfer_server_address(char *addr, int *port)
{
	link_address_local(transfer_link, addr, port);
}
