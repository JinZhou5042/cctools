# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


# This file serves as the template for Python Library Task.
# A Python Library Task runs on a worker as a pilot task. Upcoming Python
# Function Calls will be executed by this pilot task.

# import relevant libraries.
import importlib
import json
import os
import fcntl
import sys
import argparse
import traceback
import cloudpickle
import select
import signal
import time
from datetime import datetime
import socket
from threadpoolctl import threadpool_limits
from ndcctools.taskvine.utils import load_variable_from_library

# self-pipe to turn a sigchld signal when a child finishes execution
# into an I/O event.
r, w = os.pipe()
exec_method = None

# Pipe on which a forked function process reports its exit status right before it exits, so the worker learns the
# result without waiting for the process to end. Processes that reported are in `reported` until they are reaped, and
# a process that ends without reporting is reported when it is reaped.
report_r, report_w = os.pipe()
reported = set()

# The standard output file of each forked function process, by pid, for a call that ends without reporting.
pid_to_stdout = {}

# Modules that calls imported and the library failed to import, and whether the library still imports them.
failed_imports = set()
learning_imports = True

# The spare process for the next function call in fork mode, as (pid, write end of its invocation pipe, number of
# modules loaded when it was forked), or None. A spare is kept once a call arrived in fork mode (`spare_wanted`).
# Spares that were discarded without running a call are in `discarded_spares` until they are reaped.
spare = None
spare_wanted = False
discarded_spares = set()


# This class captures how results from FunctionCalls are conveyed from
# the library to the manager.
# For now, all communication details should use this class to generate responses.
# In the future, this common protocol should be listed someplace else
# so library tasks from other languages can use.
class LibraryResponse:
    def __init__(self, result=None, success=None, reason=None):
        self.result = result
        self.success = success
        self.reason = reason

    def generate(self):
        return {
            "Result": self.result,
            "Success": self.success,
            "Reason": self.reason,
        }


# A wrapper around functions in library to extract arguments and formulate responses.
def remote_execute(func):
    def remote_wrapper(event):
        args = event.get("fn_args", [])
        kwargs = event.get("fn_kwargs", {})

        # in case of FutureFunctionCall tasks
        new_args = []
        for arg in args:
            if isinstance(arg, dict) and "VineFutureFile" in arg:
                with open(arg["VineFutureFile"], "rb") as f:
                    output = cloudpickle.load(f)["Result"]
                    new_args.append(output)
            else:
                new_args.append(arg)
        args = tuple(new_args)

        try:
            result = func(*args, **kwargs)
            success = True
            reason = None
        except Exception:
            result = None
            success = False
            reason = traceback.format_exc()
        return LibraryResponse(result, success, reason).generate()

    return remote_wrapper


# Handler to sigchld when child exits.
def sigchld_handler(signum, frame):
    # write any byte to signal that there's at least 1 child
    # if this fails, the notification mechanism has failed and the error will be raised normally
    os.write(w, b"a")


# Read data from worker, start function, and dump result to `outfile`.
def start_function(in_pipe_fd, thread_limit=1):
    global spare_wanted
    # read length of buffer to read
    buffer_len = b""
    while True:
        c = os.read(in_pipe_fd, 1)
        if c == b"":
            stdout_timed_message(f"can't get length from in_pipe_fd {in_pipe_fd}")
            exit(1)
        elif c == b"\n":
            break
        else:
            buffer_len += c
    buffer_len = int(buffer_len)
    # now read the buffer to get invocation details
    line = str(os.read(in_pipe_fd, buffer_len), encoding="utf-8")

    try:
        (
            function_id,
            function_name,
            function_sandbox,
            function_stdout_filename
        ) = line.split(" ", maxsplit=3)
    except Exception as e:
        stdout_timed_message(f"error: not enough values to unpack from {line} (expected 4 items), exception: {e}")
        exit(1)

    try:
        function_id = int(function_id)
    except Exception as e:
        stdout_timed_message(f"error: can't turn {function_id} into an integer, exception: {e}")
        exit(1)

    if not function_name:
        # malformed message from worker so we exit
        stdout_timed_message(f"error: invalid function name, malformed message {line} from worker")
        exit(1)

    if exec_method == "fork":
        # A call runs in the spare process, forked before it arrived, when there is one that can run it.
        spare_wanted = True
        p = start_in_spare(line)
        if p > 0:
            pid_to_stdout[p] = os.path.join(function_sandbox, function_stdout_filename)
            return p, function_id

    with threadpool_limits(limits=thread_limit):
        if exec_method == "direct":
            library_sandbox = os.getcwd()
            try:
                os.chdir(function_sandbox)

                # parameters are represented as infile.
                with open("infile", "rb") as f:
                    event = cloudpickle.load(f)

                # output of execution should be dumped to outfile.
                result = globals()[function_name](event)
                try:
                    with open("outfile", "wb") as f:
                        cloudpickle.dump(result, f)
                except Exception:
                    if os.path.exists("outfile"):
                        os.remove("outfile")
                    raise

                try:
                    if not result["Success"]:
                        raise Exception(result["Reason"])
                except Exception:
                    raise

            except Exception as e:
                stdout_timed_message(f"Library code: Function call failed due to {e}")
                sys.exit(1)
            finally:
                os.chdir(library_sandbox)
            return -1, function_id
        elif exec_method == "fork":
            try:
                arg_infile = os.path.join(function_sandbox, "infile")
                with open(arg_infile, "rb") as f:
                    event = cloudpickle.load(f)
            except Exception:
                stdout_timed_message(f"TASK {function_id} error: can't load the arguments from {arg_infile}")
                return -1, function_id
            p = os.fork()
            if p == 0:
                run_function(function_id, function_name, function_sandbox, function_stdout_filename, event)
            elif p < 0:
                stdout_timed_message(f"TASK {function_id} error: unable to fork to execute {function_name}")
                return -1, function_id

            # return pid and function id of child process to parent.
            else:
                pid_to_stdout[p] = os.path.join(function_sandbox, function_stdout_filename)
                return p, function_id
        else:
            stdout_timed_message(f"error: invalid execution method {exec_method}")
            return -1, function_id


# Run a function call in a forked process, which reports its exit status on the report pipe and exits. The arguments
# are loaded from the call's infile when event is None.
def run_function(function_id, function_name, function_sandbox, function_stdout_filename, event):
    exit_status = 1
    # sys.modules keeps the order of imports, so the modules this call imports follow the ones it inherited.
    inherited_modules = len(sys.modules)

    try:
        # only the library holds the invocation pipe of the spare, which then sees its end when the library exits.
        if spare:
            os.close(spare[1])

        # a call that runs in the spare loads its arguments here.
        if event is None:
            arg_infile = os.path.join(function_sandbox, "infile")
            try:
                with open(arg_infile, "rb") as f:
                    event = cloudpickle.load(f)
            except Exception:
                stdout_timed_message(f"TASK {function_id} error: can't load the arguments from {arg_infile}")
                raise

        # change the working directory to the function's sandbox
        os.chdir(function_sandbox)

        stdout_timed_message(f"TASK {function_id} {function_name} arrives, starting to run in process {os.getpid()}")

        try:
            # each child process independently redirects its own stdout/stderr.
            with open(function_stdout_filename, "wb") as f:
                os.dup2(f.fileno(), sys.stdout.fileno())  # redirect stdout
                os.dup2(f.fileno(), sys.stderr.fileno())  # redirect stderr

            # once the file descriptors are redirected, we can start the function execution
            stdout_timed_message(f"TASK {function_id} {function_name} starts in PID {os.getpid()}")
            result = globals()[function_name](event)
            stdout_timed_message(f"TASK {function_id} {function_name} finished")

        except Exception:
            stdout_timed_message(f"TASK {function_id} error: can't execute {function_name} due to {traceback.format_exc()}")
            exit_status = 2
            raise

        try:
            with open("outfile", "wb") as f:
                cloudpickle.dump(result, f)
        except Exception:
            stdout_timed_message(f"TASK {function_id} error: can't load the result from outfile")
            exit_status = 3
            if os.path.exists("outfile"):
                os.remove("outfile")
            raise

        try:
            if not result["Success"]:
                exit_status = 4
        except Exception:
            stdout_timed_message(f"TASK {function_id} error: the result is invalid")
            exit_status = 5
            raise

        # nothing failed
        stdout_timed_message(f"TASK {function_id} finished successfully")
        exit_status = 0
    except Exception as e:
        stdout_timed_message(f"TASK {function_id} error: execution failed due to {e}")
    finally:
        # Report the status that waitpid() would return before exiting, because tearing down a copy of the library
        # takes longer than most functions, and the modules the call imported, which the library then imports once
        # for later calls. The report is one write shorter than PIPE_BUF, so reports of different processes never
        # interleave.
        try:
            report = f"{os.getpid()} {exit_status << 8}"
            if exit_status == 0:
                report += imported_modules(inherited_modules, select.PIPE_BUF - len(report) - 2)
            os.write(report_w, f"{report}\n".encode())
        except Exception:
            pass
        os._exit(exit_status)


# Write, in a process that waits for a call, to the memory that running any call writes: the first write to a page
# that a forked process shares with the library makes the kernel copy that page, which costs a call more time than
# its work when it happens while the call runs.
def rehearse_call():
    try:
        datetime.now().strftime("%m/%d/%y %H:%M:%S.%f")
        with open(os.devnull, "wb") as f:
            cloudpickle.dump(LibraryResponse(None, True, None).generate(), f)
    except Exception:
        pass


# Fork the spare process, which waits for the invocation of the next call and runs it, so that call does not wait for
# the fork. The spare reads the invocation until the library closes the pipe, and exits without running anything when
# the pipe ends empty because the library discarded it or exited.
def fork_spare(thread_limit):
    global spare
    read_fd, write_fd = os.pipe()
    p = os.fork()
    if p == 0:
        os.close(write_fd)
        exit_status = 1
        try:
            threadpool_limits(limits=thread_limit)
            # a call that is already waiting runs at once.
            if not select.select([read_fd], [], [], 0)[0]:
                rehearse_call()
            chunks = []
            while True:
                chunk = os.read(read_fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            if not chunks:
                exit_status = 0
            else:
                function_id, function_name, function_sandbox, function_stdout_filename = b"".join(chunks).decode().split(" ", maxsplit=3)
                run_function(int(function_id), function_name, function_sandbox, function_stdout_filename, None)
        finally:
            os._exit(exit_status)
    os.close(read_fd)
    spare = (p, write_fd, len(sys.modules))


# Hand an invocation to the spare process and return its pid, or -1 without a spare that can run it. A spare forked
# before the library imported more modules lacks them, so it is discarded.
def start_in_spare(line):
    global spare
    if not spare:
        return -1
    pid, write_fd, module_count = spare
    spare = None
    started = False
    try:
        if module_count == len(sys.modules):
            data = line.encode()
            while data:
                data = data[os.write(write_fd, data):]
            started = True
    except OSError:
        pass
    os.close(write_fd)
    if not started:
        discarded_spares.add(pid)
        return -1
    return pid


# Return " name,name,..." for the modules imported since sys.modules held count modules, at most limit characters,
# or "" when there are none or when the call left threads running, which may have imported them for themselves and
# would make an import in the library start threads that every later fork inherits half-copied.
def imported_modules(count, limit):
    threading_module = sys.modules.get("threading")
    if threading_module and threading_module.active_count() > 1:
        return ""
    names = []
    length = 0
    for name in list(sys.modules)[count:]:
        if name.startswith("__") or "," in name or " " in name:
            continue
        length += len(name) + 1
        if length + 1 > limit:
            break
        names.append(name)
    return " " + ",".join(names) if names else ""


# Import, in the library process, modules that a call imported, so the calls forked afterwards inherit them instead of
# importing them each. An import that fails is not tried again. Importing stops for good once an import leaves a
# thread running, which forked calls would inherit without its state being safe to use.
def learn_imports(names):
    global learning_imports
    if not learning_imports:
        return
    for name in names:
        if name in sys.modules or name in failed_imports:
            continue
        try:
            importlib.import_module(name)
        except BaseException:
            failed_imports.add(name)
    threading_module = sys.modules.get("threading")
    if threading_module and threading_module.active_count() > 1:
        learning_imports = False
        stdout_timed_message("an imported module started a thread, so the library imports no more modules of calls")


# Send result of a function execution to worker. Wake worker up to do work with SIGCHLD.
def send_result(out_pipe_fd, worker_pid, task_id, exit_code):
    buff = bytes(f"{task_id} {exit_code}", "utf-8")
    buff = bytes(str(len(buff)), "utf-8") + b"\n" + buff
    os.writev(out_pipe_fd, [buff])
    os.kill(worker_pid, signal.SIGCHLD)


# Self-identifying message to send back to the worker, including the name of this library.
# Send back a SIGCHLD to interrupt worker sleep and get it to work.
def send_configuration(config, out_pipe_fd, worker_pid):
    config_string = json.dumps(config)
    config_cmd = f"{len(config_string)}\n{config_string}"
    os.writev(out_pipe_fd, [bytes(config_cmd, "utf-8")])
    os.kill(worker_pid, signal.SIGCHLD)


# Use os.write to stdout instead of print for multi-processing safety
def stdout_timed_message(message):
    timestamp = datetime.now().strftime("%m/%d/%y %H:%M:%S.%f")
    os.write(sys.stdout.fileno(), f"{timestamp} {message}\n".encode())


def main():
    ppid = os.getppid()

    parser = argparse.ArgumentParser(
        "Parse input and output file descriptors this process should use. The relevant fds should already be prepared by the vine_worker."
    )
    parser.add_argument(
        "--in-pipe-fd",
        required=True,
        type=int,
        help="input fd to receive messages from the vine_worker via a pipe",
    )
    parser.add_argument(
        "--out-pipe-fd",
        required=True,
        type=int,
        help="output fd to send messages to the vine_worker via a pipe",
    )
    parser.add_argument(
        "--task-id",
        required=False,
        type=int,
        default=-1,
        help="task id for this library.",
    )
    parser.add_argument(
        "--library-cores",
        required=False,
        type=int,
        default=1,
        help="number of cores of this library",
    )
    parser.add_argument(
        "--function-slots",
        required=False,
        type=int,
        default=1,
        help="number of function slots of this library",
    )
    parser.add_argument(
        "--worker-pid",
        required=True,
        type=int,
        help="pid of main vine worker to send sigchild to let it know theres some result.",
    )
    args = parser.parse_args()

    # check if library cores and function slots are valid
    if args.function_slots > args.library_cores:
        stdout_timed_message("error: function slots cannot be more than library cores")
        exit(1)
    elif args.function_slots < 1:
        stdout_timed_message("error: function slots cannot be less than 1")
        exit(1)
    elif args.library_cores < 1:
        stdout_timed_message("error: library cores cannot be less than 1")
        exit(1)

    try:
        thread_limit = args.library_cores // args.function_slots
    except Exception as e:
        stdout_timed_message(f"error: {e}")
        exit(1)

    # check if the in_pipe_fd and out_pipe_fd are valid
    try:
        fcntl.fcntl(args.in_pipe_fd, fcntl.F_GETFD)
        fcntl.fcntl(args.out_pipe_fd, fcntl.F_GETFD)
    except IOError as e:
        stdout_timed_message(f"error: pipe fd closed\n{e}")
        exit(1)

    stdout_timed_message(f"library task starts running in process {os.getpid()}")
    stdout_timed_message(f"hostname             {socket.gethostname()}")
    stdout_timed_message(f"task id              {args.task_id}")
    stdout_timed_message(f"worker pid           {args.worker_pid}")
    stdout_timed_message(f"library pid          {os.getpid()}")
    stdout_timed_message(f"input fd             {args.in_pipe_fd}")
    stdout_timed_message(f"output fd            {args.out_pipe_fd}")
    stdout_timed_message(f"library cores        {args.library_cores}")
    stdout_timed_message(f"function slots       {args.function_slots}")
    stdout_timed_message(f"thread limit         {thread_limit}")

    # Open communication pipes to vine_worker.
    # The file descriptors are inherited from the vine_worker parent process
    # and should already be open for reads and writes.
    in_pipe_fd = args.in_pipe_fd
    out_pipe_fd = args.out_pipe_fd

    # mapping of child pid to function id of currently running functions
    pid_to_func_id = {}
    # the start of a report that the last read of the report pipe ended in
    report_buffer = b""

    # Read the reports that wait in the report pipe and send the result of each to the worker.
    def read_reports():
        nonlocal report_buffer
        report_buffer += os.read(report_r, 65536)
        *reports, report_buffer = report_buffer.split(b"\n")
        imported = []
        for report in reports:
            fields = report.decode().split()
            c_pid, c_exit_status = int(fields[0]), int(fields[1])
            if c_pid in pid_to_func_id and c_pid not in reported:
                send_result(
                    out_pipe_fd,
                    args.worker_pid,
                    pid_to_func_id[c_pid],
                    c_exit_status,
                )
                reported.add(c_pid)
            if len(fields) > 2:
                imported.extend(fields[2].split(","))
        # results go out first.
        if imported:
            learn_imports(imported)

    # read in information about this library
    with open('library_info.clpk', 'rb') as f:
        library_info = cloudpickle.load(f)

    # load and execute this library's context
    library_context_info = cloudpickle.loads(library_info['context_info'])
    context_vars = None
    if library_context_info:
        context_func = library_context_info[0]
        context_args = library_context_info[1]
        context_kwargs = library_context_info[2]
        context_vars = context_func(*context_args, **context_kwargs)

    # register functions in this library to the global namespace
    for func_name in library_info['function_list']:
        func_code = remote_execute(cloudpickle.loads(library_info['function_list'][func_name]))
        globals()[func_name] = func_code

    # update library's context to the load function
    if context_vars:
        (load_variable_from_library.__globals__).update(context_vars)

    # set execution mode of functions in this library
    global exec_method, spare, spare_wanted
    exec_method = library_info['exec_mode']

    # send configuration of library, just its name for now
    config = {
        "name": library_info['library_name'],
        "taskid": args.task_id,
        "exec_mode": exec_method,
    }
    send_configuration(config, out_pipe_fd, args.worker_pid)

    # register sigchld handler to turn a sigchld signal into an I/O event
    signal.signal(signal.SIGCHLD, sigchld_handler)

    # 5 seconds to wait for select, any value long enough would probably do
    timeout = 5

    last_check_time = time.time() - 5

    while True:
        # check if parent exits
        c_ppid = os.getppid()
        if c_ppid != ppid or c_ppid == 1:
            stdout_timed_message("library finished because parent exited")
            exit(0)

        # periodically log the number of concurrent functions
        current_check_time = time.time()
        if current_check_time - last_check_time >= 5:
            stdout_timed_message(f"{len(pid_to_func_id)} functions running concurrently")
            last_check_time = current_check_time

        # in case of "fork" exec method, wait for messages from worker or child to return
        # in case of "direct" exec method, wait for messages from worker
        try:
            rlist, wlist, xlist = select.select([in_pipe_fd, report_r, r], [], [], timeout)
        except Exception as e:
            stdout_timed_message(f"error unable to read from pipe {in_pipe_fd}\n{e}")

        for re in rlist:
            # worker has a function, run it
            if re == in_pipe_fd:
                if exec_method == 'direct':
                    _, func_id = start_function(in_pipe_fd, thread_limit)
                    send_result(
                        out_pipe_fd,
                        args.worker_pid,
                        func_id,
                        0,
                    )
                else:
                    pid, func_id = start_function(in_pipe_fd, thread_limit)
                    # pid == -1 indicates a failure during fork/setup of the function execution
                    if pid == -1:
                        # The status that waitpid() returns for a process that exited with status 1, as the worker
                        # receives for every other call. A plain 1 would read as a process killed by signal 1.
                        send_result(
                            out_pipe_fd,
                            args.worker_pid,
                            func_id,
                            1 << 8,
                        )
                    else:
                        pid_to_func_id[pid] = func_id
            elif re == report_r:
                # function processes report their status before they exit, so a report is read before its process
                # is reaped. A read may end inside a report, whose rest the next read returns.
                read_reports()
            else:
                # at least 1 child exits, reap all.
                # read only once as os.read is blocking if there's nothing to read.
                # note that there might still be bytes in `r` but it's ok as they will
                # be discarded in the next iterations.
                os.read(r, 1)
                while len(pid_to_func_id) > 0 or len(discarded_spares) > 0 or spare:
                    try:
                        c_pid, c_exit_status = os.waitpid(-1, os.WNOHANG)
                    except ChildProcessError:
                        break
                    if c_pid in pid_to_func_id:
                        # A process writes its report before it exits, so a report not read yet is in the pipe now.
                        if c_pid not in reported and c_exit_status == 0:
                            while c_pid not in reported and select.select([report_r], [], [], 0)[0]:
                                read_reports()
                        # a process that reported its status was reported to the worker already.
                        if c_pid in reported:
                            reported.discard(c_pid)
                        else:
                            # A process that exited with status 0 without reporting ended inside its function, such
                            # as with os._exit(0), and wrote no result. It failed like a call that could not start,
                            # instead of looking like a success whose result went missing.
                            if c_exit_status == 0:
                                c_exit_status = 1 << 8
                                try:
                                    with open(pid_to_stdout[c_pid], "a") as f:
                                        f.write("error: the function ended its process with status 0 instead of returning\n")
                                except OSError:
                                    pass
                            send_result(
                                out_pipe_fd,
                                args.worker_pid,
                                pid_to_func_id[c_pid],
                                c_exit_status,
                            )
                        del pid_to_func_id[c_pid]
                        pid_to_stdout.pop(c_pid, None)
                    elif c_pid > 0 and spare and c_pid == spare[0]:
                        # the spare ended before it received a call, so the next call forks as usual, and only a
                        # call in fork mode asks for a spare again.
                        os.close(spare[1])
                        spare = None
                        spare_wanted = False
                    elif c_pid > 0:
                        discarded_spares.discard(c_pid)
                    # no exited child to reap, break
                    else:
                        break

        # Keep a spare for the next call, forked after the calls that just arrived were started. While every function
        # slot runs a call, no call can arrive, so the spare is forked when a call reports and its fork does not take
        # a core from running calls.
        calls_running = len(pid_to_func_id) - len(reported)
        if spare_wanted and not spare and calls_running < args.function_slots:
            fork_spare(thread_limit)
    return 0


if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
