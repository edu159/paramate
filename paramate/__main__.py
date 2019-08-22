#!/usr/bin/env python2
#NOTE; Install pip install cryptography=2.4.2 to remove warnings
import time
import paramiko
import os
import re
import sys
import remote
import getpass
from common import _printer, ProgressBar
from study import Study, StudyGenerator
from postprocessing import create_results_table
from files import RemotesFile
from contextlib import contextmanager

import colorama as color
# from __future__ import print_function

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")

@contextmanager
def action_error_handler(debug):
    try:
        yield 
    except Exception as error:
        if debug:
            raise
        else:
            _printer.indent_level = 0
            _printer.print_msg(str(error), "error")
            _printer.print_msg("Aborting...", "error")
            sys.exit(1)

def decode_case_selector(selector, nof_cases):
    cases_idx = []
    if selector is None:
        return None
    elif selector == '*':
        cases_idx = list(xrange(0, nof_cases))
    else:
        match_group = re.match("(\d+):(\d+)(?::(\d+))?$", selector)
        if match_group is not None:
            selector_split = selector.split(':')
            rmin, rmax = int(selector_split[0]), int(selector_split[1])
            if len(selector_split) == 3:
                step = int(selector_split[2])
            else:
                step = 1
            try:
                cases_idx = list(xrange(rmin, rmax+1, step))
            except Exception:
                raise Exception("Case selector malformed.")
        match_group = re.match("\d+(?:,(?:\d+))*$", selector)
        if match_group is not None:
            cases_idx = [int(c) for c in selector.split(',')]
    if not cases_idx:
        raise Exception("Case selector malformed.")
    for i in cases_idx:
        if i >= nof_cases:
            raise Exception("Index '%d' generated out of range. The number of cases is '%d'." % (i, nof_cases))
    return cases_idx


def connect(remote, debug=False, progress_bar=None):
    attempts = 0
    pass_required = False
    prompt_str = ""
    passwd = None
    if remote.auth_type == "password":
        prompt_str = "Password ({}):".format(remote.name)
        pass_required = True
    elif remote.passphrase_required():
        prompt_str = "Key passphrase ({}):".format(remote.name)
        pass_required = True
    while attempts < 3 or attempts == 0:
        try:
            if pass_required:
                _printer.print_msg(prompt_str, "input", end='')
                passwd = getpass.getpass("")
            if progress_bar is not None:
                remote.connect(passwd, progress_callback=progress_bar.callback)
            else:
                remote.connect(passwd)
        except paramiko.ssh_exception.AuthenticationException as error:
            attempts += 1
            if pass_required:
                if attempts < 3:
                    _printer.print_msg("Authentication failed", "warning")
                    continue
                else:
                    raise Exception(str(error).rstrip('.') + " (3 attempts)")
            raise
        break

def get_remote(study_path, remote_name_in):
    # _printer.print_msg("Testing connection to remote '%s'..." % r.name, end='')
    # r.available()
    # print "OK"

    remotes = RemotesFile(study_path)
    remotes.load()
    r = remote.Remote()
    # Set to "default" if args.remote is None
    if remote_name_in == None:
        remote_name = remotes.default_remote
        _printer.print_msg("Using default remote '{}'...".format(remote_name))
    else:
        remote_name = remote_name_in
    try:
        remote_yaml = remotes[remote_name]
    except KeyError:
        raise Exception("Remote '{}' not found in 'remotes.yaml'.".format(remote_name))
    r.configure(remote_name, remote_yaml)
    return r


# Actions for maim program
def create_action(args):
    study_name = os.path.basename('.')
    study_path = os.path.dirname(os.path.abspath(args.create))
    with action_error_handler(args.debug):
        StudyGenerator.create_study(study_path, study_name)
        print "Created study '%s.'" % study_name

# TODO: Improve with info about size of lists and total number of params
def print_tree_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    with action_error_handler(args.debug):
        study = Study(study_name, study_path)
        _printer.print_msg("Printing param tree...", "info")
        _printer.indent_level = 1
        study.param_file.print_tree()

    _printer.indent_level = 0
    _printer.print_msg("Done.", "info")

def postproc_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    with action_error_handler(args.debug):
        study = Study(study_name, study_path)
        study.load()
        try:
            # Insert study path to load postproc functions
            sys.path.insert(0, study_path)
            import postproc
        except Exception as err:
            raise
            raise Exception("File 'postproc.py' not found in study directory.")
        create_results_table(postproc.POSTPROC_TABLE_FIELDS, study)

    _printer.indent_level = 0
    _printer.print_msg("Done.", "info")


def generate_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    with action_error_handler(args.debug):
        study = Study(study_name, study_path)
        sb  =  StudyGenerator(study, short_name=args.shortname,
                              build_once=args.build_once,
                              keep_onerror=args.keep_on_error,
                              abort_undefined=args.abort_undefined)
        r = get_remote(study_path, args.local_remote)
        sb.generate_cases(r)
    _printer.print_msg("Done.", "info")

def delete_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    with action_error_handler(args.debug):
        study = Study(study_name, study_path, load_param_file=False)
        study.load()
        _printer.print_msg("Selected %d cases to delete..." % study.nof_cases, "info")
        if args.yes:
            opt = 'y'
        else:
            _printer.print_msg("Are you sure to delete?[Y,y]: ", "input", end="")
            opt = raw_input("")
        if opt in ['y', 'Y']:
            _printer.print_msg("Deleting study files...", "info")
            study.delete()
        else:
            _printer.print_msg("Delete aborted.", "info")
    _printer.print_msg("Done.", "info")

def remote_init_action(args):
    remote_name = args.remote
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    with action_error_handler(args.debug):
        r = get_remote(study_path, remote_name)
        connect(r, debug=args.debug)
        r.init_remote()
    _printer.print_msg("Done.", "info")


def clean_action(args):
    study_path = os.path.abspath('.')
    with action_error_handler(args.debug):
        r = get_remote(study_path, args.remote)
        connect(r, debug=args.debug)
    _printer.print_msg("Done.", "info")


    # study_path = os.path.abspath('.')
    # study_name = os.path.basename(study_path)
    # try:
    #     study = Study(study_name, study_path, load_param_file=False)
    #     study.load()
    #     if args.c is None:
    #         case_selector = "*"
    #     else:
    #         case_selector = args.c
    #     cases_idx = decode_case_selector(case_selector, study.nof_cases)
    #     study.set_selection(cases_idx)
    #     print "Cleaning %d cases..." % len(cases_idx)
    #     study.clean()
    #     print "Done."
    # except Exception as error:
    #     if args.debug:
    #         raise
    #     else:
    #         sys.exit("Error: " + str(error))

def get_cases_byremote(cases_idx, study, allowed_states, remote=None):
    cases_remote = study.get_cases(cases_idx, "id", sortby="remote")
    no_remote_cases = cases_remote.pop(None, None)
    if no_remote_cases is None:
        no_remote_cases = []
    remote_info = {}
    state_list = ["CREATED", "UPLOADED", "SUBMITTED", "FINISHED", "DELETED", "DOWNLOADED"]
    remotes = []
    if remote is None:
        remotes = cases_remote.keys()
    else:
        # Only check that remote actually exists
        get_remote(study.path, remote)
        # This will happen in "upload" when a remote specified probably not have cases already uploaded
        if remote in cases_remote.keys():
            remotes = [remote]
        # if "CREATED" in allowed_states:
        #     if no_remote_cases is not None:
        #         try:
        #             cases_remote[remote].extend(no_remote_cases)
        #         except KeyError:
        #             cases_remote[remote] = no_remote_cases
        #         remotes = [remote]
        # else:
        #     remotes = [remote]

    for r in remotes:
        remote_info[r] =  {"nof_selected": len(cases_remote[r]), "nof_valid": 0, "cases": {}, "valid_cases": []}
        for state in state_list:
            cases = list(set(study.get_cases([state], "status")).intersection(set(cases_remote[r])))
            remote_info[r]["cases"][state] = {"nof": len(cases), "list": cases}
            if state in allowed_states:
                remote_info[r]["valid_cases"].extend(cases)

        remote_info[r]["nof_valid"] = len(remote_info[r]["valid_cases"])

    return remote_info, no_remote_cases

def job_status_action(args):
    action = "job-status"
    allowed_states = ["SUBMITTED"]
    def action_func_job_status(study_manager, remote):
        return study_manager.job_status(remote)

    def output_handler_job_status(output):
        _printer.indent_level = 2
        _printer.print_msg("")
        for l in output:
            _printer.print_msg(l, end='')
    state_action(args, action, allowed_states, action_func_job_status, output_handler_job_status)

def job_submit_action(args):
    action = "job-submit"
    allowed_states = ["UPLOADED"]
    def action_func_job_submit(study_manager, remote):
        return study_manager.job_submit(remote, array_job=args.array_job)

    def output_handler_job_submit(output):
        pass
    state_action(args, action, allowed_states, action_func_job_submit, output_handler_job_submit)

def job_delete_action(args):
    action = "job-delete"
    allowed_states = ["SUBMITTED"]
    def action_func_job_delete(study_manager, remote):
        return study_manager.job_delete(remote)
    def output_handler_job_delete(output):
        _printer.print_msg("Marked for deletion {} cases. Waiting...".format(output))
    state_action(args, action, allowed_states, action_func_job_delete, output_handler_job_delete)

def upload_action(args):
    action = "upload"
    allowed_states = ["CREATED"]
    def action_func_upload(study_manager, remote):
        return study_manager.upload(remote, array_job=args.array_job, force=args.force)

    def output_handler_upload(output):
       pass 
    progress_bar_upload = ProgressBar("Uploading: ") 
    state_action(args, action, allowed_states, action_func_upload,
                 output_handler_upload, progress_bar_upload)

def download_action(args):
    action = "download"
    allowed_states = ["SUBMITTED", "FINISHED"]
    def action_func_download(study_manager, remote):
        return study_manager.download(remote, force=args.force)
    def output_handler_download(output):
       pass 
    progress_bar_download = ProgressBar("Downloading: ")
    state_action(args, action, allowed_states, action_func_download,
                output_handler_download, progress_bar_download)

def state_action(args, action, allowed_states, action_func, output_handler, action_progress_bar=None):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    study = Study(study_name, study_path)
    study.load()
    if args.selector is None:
        case_selector = "*"
    else:
        case_selector = args.selector
    with action_error_handler(args.debug):
        cases_idx = decode_case_selector(case_selector, study.nof_cases)
        study.set_selection(cases_idx)
        remote_cases, no_remote_cases  = get_cases_byremote(cases_idx, study, allowed_states, remote=args.remote)
    nof_remotes = len(remote_cases.keys())
    nof_selected_cases = len(cases_idx)
    nof_no_remote_cases = len(no_remote_cases)
    nof_remote_cases = nof_selected_cases - nof_no_remote_cases

    # Print info about the state of selected cases
    _printer.print_msg("Selected {} cases for action '{}':".format(nof_selected_cases, action), "info")
    _printer.indent_level = 1
    if nof_no_remote_cases:
        _printer.print_msg("- {} cases with no associated remote...".format(nof_no_remote_cases), "info")
    if nof_remote_cases > 0:
        _printer.print_msg("- {} cases in ({} remotes)':".format(len(cases_idx)-len(no_remote_cases), nof_remotes), "info")
        for counter, (remote_name, remote_info) in enumerate(remote_cases.items()):
            _printer.indent_level = 2
            _printer.print_msg("[{}] '{}': {} cases selected".format(counter+1, remote_name, remote_info["nof_selected"]), "info")
            for status, case_info  in remote_info["cases"].items():
                if case_info["nof"] > 0:
                    _printer.indent_level = 3
                    _printer.print_msg("{}: {}".format(status, case_info["nof"]))
            nof_excluded_cases = remote_info["nof_selected"] - remote_info["nof_valid"]
            if nof_excluded_cases > 0:
                _printer.print_msg("Found {} cases with a state not in {}. They will be ignored.".format(nof_excluded_cases, allowed_states))

    # The cases which has no remote are the ones to upload
    if action == "upload":
        with action_error_handler(args.debug):
            _printer.indent_level = 0
            r = get_remote(study_path, args.remote)
            remote_cases = {}
            remote_cases[r.name] = {"nof_valid": len(no_remote_cases), "valid_cases": no_remote_cases}
 
    # Iterate over remotes
    for remote_name, remote_info in remote_cases.items():
        # If not valid cases to perform action go to next remote
        if remote_info["nof_valid"] == 0:
            continue
        # Continue to the next remote if there are not cases to download
        study.set_selection(remote_info["valid_cases"])
        _printer.print_msg("", "blank")
        remote_header = "[{}: '{}']".format(action.capitalize(), remote_name)
        _printer.indent_level = 0
        _printer.print_msg(remote_header, "info")
        _printer.indent_level = 1

        # Ask for confirmation
        if args.yes:
            opt = 'y'
        else:
            _printer.print_msg("Perform action '{}' on '{}'?[Y,y]: ".format(action, remote_name), "input", end="")
            opt = raw_input("")
        if not opt in ['y', 'Y']:
            _printer.print_msg("Skipping...")
            _printer.print_msg("", "blank")
            continue
        with action_error_handler(args.debug):
            r = get_remote(study_path, remote_name)
            connect(r, debug=args.debug, progress_bar=action_progress_bar)
            sm = remote.StudyManager(study)
            # The update affect all cases not only the selection
            sm.update_status(r)
            # Upload is not affected by update_status()
            if action == "upload":
                valid_cases = remote_cases[remote_name]["valid_cases"]
            else:
                remote_cases_updated, no_remote_cases = get_cases_byremote(cases_idx, study, allowed_states, remote=remote_name)
                valid_cases = remote_cases_updated[remote_name]["valid_cases"]
            output = ""
            if valid_cases:
                _printer.print_msg("Performing action '{}' on {} cases...".format(action, len(valid_cases)), "info")
                study.set_selection(valid_cases)
                output = action_func(sm, r)
                output_handler(output)
            else:
                _printer.print_msg("No jobs running found. Skipping...")
        r.close()
    _printer.print_msg("", "blank")
    _printer.indent_level = 0
    if sum([r["nof_valid"] for r in remote_cases.values()]) == 0:
            _printer.print_msg("Nothing to do for action: '{}'".format(action), "info")
    _printer.print_msg("Done.", "info")
 

def main(args=None):
    """The main routine."""
    # if args is None:
    #     args = sys.argv
    import argparse
    color.init()
    parser = argparse.ArgumentParser(description="Program to generate parameter studies.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False, help="Verbose mode.")
    group.add_argument("-q", "--quiet", action="store_true", default=False, help="Quite mode.")
    parser.add_argument("--debug", action="store_true", default=False, help="Debug mode.")
    # actions_group = parser.add_mutually_exclusive_group()
    subparsers = parser.add_subparsers(help='sub-command help')

    # Subparsers 

    # Parser create
    parser_create = subparsers.add_parser('create', help="Create an empty study template.")
    parser_create.set_defaults(func=create_action)
    parser_create.add_argument('-n', required=True, type=str, help="Study name.")

    # Parser generate 
    parser_generate = subparsers.add_parser('generate', help="Generate the instances of the study based on the 'params.yaml' file.")
    parser_generate.set_defaults(func=generate_action)
    parser_generate.add_argument("--shortname", action="store_true", default=False, help="Study instances are short named.")
    parser_generate.add_argument("--keep-on-error", action="store_true", default=False, help="Keep files in case of an error during generation.")
    parser_generate.add_argument("--build-once", action="store_true", default=False, help="Execute only once the build script.")
    parser_generate.add_argument("--abort-undefined", action="store_false", default=True, help="Abort execution if an undefined parameter is found.")
    parser_generate.add_argument('--local-remote', type=str, help="Local remote name.")

    # Parser print-tree
    parser_print_tree = subparsers.add_parser('print-tree', help="Print parameter tree.")
    parser_print_tree.set_defaults(func=print_tree_action)

    # Parser postproc
    parser_postproc = subparsers.add_parser('postproc', help="Postprocess study. Generates a table.")
    parser_postproc.set_defaults(func=postproc_action)


    # Parser delete 
    parser_delete = subparsers.add_parser('delete', help="Delete all instances in a study.")
    parser_delete.set_defaults(func=delete_action)
    parser_delete.add_argument('-y', '--yes', action="store_true", help="Yes to all.")

    # Parser clean
    parser_clean = subparsers.add_parser('clean', help="Clean all instances in a study.")
    parser_clean.set_defaults(func=clean_action)
    parser_clean.add_argument('-c', type=str, help="Case selector.")
    parser_clean.add_argument('-r', '--remote', type=str, help="Remote name.")

    # Parser remote-init
    parser_remote_init = subparsers.add_parser('remote-init', help="Get a resumed info of the study.")
    parser_remote_init.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_remote_init.set_defaults(func=remote_init_action)
    
    # Parser upload
    parser_upload = subparsers.add_parser('upload', help="Upload study to remote.")
    parser_upload.set_defaults(func=upload_action)
    parser_upload.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_upload.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_upload.add_argument('-f', '--force', action="store_true", help="Force upload. Overwrite files.")
    parser_upload.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_upload.add_argument("--array-job", action="store_true", default=False, help="Upload to run as a array of jobs.")

    # Parser download 
    parser_download = subparsers.add_parser('download', help="download study to remote.")
    parser_download.set_defaults(func=download_action)
    parser_download.add_argument('-f', '--force', action="store_true", help="Force download. Overwrite files.")
    parser_download.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_download_mexgroup = parser_download.add_mutually_exclusive_group()
    parser_download_mexgroup.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_download_mexgroup.add_argument('-r', '--remote', type=str, help="Remote name.")

    # Parser job-submit
    parser_job_submit = subparsers.add_parser('job-submit', help="Submit study remotely/localy.")
    parser_job_submit.set_defaults(func=job_submit_action)
    parser_job_submit.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_job_submit.add_argument("--array-job", action="store_true", default=False, help="Submit study as a array of jobs.")
    parser_job_submit_mexgroup = parser_job_submit.add_mutually_exclusive_group()
    parser_job_submit_mexgroup.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_submit_mexgroup.add_argument('-r', '--remote', type=str, help="Remote name.")

    # Parser job-status 
    parser_job_status = subparsers.add_parser('job-status', help="Query job status.")
    parser_job_status.set_defaults(func=job_status_action)
    parser_job_status.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_job_status_mexgroup = parser_job_status.add_mutually_exclusive_group()
    parser_job_status_mexgroup.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_status_mexgroup.add_argument('-r', '--remote', type=str, help="Remote name.")

    # Parser job-delete
    parser_job_delete = subparsers.add_parser('job-delete', help="Delete jobs associated with cases.")
    parser_job_delete.set_defaults(func=job_delete_action)
    parser_job_delete.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_job_delete_mexgroup = parser_job_delete.add_mutually_exclusive_group()
    parser_job_delete_mexgroup.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_delete_mexgroup.add_argument('-r', '--remote', type=str, help="Remote name.")
 
    # actions_group.add_argument("--check", action="store_true", help="Check if the study is consistent with 'params.yaml' file.")
    # actions_group.add_argument("--info", action="store_true", help="Get a resumed info of the study.")

    # Actions modifiers
    # parser.add_argument("--cases", default='*', metavar="case_indices", nargs="?", const="*", help="Case selector.")
    # parser.add_argument("--array-job", action="store_true", default=False, help="Submit the study as a array of jobs.")
    # parser.add_argument("--remote", nargs="?", const=None, metavar="remote_name", help="Specify remote for an action.")
    # parser.add_argument("--force", action="store_true", default=False, help="Specify remote for an action.")
    args = parser.parse_args()
    _printer.configure(args.verbose, args.quiet)
    args.func(args)

if __name__ == "__main__":
    main()
