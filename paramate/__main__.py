#!/usr/bin/env python2
import time
import os
import re
import sys
import remote
import getpass
from common import _printer, ProgressBar
from study import Study, StudyGenerator
from files import RemotesFile

import colorama as color
# from __future__ import print_function

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")

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


# Look for a remote. First look in the ConfigDir. If not present 
def opts_get_remote(abs_remote_path, remote_name):
    r = remote.Remote()
    remote_yaml_path = os.path.join(abs_remote_path, "remote.yaml")

    try:
        r.load(remote_yaml_path, remote_name=remote_name)
    except IOError as error:
        _printer.print_msg("File 'remote.yaml' not found in study directory.", "error")
        _printer.print_msg("Aborting...", "error")
        sys.exit() 

    try:
        _printer.print_msg("Testing connection to remote '%s'..." % r.name, end='')
        r.available()
        print "OK"
    except Exception as error: 
        print "Failed."
        _printer.print_msg(str(error), "error")
        _printer.print_msg("Aborting...", "error")
        sys.exit()
    return r


def connect(remote, debug=False, show_progress_bar=False):
    attempts = 0
    pass_required = False
    prompt_str = ""
    passwd = None
    print remote.name
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
            if show_progress_bar:
                progress_bar = ProgressBar()
                remote.connect(passwd, progress_callback=progress_bar.callback)
            else:
                remote.connect(passwd)
        except Exception as error:
            attempts += 1
            if attempts < 3 and pass_required:
                _printer.print_msg("Authentication failed", "warning")
                continue
            if debug:
                raise
            else:
                _printer.print_msg(str(error).rstrip('.') + " (3 attempts)", "error")
                _printer.print_msg("Aborting...", "error")
                sys.exit()
        break


# Actions for maim program
def create_action(args):
    study_name = os.path.basename('.')
    study_path = os.path.dirname(os.path.abspath(args.create))
    try:
        StudyGenerator.create_study(study_path, study_name)
        print "Created study '%s.'" % study_name
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg(str(error), "error")
            _printer.print_msg("Aborting...", "error")
            sys.exit()

def print_tree_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    try:
        study = Study(study_name, study_path)
        _printer.print_msg("Printing param tree...", "info")
        study.param_file.print_tree()
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg(str(error), "error")
            _printer.print_msg("Aborting...", "error")
            sys.exit()

def generate_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    try:
        study = Study(study_name, study_path)
        sb  =  StudyGenerator(study, short_name=args.shortname,
                              build_once=args.build_once,
                              keep_onerror=args.keep_on_error,
                              abort_undefined=args.abort_undefined)
        sb.generate_cases()
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg(str(error), "error")
            _printer.print_msg("Aborting...", "error")
            sys.exit()

def delete_action(args):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    try:
        study = Study(study_name, study_path, load_param_file=False)
        try:
            study.load()
        except Exception:
            raise Exception("File 'cases.info' not found. Nothing to delete.")
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
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg(str(error), "error")
            sys.exit()
    _printer.print_msg("Done.", "info")

def remote_init_action(args):
    remote_name = args.remote
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    r = opts_get_remote(study_path, remote_name)
    connect(r, debug=args.debug)
    try:
        r.init_remote()
    except Exception as error:
        if args.debug:
            raise
        else:
            r.close()
            _printer.print_msg("Error: " + str(error), "error")
            sys.exit()
    r.close()

def clean_action(args):
    study_path = os.path.abspath('.')
    try:
        remotes = RemotesFile(study_path)
        remotes.load()
        r = remote.Remote()
        # Set to "default" if args.remote is None
        if args.remote == None:
            remote_name = remotes.default_remote
            _printer.print_msg("Using remote '{}'...".format(remote_name))
        else:
            remote_name = args.remote
        remote_yaml = remotes[remote_name]
        r.configure(remote_name, remote_yaml)
        print r.ssh_key_file
        connect(r, debug=args.debug)
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg("Error: " + str(error), "error")
            sys.exit()


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
    ret_info = {}
    state_list = ["CREATED", "UPLOADED", "SUBMITTED", "FINISHED", "DELETED", "DOWNLOADED"]
    remotes = []
    if remote is None:
        remotes = cases_remote.keys()
    else:
        if "CREATED" in allowed_states:
            if no_remote_cases is not None:
                try:
                    cases_remote[remote].extend(no_remote_cases)
                except KeyError:
                    cases_remote[remote] = no_remote_cases
                remotes = [remote]
        else:
            remotes = [remote]

    for r in remotes:
        ret_info[r] =  {"nof_selected": len(cases_remote[r]), "nof_valid": 0, "cases": {}, "valid_cases": []}
        for state in state_list:
            cases = list(set(study.get_cases([state], "status")).intersection(set(cases_remote[r])))
            ret_info[r]["cases"][state] = {"nof": len(cases), "list": cases}
            if state in allowed_states:
                ret_info[r]["valid_cases"].extend(cases)

        ret_info[r]["nof_valid"] = len(ret_info[r]["valid_cases"])

    return ret_info 

def job_status_action(args):
    action = "job-status"
    allowed_states = ["SUBMITTED"]
    def action_func_job_status(study_manager, remote):
        return study_manager.job_status(remote)

    def output_handler_job_status(output):
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
        _printer.print_msg("Marked for delete {} cases.".format(output))
    state_action(args, action, allowed_states, action_func_job_delete, output_handler_job_delete)

def upload_action(args):
    action = "upload"
    allowed_states = ["CREATED"]
    def action_func_upload(study_manager, remote):
        return study_manager.upload(remote, array_job=args.array_job, force=args.force)

    def output_handler_upload(output):
       pass 
    state_action(args, action, allowed_states, action_func_upload, output_handler_upload)

def download_action(args):
    action = "download"
    allowed_states = ["SUBMITTED", "FINISHED"]
    def action_func_download(study_manager, remote):
        return study_manager.download(remote, force=args.force)
    def output_handler_download(output):
       pass 
    state_action(args, action, allowed_states, action_func_download, output_handler_download)

def state_action(args, action, allowed_states, action_func, output_handler):
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    study = Study(study_name, study_path)
    study.load()
    if args.selector is None:
        case_selector = "*"
    else:
        case_selector = args.selector
    cases_idx = decode_case_selector(case_selector, study.nof_cases)
    study.set_selection(cases_idx)
    remote_cases = get_cases_byremote(cases_idx, study, allowed_states, remote=args.remote)
    if args.remote is not None:
        nof_remotes = 1
    else:
        nof_remotes = len(remote_cases.keys())
    _printer.print_msg("Selected {} cases ({} remotes) for action: '{}'...".format(len(cases_idx), nof_remotes,  action), "info")
    for counter, (remote_name, remote_info) in enumerate(remote_cases.items()):
        _printer.indent_level = 1
        _printer.print_msg("[{}] '{}': {} cases selected".format(counter+1, remote_name, remote_info["nof_selected"]), "info")
        for status, case_info  in remote_info["cases"].items():
            if case_info["nof"] > 0:
                _printer.indent_level = 2
                _printer.print_msg("{}: {}".format(status, case_info["nof"]))
        nof_excluded_cases = remote_info["nof_selected"] - remote_info["nof_valid"]
        if nof_excluded_cases > 0:
            _printer.print_msg("Found {} cases with a state not in {}. They will be ignored.".format(nof_excluded_cases, allowed_states))

 
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
        r = opts_get_remote(study_path, remote_name)
        connect(r, debug=args.debug, show_progress_bar=True)
        sm = remote.StudyManager(study)
        try:
            # The update affect all cases not only the selection
            sm.update_status(r)
            remote_cases_updated = get_cases_byremote(cases_idx, study, allowed_states, remote=remote_name)
            valid_cases = remote_cases_updated[remote_name]["valid_cases"]
            output = ""
            if valid_cases:
                _printer.print_msg("Performing action '{}' on {} cases...".format(action, len(valid_cases)), "info")
                study.set_selection(valid_cases)
                output = action_func(sm, r)
                _printer.indent_level = 2
                output_handler(output)
            else:
                _printer.print_msg("No jobs running found. Skipping...")
        except Exception as error:
            if args.debug:
                raise
            else:
                r.close()
                _printer.indent_level = 0
                _printer.print_msg(str(error), "error")
                sys.exit()
        r.close()
    _printer.print_msg("", "blank")
    _printer.indent_level = 0
    if sum([r["nof_valid"] for r in remote_cases.values()]) == 0:
            _printer.print_msg("Nothing to do for action: '{}'.".format(action), "info")
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

    # Parser print-tree
    parser_print_tree = subparsers.add_parser('print-tree', help="Print parameter tree.")
    parser_print_tree.set_defaults(func=print_tree_action)

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
    parser_download.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_download.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_download.add_argument('-f', '--force', action="store_true", help="Force download. Overwrite files.")
    parser_download.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    
    # Parser job-submit
    parser_job_submit = subparsers.add_parser('job-submit', help="Submit study remotely/localy.")
    parser_job_submit.set_defaults(func=job_submit_action)
    parser_job_submit.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_submit.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_job_submit.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    parser_job_submit.add_argument("--array-job", action="store_true", default=False, help="Submit study as a array of jobs.")

    # Parser job-status 
    parser_job_status = subparsers.add_parser('job-status', help="Query job status.")
    parser_job_status.set_defaults(func=job_status_action)
    parser_job_status.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_status.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_job_status.add_argument('-y', '--yes', action="store_true", help="Yes to all.")

    # Parser job-delete
    parser_job_delete = subparsers.add_parser('job-delete', help="Delete jobs associated with cases.")
    parser_job_delete.set_defaults(func=job_delete_action)
    parser_job_delete.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_job_delete.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_job_delete.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
 
       
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
