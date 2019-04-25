#!/usr/bin/env python2
import time
import yaml
import math
import stat
import os
import glob
import shutil
import re
import sys
import remote
import getpass
import subprocess
import json
import anytree
from anytree.importer import DictImporter
from common import replace_placeholders, _printer, ProgressBar
from study import Study, Case
from files import ParamInstance

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



class StudyGenerator(Study):
    DEFAULT_DIRECTORIES = ["template/build", "template/input", "template/output", "template/postproc"]
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, study, short_name=False, build_once=False,
                 quiet=False, verbose=False, keep_onerror=False, abort_undefined=True):
        super(StudyGenerator, self).__init__(quiet, verbose)
        #TODO: Check if the study case directory is empty and in good condition
        self.study = study
        self.short_name = short_name
        self.build_once = build_once
        self.keep_onerror = keep_onerror
        self.abort_undefined = abort_undefined 
        self.multiv_params = self.study.param_file.sections["PARAMS-MULTIVAL"].tree
        # Not mandatory to have this section
        try:
            self.singlev_params = self.study.param_file.sections["PARAMS-SINGLEVAL"].data
        except KeyError:
            pass
        # Include build.sh to files to replace placeholders
        self.study.param_file["FILES"].append({"path": ".", "files": ["build.sh"]})
        self.template_path = os.path.join(self.study.path, "template")
        self.build_script_path = os.path.join(self.template_path, "build.sh")
        self.instances = []

    def execute_build_script(self, build_script_path):
        output = ""
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(build_script_path))
            output = subprocess.check_output(["bash", "-e", build_script_path], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            output = error.output
            raise Exception("Error while running 'build.sh' script. Check 'build.log' file.")
        finally:
            os.chdir(cwd)
            with open("build.log", "a+") as log:
                case = os.path.basename(os.path.dirname(build_script_path))
                log.write(case + ':\n')
                log.writelines(output)
                log.write('\n')


    #TODO: Create a file with instance information
    def _create_instance(self, instance_name, instance):
        _printer.print_msg("Creating instance '%s'..." % instance_name, verbose=True)
        casedir = os.path.join(self.study.path, instance_name)
        studydir = os.path.dirname(casedir)
        shutil.copytree(self.template_path, casedir)
        try:
            # self._create_instance_infofile(instance)
            # Create paths for files listed for replace params on them
            file_paths =  []
            for path in self.study.param_file["FILES"]:
                for f in path["files"]:
                    p = os.path.join(os.path.join(self.study.path, instance_name), path["path"])
                    p = os.path.join(p, f)
                    file_paths.append(p)
            # Add parampy specific params
            params = {"PARAMPY-CN": instance_name,
                      "PARAMPY-SN": self.study.param_file["STUDY"]["name"],
                      "PARAMPY-CD": casedir, 
                      "PARAMPY-SD": studydir}
            params.update(instance)
            replace_placeholders(file_paths, params, self.abort_undefined)
            if not self.build_once:
                # Force execution permissions to 'build.sh'
                _printer.print_msg("Building...", verbose=True, end="")
                build_script_path = os.path.join(casedir, "build.sh")
                os.chmod(build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(build_script_path).st_mode))
                self.execute_build_script(build_script_path)
                _printer.print_msg("Done.", verbose=True, msg_type="unformated")
        except Exception:
            if not self.keep_onerror:
                shutil.rmtree(casedir)
            raise
        return instance_name

    def _instance_directory_string(self, instance_id, params, nof_instances, short_name=False):
        instance_string = ""
        nof_figures = len(str(nof_instances-1))
        instance_string = "%0*d_" % (nof_figures, instance_id)
        if not short_name:
            for pname, pval in params.items():
                instance_string += "%s-%s_" % (pname, pval)
        instance_string = instance_string[:-1]
        return instance_string


    #TODO: Decouple state and behaviour of instances into a new class
    def generate_cases(self):
        self._generate_instances()
        # Check if build.sh has to be run before generating the instances
        _printer.print_msg("Generating cases...")
        if not os.path.exists(self.template_path):
            raise Exception("Cannot find 'template' directory!")
        if os.path.exists(self.build_script_path):
            if self.build_once:
                _printer.print_msg("Building once from 'build.sh'...")
                # Force execution permissions to 'build.sh'
                os.chmod(self.build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(self.build_script_path).st_mode))
                self.execute_build_script(self.build_script_path)
        else:
            if self.build_once:
                raise Exception("No 'build.sh' script found but '--build-once' option was specified.")
        nof_instances = len(self.instances)
        for instance_id, instance in enumerate(self.instances):
            # Resolve generators
            instance.resolve_params()
            multival_params = self._get_multival_params(instance)
            instance_name = self._instance_directory_string(instance_id, multival_params,
                                                      nof_instances, self.short_name)
            self._create_instance(instance_name, instance)
            self.study.add_case(instance_name, multival_params, short_name=self.short_name)

        self.study.save()
        _printer.print_msg("Success: Created %d cases." % nof_instances)

    def _generate_instances(self):
        instance = ParamInstance()
        self.instances = []
        self._gen_comb_instances(instance, self.multiv_params)

    def _gen_comb_instances(self, instance, node, val_idx=0, defaults={}):
        # Stop condition
        if node is None:
            instance.update(self.singlev_params)
            instance.update(defaults)
            self.instances.append(instance.copy())
            return 

        def span_mult(child):
            for val_idx, val in enumerate(node.values):
                instance[node.name] = val
                self._gen_comb_instances(instance, child, val_idx, defaults.copy())
                
        def span_add(child):
            instance[node.name] = node.values[val_idx]
            self._gen_comb_instances(instance, child, defaults=defaults.copy())

        try:
            defaults_node = node.defaults
        except:
            defaults_node = {}
        common_params = set(defaults.keys()).intersection(set(defaults_node.keys()))
        if common_params:
            # print defaults.keys(), defaults_node.keys(), node
            raise Exception("Parameter(s) '{}'  with same name.".format(tuple(common_params)))
        defaults.update(defaults_node)
        if node.is_root:
            if node.children:
                for child in node.children:
                    span_mult(child)
            else:
                span_mult(None)
        elif not node.is_leaf:
            for child in node.children:
                if node.mode == "*":
                    span_mult(child)
                elif node.mode == "+":
                    span_add(child)
        else:
            if node.mode == "*":
                span_mult(None)
            elif node.mode == "+":
                span_add(None)

    def _get_multival_params(self, instance):
        return {k:v for k,v in instance.items() if k in
                self.study.param_file.sections["PARAMS-MULTIVAL"].param_names}

    @classmethod 
    def create_study(cls, path, study_name):
        study_path = os.path.join(path, study_name)
        study_template_path = os.path.join(study_path, "template")
        if os.path.exists(study_path):
            raise Exception("Cannot create study. Directory '%s' already exists!" % study_name)
        os.makedirs(study_path)
        os.makedirs(study_template_path)
        for directory in cls.DEFAULT_DIRECTORIES:
            directory_path = os.path.join(study_path, directory) 
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
        for f in cls.DEFAULT_FILES:
            shutil.copy(os.path.join(DEFAULTS_DIR, os.path.basename(f)), 
                        os.path.join(study_path, f))

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
        print "OK."
    except remote.ConnectionTimeout as error:
        print "Failed."
        _printer.print_msg(str(error), "error")
        _printer.print_msg("Aborting...", "error")
        sys.exit()
    except Exception as error: 
        print "Failed."
        _printer.print_msg(str(error), "error")
        _printer.print_msg("Aborting...", "error")
        sys.exit()
    return r


def connect(remote, debug=False, show_progress_bar=False):
    attempts = 0
    while attempts < 3 and remote.password_ask or attempts == 0:
        try:
            passwd = None
            if remote.password_ask:
                _printer.print_msg("Password (%s): " % remote.name , "input", end='')
                passwd = getpass.getpass("")
            if show_progress_bar:
                progress_bar = ProgressBar()
                remote.connect(passwd, progress_callback=progress_bar.callback)
            else:
                remote.connect(passwd)
        except Exception as error:
            attempts += 1
            if attempts < 3 and remote.password_ask:
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
                              quiet=args.quiet,
                              verbose=args.verbose, keep_onerror=args.keep_on_error,
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
    study_name = os.path.basename(study_path)
    try:
        study = Study(study_name, study_path, load_param_file=False)
        study.load()
        if args.c is None:
            case_selector = "*"
        else:
            case_selector = args.c
        cases_idx = decode_case_selector(case_selector, study.nof_cases)
        study.set_selection(cases_idx)
        print "Cleaning %d cases..." % len(cases_idx)
        study.clean()
        print "Done."
    except Exception as error:
        if args.debug:
            raise
        else:
            sys.exit("Error: " + str(error))

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
        sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
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
