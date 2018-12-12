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
from common import replace_placeholders, MessagePrinter, ProgressBar
from study import Study, Case
from files import ParamInstance

import colorama as color
_printer = None
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



class StudyGenerator(MessagePrinter, Study):
    DEFAULT_DIRECTORIES = ["template/build", "template/input", "template/output", "template/postproc"]
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, study, short_name=False, build_once=False,
                 quiet=False, verbose=False, keep_onerror=False):
        super(StudyGenerator, self).__init__(quiet, verbose)
        #TODO: Check if the study case directory is empty and in good condition
        self.study = study
        self.short_name = short_name
        self.build_once = build_once
        self.keep_onerror = keep_onerror
        self.multiv_params = self.study.param_file.sections["PARAMS-MULTIVAL"].tree
        self.singlev_params = self.study.param_file.sections["PARAMS-SINGLEVAL"].data
        #TODO:Include build.sh to files to replace placeholders
        self.study.param_file["FILES"].append({"path": ".", "files": ["build.sh"]})
        self.template_path = os.path.join(self.study.path, "template")
        self.build_script_path = os.path.join(self.template_path, "build.sh")
        self.instances = []

    def execute_build_script(self, build_script_path):
        output = ""
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(build_script_path))
            output = subprocess.check_output(["bash", build_script_path], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            output = error.output
            raise Exception("Error while running 'build.sh' script.")
        finally:
            os.chdir(cwd)
            with open("build.log", "a+") as log:
                case = os.path.basename(os.path.dirname(build_script_path))
                log.write(case + ':\n')
                log.writelines(output)
                log.write('\n')


      #TODO: Create a file with instance information
    def _create_instance(self, instance_name, instance):
        self.print_msg("Creating instance '%s'..." % instance_name, verbose=True)
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
            replace_placeholders(file_paths, params)
            if not self.build_once:
                # Force execution permissions to 'build.sh'
                self.print_msg("Building...", verbose=True, end="")
                build_script_path = os.path.join(casedir, "build.sh")
                os.chmod(build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(build_script_path).st_mode))
                self.execute_build_script(build_script_path)
                self.print_msg("Done.", verbose=True, msg_type="unformated")
        except Exception as error:
            if not self.keep_onerror:
                shutil.rmtree(casedir)
            raise error
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
        self.print_msg("Generating cases...")
        if not os.path.exists(self.template_path):
            raise Exception("Cannot find 'template' directory!")
        if os.path.exists(self.build_script_path):
            if self.build_once:
                self.print_msg("Building once from 'build.sh'...")
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
        self.print_msg("Success: Created %d cases." % nof_instances)

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
            print defaults.keys(), defaults_node.keys(), node
            raise Exception("Parameter(s) '{}'  with same name.".format(tuple(common_params)))
        defaults.update(defaults_node)
        if node.is_root:
            for child in node.children:
                span_mult(child)
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
        sys.exit("Error: File 'remote.yaml' not found in study directory.")
    try:
        _printer.print_msg("Testing connection to remote '%s'..." % remote_name, end='')
        r.available()
        #TODO: Change to Python3 print function to avoid new lines
        # print "OK."
    except remote.ConnectionTimeout as error:
        sys.exit("Error: " + str(error))
    except Exception as error: 
        sys.exit("Error: " + str(error))
    return r


def connect(remote, passwd, show_progress_bar=False):
    try:
        if show_progress_bar:
            progress_bar = ProgressBar()
            remote.connect(passwd, progress_callback=progress_bar.callback)
        else:
            remote.connect(passwd)
    except Exception as error:
        if args.debug:
            raise
        else:
            sys.exit("Error: " + str(error))


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
            sys.exit("Error: " + str(error))

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
                              verbose=args.verbose, keep_onerror=args.keep_on_error)
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
            sys.exit("File 'cases.info' not found. Nothing to delete.")
        _printer.print_msg("Deleting %d cases..." % study.nof_cases, "info")
        _printer.print_msg("Deleting study files...", "info")
        study.delete()
        _printer.print_msg("Done.", "info")
    except Exception as error:
        if args.debug:
            raise
        else:
            _printer.print_msg("Error: " + str(error), "error")
            sys.exit()

def init_remote_action(args):
    remote_name = args.remote
    study_path = os.path.abspath('.')
    study_name = os.path.basename(study_path)
    r = opts_get_remote(study_path, remote_name)
    passwd = getpass.getpass("Password(%s): " % r.name)
    connect(r, passwd)
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


#NOTES: 1) Only cases in the CREATED state are uploaded by default.
#       2) Report selected ones which are in SUBMITTED, DOWNLOADED, FINISHED and UPLOADED state.
#       3) If --force is used all selected cases will be (re-)uploaded and STATE=UPLOADED
def upload_action(args):
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
    _printer.print_msg("Selected %d cases to upload" % len(cases_idx), "info")
    no_upload_cases = study.get_cases(["UPLOADED", "FINISHED", "SUBMITTED", "DOWNLOADED"], "status")
    # no_upload_cases = list(set(study.case_selection).intersection(set(no_upload_cases)))
    if no_upload_cases:
        if not args.force:
            _printer.print_msg("%d selected cases has been already uploaded and will be *ignored*." % len(no_upload_cases), "info")
        else:
            _printer.print_msg("%d selected cases has been already uploaded and will be *overwritten*."  % len(no_upload_cases), "warning")
    if args.yes:
        opt = 'Y'
    else:
        _printer.print_msg("Continue?['Y','y']:", "input", end='')
        opt = raw_input("")
    if opt in ['y', 'Y']:
        r = opts_get_remote(study_path, args.remote)
        if not args.force:
            upload_cases = set(study.case_selection) - set(no_upload_cases)
            upload_cases_idx = [c.id for c in upload_cases]
            study.set_selection(upload_cases_idx)
        _printer.print_msg("Uploading %d cases to remote '%s'..." % (len(study.case_selection), args.remote), "info")
        _printer.print_msg("Password(%s): " % r.name , "input", end='')
        passwd = getpass.getpass("")
        connect(r, passwd, show_progress_bar=True)
        sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
        try:
            sm.upload(r, array_job=args.array_job, force=args.force)
        except Exception as error:
            if args.debug:
                raise
            else:
                r.close()
                sys.exit("Error: " + str(error))
        r.close()

#NOTES: 1) Cases that have not been uploaded to a remote are ignored from the selected ones.
#       2) Cases in UPLOADED and DOWNLOADED state are not downloaded either.
#       3) If --force is used all selected cases will be downloaded and local files/directories overwritten.

def download_action(args):
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
    cases_remote = study.get_cases(cases_idx, "id", sortby="remote")
    print "[Selected %d cases to download]" % len(cases_idx)
    no_remote_cases = cases_remote.pop(None, None)
    if no_remote_cases is not None:
        print "Info: %d selected cases has not been uploaded yet and will be ignored." % len(no_remote_cases)
    nof_remote_downloads = {}
    for remote_name, remote_cases in cases_remote.items():
        ucases = set(study.get_cases(["UPLOADED"], "status"))
        nof_uploaded = len(set(remote_cases).intersection(set(ucases)))  
        dcases = set(study.get_cases(["DOWNLOADED"], "status"))
        nof_downloaded = len(set(remote_cases).intersection(set(dcases)))  
        print "  -'%s': %d cases (Info: %d downloaded, %d not submitted)" % (remote_name,\
                len(remote_cases), nof_downloaded, nof_uploaded)
        if args.force:
            nof_remote_downloads.update({remote_name: len(remote_cases)})
        else:
            nof_remote_downloads.update({remote_name: len(remote_cases) - (nof_downloaded + nof_uploaded)})
    if args.yes:
        opt = 'Y'
    else:
        opt = raw_input("Continue?['Y','y']:")
    if opt in ['y', 'Y']:
        for remote_name, remote_cases in cases_remote.items():
            r = opts_get_remote(study_path, remote_name)
            print "Downloading %d cases from remote '%s'..." % (nof_remote_downloads[remote_name], remote_name)
            # Continue to the next remote if there are not cases to download
            if nof_remote_downloads[remote_name] == 0:
                continue
            passwd = getpass.getpass("Password(%s): " % remote_name)
            connect(r, passwd, show_progress_bar=True)
            sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
            try:
                # Force to download even not submitted cases
                if not args.force:
                    sm.update_status(r)
                    # Get cases in remote and "FINISHED" from the selection
                    fcases = set(study.get_cases(["FINISHED"], "status"))
                    new_selection = set(study.case_selection).intersection(remote_cases, fcases)
                else:
                    new_selection = remote_cases
                new_selection_idx = [c.id for c in new_selection]
                study.set_selection(new_selection_idx)
                sm.download(r, force=args.force)
            except Exception as error:
                if args.debug:
                    raise
                else:
                    r.close()
                    sys.exit("Error:" + str(error))
            r.close()
        print "Done."

def submit_action(args):
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
    cases_remote = study.get_cases(cases_idx, "id", sortby="remote")
    print "[Selected %d cases to submit]" % len(cases_idx)
    no_remote_cases = cases_remote.pop(None, None)
    if no_remote_cases is not None:
        print "Warning: %d selected cases has not been uploaded yet to a remote. Ignoring them..." % len(no_remote_cases)
    nof_remote_submit = {}
    for remote_name, remote_cases in cases_remote.items():
        acases = set(study.get_cases(["FINISHED", "SUBMITTED", "DOWNLOADED"], "status"))
        nof_not_available = len(set(remote_cases).intersection(set(acases)))
        if nof_not_available == len(remote_cases):
            print "  -'%s': %d cases (Warning: No cases available to submit)" % (remote_name, len(remote_cases))
            continue
        elif nof_not_available == 0:
            print "  -'%s': %d cases." % (remote_name, len(remote_cases))
        else:
            print "  -'%s': %d cases (Warning: %d cases not available to submit.)"\
                  % (remote_name, len(remote_cases), nof_not_available)
        nof_remote_submit.update({remote_name: len(remote_cases) - nof_not_available})
    if nof_remote_submit:
        opt = raw_input("Continue?['Y','y']:")
    else:
        opt = "no"
    if opt in ['y', 'Y']:
        for remote_name, remote_cases in cases_remote.items():
            r = opts_get_remote(study_path, remote_name)
            print remote_name
            print "Submitting %d cases to remote '%s'..." % (nof_remote_submit[remote_name], remote_name)
            passwd = getpass.getpass("Password(%s): " % remote_name)
            connect(r, passwd)
            sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
            try:
                # Get cases in remote and "FINISHED" from the selection
                fcases = set(study.get_cases(["UPLOADED"], "status"))
                new_selection = set(study.case_selection).intersection(remote_cases, fcases)
                new_selection_idx = [c.id for c in new_selection]
                study.set_selection(new_selection_idx)
                sm.submit(r, force=args.force, array_job=args.array_job)
            except Exception as error:
                if args.debug:
                    raise
                else:
                    r.close()
                    sys.exit("Error:" + str(error))
            r.close()







def main(args=None):
    """The main routine."""
    # if args is None:
    #     args = sys.argv
    import argparse
    color.init()
    parser = argparse.ArgumentParser(description="Program to generate parameter studies.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False)
    group.add_argument("-q", "--quiet", action="store_true", default=False)
    actions_group = parser.add_mutually_exclusive_group()
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

    # Parser print-tree
    parser_delete = subparsers.add_parser('print-tree', help="Print parameter tree.")
    parser_delete.set_defaults(func=print_tree_action)

    # Parser delete 
    parser_delete = subparsers.add_parser('delete', help="Delete all instances in a study.")
    parser_delete.set_defaults(func=delete_action)

    # Parser init-remote
    parser_init_remote = subparsers.add_parser('init-remote', help="Get a resumed info of the study.")
    parser_init_remote.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_init_remote.set_defaults(func=init_remote_action)

    # Parser clean
    parser_clean = subparsers.add_parser('clean', help="Clean all instances in a study.")
    parser_clean.set_defaults(func=clean_action)
    parser_clean.add_argument('-c', type=str, help="Case selector.")

    # Parser upload
    parser_upload = subparsers.add_parser('upload', help="Upload study to remote.")
    parser_upload.set_defaults(func=upload_action)
    parser_upload.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_upload.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_upload.add_argument('-f', '--force', action="store_true", help="Force upload. Overwrite files.")
    parser_upload.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    
    # Parser submit
    parser_upload = subparsers.add_parser('submit', help="Submit study remotely/localy.")
    parser_upload.set_defaults(func=submit_action)
    parser_upload.add_argument('-s', '--selector', type=str, help="Case selector.")
    parser_upload.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_upload.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    

    
    # Parser download 
    parser_download = subparsers.add_parser('download', help="download study to remote.")
    parser_download.set_defaults(func=download_action)
    parser_download.add_argument('-s', '--selector', type=str, help="Case selector.")
    # NOTE: In principle this is not necessary as the remote is provided from cases.info
    # parser_download.add_argument('-r', '--remote', type=str, help="Remote name.")
    parser_download.add_argument('-f', '--force', action="store_true", help="Force download. Overwrite files.")
    parser_download.add_argument('-y', '--yes', action="store_true", help="Yes to all.")
    
    # actions_group.add_argument("--check", action="store_true", help="Check if the study is consistent with 'params.yaml' file.")
    # actions_group.add_argument("--create", metavar="study_name", help="Create an empty study template.")
    # actions_group.add_argument("--generate", nargs="?", metavar="study_name", const=".", help="Generate the instances of the study based on the 'params.yaml' file.")
    # actions_group.add_argument("--init-remote", nargs="?", metavar="remote_name", const="default", help="Get a resumed info of the study.")
    actions_group.add_argument("--info", action="store_true", help="Get a resumed info of the study.")
    # actions_group.add_argument("--clean", nargs="?", metavar="cases_list", const='*', help="Clean the instances of a study.")
    # actions_group.add_argument("--delete", action="store_true", help="Delete all the instances of a study.")
    # actions_group.add_argument("--upload", nargs="?", const="*", metavar="study_name", help="Upload study to remote.")
    actions_group.add_argument("--download", nargs="?", const="*", metavar="study_name", help="Download study from remote.")

    actions_group.add_argument("--qstatus", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("--qdelete", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("--qsubmit", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    # Actions modifiers
    parser.add_argument("--cases", default='*', metavar="case_indices", nargs="?", const="*", help="Case selector.")
    parser.add_argument("--array-job", action="store_true", default=False, help="Submit the study as a array of jobs.")
    parser.add_argument("--remote", nargs="?", const=None, metavar="remote_name", help="Specify remote for an action.")
    parser.add_argument("--force", action="store_true", default=False, help="Specify remote for an action.")
    parser.add_argument("--debug", action="store_true", default=False, help="Debug mode.")
    args = parser.parse_args()
    global _printer
    _printer = MessagePrinter(verbose=args.verbose, quiet=args.quiet)
    args.func(args)


                
    if args.qsubmit:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        study = Study(study_name, study_path)
        study.load()
        case_selector = args.qsubmit
        cases_idx = decode_case_selector(case_selector, study.nof_cases)
        study.set_selection(cases_idx)
        cases_remote = study.get_cases(cases_idx, "id", sortby="remote")
        print "[Selected %d cases to submit]" % len(cases_idx)
        no_remote_cases = cases_remote.pop(None, None)
        if no_remote_cases is not None:
            print "Warning: %d selected cases has not been uploaded yet to a remote. Ignoring them..." % len(no_remote_cases)
        nof_remote_submit = {}
        for remote_name, remote_cases in cases_remote.items():
            acases = set(study.get_cases(["FINISHED", "SUBMITTED", "DOWNLOADED"], "status"))
            nof_not_available = len(set(remote_cases).intersection(set(acases)))
            if nof_not_available == len(remote_cases):
                print "  -'%s': %d cases (Warning: No cases available to submit)" % (remote_name, len(remote_cases))
                continue
            elif nof_not_available == 0:
                print "  -'%s': %d cases." % (remote_name, len(remote_cases))
            else:
                print "  -'%s': %d cases (Warning: %d cases not available to submit.)"\
                      % (remote_name, len(remote_cases), nof_not_available)
            nof_remote_submit.update({remote_name: len(remote_cases) - nof_not_available})
        if nof_remote_submit:
            opt = raw_input("Continue?['Y','y']:")
        else:
            opt = "no"
        if opt in ['y', 'Y']:
            for remote_name, remote_cases in cases_remote.items():
                r = opts_get_remote(study_path, remote_name)
                print remote_name
                print "Submitting %d cases to remote '%s'..." % (nof_remote_submit[remote_name], remote_name)
                passwd = getpass.getpass("Password(%s): " % remote_name)
                connect(r, passwd)
                sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
                try:
                    # Get cases in remote and "FINISHED" from the selection
                    fcases = set(study.get_cases(["UPLOADED"], "status"))
                    new_selection = set(study.case_selection).intersection(remote_cases, fcases)
                    new_selection_idx = [c.id for c in new_selection]
                    study.set_selection(new_selection_idx)
                    sm.submit(r, force=args.force, array_job=args.array_job)
                except Exception as error:
                    if args.debug:
                        raise
                    else:
                        r.close()
                        sys.exit("Error:" + str(error))
                r.close()




    # elif args.submit_case:
    #     abs_remote_path = os.path.abspath(args.submit_case)
    #     r = opts_get_remote(os.path.dirname(abs_remote_path), args)
    #     if r.available():
    #         passwd = getpass.getpass("Password: ")
    #     else:
    #         sys.exit("Error: Remote '%s' not available." % r.name)
    #     try:
    #         r.connect(passwd)
    #     except Exception as error:
    #         sys.exit(error)
    #     sm = remote.StudyManager(r, case_path=abs_remote_path)
    #     try:
    #         sm.submit_case()
    #     except Exception as error:
    #         r.close()
    #         sys.exit(error)
    #     r.close()
    #





            # raise Exception("File 'cases.info' is empty. Cannot download case.")

    # elif args.upload_case:
    #     abs_remote_path = os.path.abspath(args.upload_case)
    #     r = opts_get_remote(os.path.dirname(abs_remote_path), args)
    #     if r.available():
    #         passwd = getpass.getpass("Password: ")
    #     else:
    #         sys.exit("Error: Remote '%s' not available." % r.name)
    #     try:
    #         r.connect(passwd)
    #     except Exception as error:
    #         sys.exit(error)
    #     sm = remote.StudyManager(r, case_path=abs_remote_path)
    #     try:
    #         sm.upload_case(force=args.force)
    #     except Exception as error:
    #         r.close()
    #         sys.exit(error)
    #     r.close()
    #


    elif args.qstatus:
        c = Case()
        print c["id"]

        # abs_remote_path = os.path.abspath(args.status)
        # r = opts_get_remote(abs_remote_path, args)
        # if r.available():
        #     passwd = getpass.getpass("Password: ")
        # else:
        #     sys.exit("Error: Remote '%s' not available." % r.name)
        # try:
        #     r.connect(passwd)
        # except Exception as error:
        #     sys.exit(error)
        # sm = remote.StudyManager(r, study_path=abs_remote_path)
        # sm.update_status()
        # # try:
        # #     sm.status()
        # # except Exception as error:
        # #     r.close()
        # #     sys.exit(error)
        # r.close()


if __name__ == "__main__":
    main()
