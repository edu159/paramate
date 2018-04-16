#!/usr/bin/env python2
import yaml
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

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")

# NOTE: NOT USED ANYMORE BUT USEFUL!
# def str2type(l):
#     def converter(l):
#         for i in l:
#             try:
#                 yield json.loads(i)
#             except ValueError:
#
#                 yield i
#     return list(converter(l))
#

def _decode_case_selector(selector):
    if selector is None:
        return None
    match_group = re.match("(\d+):(\d+)(?::(\d+))?$", selector)
    if match_group is not None:
        selector_split = selector.split(':')
        rmin, rmax = int(selector_split[0]), int(selector_split[1])
        if len(selector_split) == 3:
            step = int(selector_split[2])
        else:
            step = 1
        try:
            return list(xrange(rmin, rmax+1, step))
        except Exception:
            raise Exception("Case selector range not well formed.")
    match_group = re.match("\d+(?:,(?:\d+))*$", selector)
    if match_group is not None:
        return [int(c) for c in selector.split(',')]
    return False



def instance_dirstring(instance_id, params, nof_instances, short_name=False):
    instance_string = ""
    nof_figures = len(str(nof_instances))
    instance_string = "%0*d_" % (nof_figures, instance_id)
    if not short_name:
        for pname, pval in params.items():
            instance_string += "%s-%s_" % (pname, pval)
    instance_string = instance_string[:-1]
    return instance_string


# Parampy params useful to build paths.
def replace_placeholders(file_paths, params):
    for path in file_paths:
        try:
            lines = []
            with open(path, 'r') as placeholder_file:
                lines = placeholder_file.readlines()                                                                                                                                                                                                 
            for ln, line in enumerate(lines):
                line_opts = set(re.findall(r'\$\[([a-zA-Z0-9\-]+?)\]', line))
                for opt in line_opts:
                    try:
                        lines[ln] = lines[ln].replace("$[" + opt + "]", str(params[opt]))
                    except KeyError as error:
                        raise Exception("Parameter '%s' not present." % opt)
            with open(path, 'w+') as replaced_file:
                replaced_file.writelines(lines)
        except Exception as error:
            raise error


# class ConfigDirectory:
#     def __init__(self):
#         self.default_dir = os.path.join(os.getenv("HOME"), ".parampy")
#         self.default_remotes_dir = os.path.join(self.default_dir, "remotes")
#
class StudySection:
    pass
class ParamsSection:
    pass
class FilesSection:
    pass
class DownloadSection:
    pass

#TODO: Decouple allowed sections from Param file to make it general
class ParamFile:
    def __init__(self, allowed_sections=None):
        self.ALLOWED_SECTIONS = {"STUDY": StudySection, 
                                 "PARAMETERS": ParamsSection,
                                 "DOWNLOAD": DownloadSection,
                                 "FILES": FilesSection}
        self.params_fname = ""
        self.loaded = False
        self.params_data = {}
        self.sections ={}
        self.path = ""

    @staticmethod
    def paramfile_exists(path):
        p = os.path.join(path, "params.yaml")
        return os.path.exists(p)
        
    def load(self, path="."):
        fname = os.path.join(path, "params.yaml")
        self.path = path
        try:
            with open(fname, 'r') as paramfile:
                self.params_data = yaml.load(paramfile)
        except IOError:
            raise Exception("File params.yaml not found!")
        except Exception as error:
            raise Exception("Parsing error: \n" + str(error))
        self._load_sections()
        self.params_fname = fname
        self.loaded = True


    def _load_sections(self):
        for section_name, section_opts in  self.params_data.items():
            try:
                section_class =  self.ALLOWED_SECTIONS[section_name]
                # self.sections[section_name] = section_class(section_name, section_opts)
            except Exception as error:
                raise
                # raise Exception("Error: Section '%s' is mandatory in 'params.yaml'." % section_name)

            #TODO: Rework this and check for correct format of params.yaml. Add this into ParamSection
            if section_name == "PARAMETERS":
                try:
                    sys.path.insert(0, self.path)
                    import generators
                except Exception as err:
                    pass
                for param_name, param_fields in section_opts.items():
                    if str(param_fields["value"]).startswith("gen:"):
                        gen_name = param_fields["value"].split(":")[1]
                        try:
                            param_fields["value"] = getattr(generators, gen_name)()
                        except AttributeError as error:
                            raise Exception("Generator '%s' not found in 'generators.py'.")
                        except Exception as error:
                            raise Exception("Error in 'genenerators.py - '" + str(error))
                        if type(param_fields["value"]) is not list:
                            raise Exception("Generators must return a list of values. Got '%s'."\
                                            % type(param_fields["value"]))

            
    def add_section(self, section, opts):
        self.config_data[section] = opts

    #TODO: Add this to the specific DownloadSection object
    def get_download_paths(self, case):
        path_list = []
        case_path = os.path.join(self.path, case)
        try:
            paths = self["DOWNLOAD"]
            for path in paths:
                current_path = os.path.join(case_path, path["path"])
                try:
                    include_files = path["include"]
                    for f in include_files:
                        path_list.append(os.path.join(current_path, f))
                except KeyError:
                    try:
                        import glob
                        exclude = [os.path.join(current_path, p)  for p in path["exclude"]]
                        all_files = glob.glob(os.path.join(current_path, "*"))
                        # print all_files, exclude
                        path_list.extend(list(set(all_files) - set(exclude)))
                    except KeyError:
                        path_list.append(current_path)
        except KeyError:
            #BY default case/postproc and case/output are the ones to download
            path_list.append(os.path.join(case_path, "postproc"))
            path_list.append(os.path.join(case_path, "output") )
        return path_list


    def __getitem__(self, key):
        if self.loaded:
            return self.params_data[key]
        else:
            raise Exception()

JOB_STATES = ["CREATED", "UPLOADED", "SUBMITTED", "FINISH", "DOWNLOADED"]

class StudyFile:
    def __init__(self, path='.', fname="cases.info"):
        self.lines = []
        self.fname = fname
        self.cases = []
        self.file_path = os.path.join(os.path.abspath(path), fname)
        self.nof_cases = 0

    def backup(self, dest):
        shutil.copy(self.file_path, os.path.join(dest, "cases.info.bak"))

    def restore(self, orig):
        shutil.copy(os.path.join(orig, "cases.info.bak"), self.file_path)

    def add_instance(self, params, short_name=False):
        self.nof_cases += 1
        self.cases.append({"id": self.nof_cases, 
                           "params": params.copy(), 
                           "shortname": short_name,
                           "jid": None,
                           "status": "CREATED",
                           "req_time": None,
                           "remote": None,
                           "sub_date": None,
                           "name":"" })

    def clean_case(self, idx):
        self.cases[idx-1]["jid"] = None 
        self.cases[idx-1]["status"] = "CREATED"
        self.cases[idx-1]["req_time"] = None
        self.cases[idx-1]["sub_date"] = None
        self.cases[idx-1]["remote"] = None


    def get_cases(self, search_vals, field, sortby=None):
        match_list = []
        for case in self.cases:
            if case[field] in search_vals:
                match_list.append(case)
        return match_list
# for case in download_cases:
#             try:
#                 remote_cases[case["remote"]].append(case)
#             except KeyError:
#                 remote_case[case["remote"]] = []
#                 remote_cases[case["remote"]].append(case)
#


    def read(self):
        with open(self.file_path, 'r') as rfile:
            json_data = rfile.read()
            json_data = json.loads(json_data)
            self.cases = json_data["cases"]
            self.nof_cases = len(self.cases)

    def write(self):
        with open(self.file_path, 'w') as wfile:
            for i, case in enumerate(self.cases):
                case["name"] = instance_dirstring(i+1, case["params"], self.nof_cases,
                                                  short_name=case["shortname"])
            json_data = {"date": "May", "cases": self.cases}
            wfile.write(json.dumps(json_data, indent=4, sort_keys=True))

    def update_case(self, id_field, values):
        pass


    def is_empty(self):
        pass

    @staticmethod
    def exists(path, fname="cases.info"):
        return os.path.exists(os.path.join(path, fname))

class MessagePrinter(object):
    def __init__(self, quiet, verbose):
        self.quiet = quiet
        self.verbose = verbose

    def print_msg(self, message, ignore_quiet=False, verbose=False, end="\n"):
        if not self.quiet:
            if verbose:
                if self.verbose:
                    sys.stdout.write(message+end)
            else:
                sys.stdout.write(message+end)
        else:
            if ignore_quiet:
                    sys.stdout.write(message+end)



class StudyBuilder(MessagePrinter):
    DEFAULT_DIRECTORIES = ["template/build", "template/input", "template/output", "template/postproc"]
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, study_path, only_one=False, short_name=False, build_once=False,
                 quiet=False, verbose=False):
        super(StudyBuilder, self).__init__(quiet, verbose)
        #TODO: Check if the study case directory is empty and in good condition
        self.study_path = os.path.abspath(study_path)
        self.only_one = only_one
        self.short_name = short_name
        self.build_once = build_once
        self.instance_counter = 0
        self.param_file = None
        self._load_config_file()
        self.params = self.param_file["PARAMETERS"]
        self.linear_param_size = 0
        self.linear_param_list = self._get_params_by_mode("linear")
        self.combinatoric_param_list = self._get_params_by_mode("combinatoric")
        linear_multival_param_list = self._check_linear_params()
        self.nof_instances = self._compute_nof_instances()
        self.multival_param_list = linear_multival_param_list + self.combinatoric_param_list
        self.multival_keys = [d["name"] for d in self.multival_param_list] 
        self.manifest_lines = []
        self.study_file = StudyFile(path=self.study_path)
        self.template_path = os.path.join(self.study_path, "template")
        self.build_script_path = os.path.join(self.template_path, "build.sh")
        self.instance_name = None

    def execute_build_script(self, build_script_path):
        output = ""
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(build_script_path))
            output = subprocess.check_output([build_script_path], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            output = error.output
            raise Exception("Error while running 'build.sh' script.")
        finally:
            os.chdir(cwd)
            with open("build.log", "w") as log:
                log.writelines(output)

 

    def _compute_nof_instances(self):
        nof_instances = 1
        if self.linear_param_list:
            nof_instances = len(self.linear_param_list[0]["value"])
        for param in self.combinatoric_param_list:
            nof_instances *= len(param["value"])
        return nof_instances

    @staticmethod
    def templatedir_exists(path):
        p = os.path.join(path, "template")
        return os.path.exists(p)

    @staticmethod
    def clean_study(path, case_selector):
        study_file = StudyFile(path=path)
        if study_file.exists(path):
            study_file.read()
            if case_selector == '*':
                case_idx = list(xrange(1, study_file.nof_cases+1))
            else:
                case_idx = _decode_case_selector(case_selector)
                if not case_idx:
                    raise Exception("Case selector malformed.")
            for i in case_idx:
                if i > study_file.nof_cases:
                    raise Exception("Index '%d' out of range. The number of cases is '%d'." % (i, study_file.nof_cases))
                if i == 0:
                    raise Exception("Index 0 found. Case indices start at 1.")

            print "Cleaning '%d' cases..." % len(case_idx)
            param_file = ParamFile()
            param_file.load(path)
            for idx in case_idx:
                study_file.clean_case(idx)
                d = param_file.get_download_paths(study_file.cases[idx-1]["name"])
                #TODO: Remove files for real
                print study_file.cases[idx-1]["name"]
                print d 
                # shutil.rmtree()
            study_file.write()
            print "Done."
        else:
            print "Nothing to clean, file 'cases.info' not found."

    
    @staticmethod
    def erase_study(path):
        study_file = StudyFile(path=path)
        if study_file.exists(path):
            study_file.read()
            if not study_file.is_empty():
                    print "Deleting %d cases..." % len(study_file.cases)
                    for case in study_file.cases:
                        f = instance_dirstring(case["id"], case["params"],
                                               study_file.nof_cases,
                                               short_name=case["shortname"])
                        try:
                            shutil.rmtree(os.path.join(path, f))
                        except Exception as error:
                            pass
                    print "Deleting file 'cases.info'..."
                    os.remove(os.path.join(path, "cases.info"))
            print "Done."
        else:
            print "Nothing to delete, file 'cases.info' not found."


    def _create_instance_infofile(self, instance):
        f = os.path.join(self.instance_name, "instance.info")
        f = os.path.join(self.study_path, f)
        open(f, 'a').close()

    def _load_config_file(self):
        try:
            self.param_file = ParamFile()
            self.param_file.load(self.study_path)
            #Include build.sh by default
            self.param_file["FILES"].append({"path": ".", "files": ["build.sh"]})
        except Exception as error:
            raise Exception(error)
            

    def _get_params_by_mode(self,  mode):
        params_out = []
        for k, v in self.params.items():
            if v["mode"] == mode:
                param = v.copy()
                param["name"] = k
                params_out.append(param)
        return params_out

           
    def _check_linear_params(self):
        max_param_size = max([len(p["value"]) for p in self.linear_param_list])
        multivalued_params = []
        for p in self.linear_param_list:
            if len(p["value"]) == 1:
                p["value"] = p["value"] * max_param_size
            elif len(p["value"]) == max_param_size:
                multivalued_params.append(p)
            elif len(p["value"]) != max_param_size:
                raise Exception("Error: All linear params lists must be same size or one.")
        self.linear_param_size = max_param_size
        return multivalued_params


    def _gen_comb_instance(self, instance, params):
        if params:
            pname = params[0]["name"]
            for pval in params[0]["value"]:
                param = {pname: pval}
                instance_copy = instance.copy()
                instance_copy.update(param)
                self._gen_comb_instance(instance_copy, params[1:])
        else:
            self.instance_counter += 1
            self._create_instance(instance)
            multival_params = self._get_multival_params(instance)
            self.study_file.add_instance(multival_params, short_name=self.short_name)
    
    #TODO: Create a file with instance information
    def _create_instance(self, instance):
        multival_params = self._get_multival_params(instance)
        self.instance_name = instance_dirstring(self.instance_counter, multival_params,
                                                   self.nof_instances, self.short_name)
        self.print_msg("Creating instance '%s'..." % self.instance_name, verbose=True)
        casedir = os.path.join(self.study_path, self.instance_name)
        studydir = os.path.dirname(casedir)
        shutil.copytree(self.template_path, casedir)
        try:
            self._create_instance_infofile(instance)
            # Create paths for files listed for replace params on them
            file_paths =  []
            for path in self.param_file["FILES"]:
                for f in path["files"]:
                    p = os.path.join(os.path.join(self.study_path, self.instance_name), path["path"])
                    p = os.path.join(p, f)
                    file_paths.append(p)
            # Add parampy specific params
            params = {"PARAMPY-CN": self.instance_name,
                      "PARAMPY-SN": self.param_file["STUDY"]["name"],
                      "PARAMPY-CD": casedir, 
                      "PARAMPY-SD": studydir}
            params.update(instance)
            replace_placeholders(file_paths, params)
            if not self.build_once:
                # Force execution permissions to 'build.sh'
                self.print_msg("--|Building...", verbose=True, end="")
                build_script_path = os.path.join(casedir, "build.sh")
                os.chmod(build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(build_script_path).st_mode))
                self.execute_build_script(build_script_path)
                self.print_msg("Done", verbose=True)
        except Exception as error:
            shutil.rmtree(casedir)
            raise error

    #TODO: Decouple state and behaviour of instances into a new class
    def generate_instances(self):
        instance = {}
        self.instance_counter = 0
        # Check if build.sh has to be run before generating the instances
        self.print_msg("Generating instances...")
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

        for _ in xrange(self.linear_param_size):
            for lp in self.linear_param_list:
                param = {lp["name"]: lp["value"][_]}
                instance.update(param)
            self._gen_comb_instance(instance, self.combinatoric_param_list)
            instance = {}
        self.study_file.write()
        self.print_msg("Success: Created %d cases." % self.nof_instances)

    def _get_multival_params(self, instance):
        return {k:v for k,v in instance.items() if k in self.multival_keys}

    @classmethod 
    def create_dir_structure(cls, path, study_name):
        study_path = os.path.join(path, study_name)
        study_template_path = os.path.join(study_path, "template")
        if not os.path.exists(study_path):
            os.makedirs(study_path)
            os.makedirs(study_template_path)
            for directory in cls.DEFAULT_DIRECTORIES:
                directory_path = os.path.join(study_path, directory) 
                if not os.path.exists(directory_path):
                    os.makedirs(directory_path)
            for f in cls.DEFAULT_FILES:
                shutil.copy(os.path.join(DEFAULTS_DIR, os.path.basename(f)), 
                            os.path.join(study_path, f))
        else:
            raise Exception("Directory '%s' already exists!" % study_path)
        print "SUCCESS: Created study '%s'." % study_name 

# Look for a remote. First look in the ConfigDir. If not present 
def opts_get_remote(abs_remote_path, args):
    r = remote.Remote()
    remote_yaml_path = os.path.join(abs_remote_path, "remote.yaml")
    print remote_yaml_path
    try:
        r.load(remote_yaml_path, args.remote)
    except IOError as error:
        sys.exit("Error: File 'remote.yaml' not found in study directory.")
    return r


if __name__ == "__main__":
    import argparse
    # config_dir = ConfigDirectory()
    parser = argparse.ArgumentParser(description="Program to generate parameter studies.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False)
    group.add_argument("-q", "--quiet", action="store_true", default=False)
    actions_group = parser.add_mutually_exclusive_group()
    actions_group.add_argument("-c", "--create", metavar="study_name", help="Create an empty study template.")
    actions_group.add_argument("-g", "--generate", nargs="?", metavar="study_name", const=".", help="Generate the instances of the study based on the 'params.yaml' file.")
    actions_group.add_argument("--check", action="store_true", 
			help="Check if the study is consistent with 'params.yaml' file.")
    actions_group.add_argument("--clean", nargs="?", metavar="cases_list", const='*', help="Clean the instances of a study.")
    actions_group.add_argument("--erase", nargs="?", metavar="cases_list", const=".", help="Erase the instances of a study.")
    actions_group.add_argument("-i", "--info", action="store_true", help="Get a resumed info of the study.")
    actions_group.add_argument("-u", "--upload-case", metavar="case_name", help="Upload case to remote.")
    actions_group.add_argument("-U", "--upload-study", nargs="?", const=".", metavar="study_name", help="Upload study to remote.")
    actions_group.add_argument("-s", "--submit-case", metavar="case_name", help="Submit case to execution.")
    actions_group.add_argument("-S", "--submit-study", nargs="?", const=".", metavar="study_name", help="Submit case to execution.")
    actions_group.add_argument("-d", "--download-case", metavar="case_name", help="Download case from remote.")
    actions_group.add_argument("-D", "--download-study", nargs="?", const=".", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("-l", "--status", nargs="?", const=".", metavar="study_name", help="Download study from remote.")
    parser.add_argument("--shortname", action="store_true", default=False, help="Study instances are short named.")
    parser.add_argument("--cases", metavar="case_indices", help="Case selector.")
    parser.add_argument("--array-job", action="store_true", default=False, help="Submit the study as a array of jobs.")
    parser.add_argument("--remote", nargs="?", const=None, metavar="remote_name", help="Specify remote for an action.")
    parser.add_argument("--force", action="store_true", default=False, help="Specify remote for an action.")
    parser.add_argument("--paramfile", metavar="file_name", help="Specify path to paramfile.")
    parser.add_argument("--build-once", action="store_true", default=False, help="Study instances are short named.")
    parser.add_argument("--debug", action="store_true", default=False, help="Debug mode.")
    args = parser.parse_args()


    if args.create:
        study_name = args.create
        study_path = os.path.dirname(os.path.abspath(study_name))
        try:
            StudyBuilder.create_dir_structure(study_path, study_name)
        except Exception as error:
            if args.debug:
                raise
            else:
                sys.exit(error)

    elif args.generate:
        study_name = args.generate
        study_path = os.path.abspath(study_name)
        if not ParamFile.paramfile_exists(study_path):
            sys.exit("Error:\nParameter file 'params.yaml' do not exists!")
        if not StudyBuilder.templatedir_exists(study_path):
            sys.exit("Error:\nTemplate directory do not exists!")
        try:
            study =  StudyBuilder(study_path, short_name=args.shortname,
                                  build_once=args.build_once, quiet=args.quiet,
                                  verbose=args.verbose)
            study.generate_instances()
        except Exception as error:
            if args.debug:
                raise
            else:
                sys.exit(error)

    elif args.clean:
        study_name = "."
        study_path = os.path.abspath(study_name)
        try:
            StudyBuilder.clean_study(study_path, args.clean)
        except Exception as error:
            if args.debug:
                raise
            else:
                sys.exit(error)

    elif args.erase:
        study_path = os.path.abspath(args.erase)
        try:
            StudyBuilder.erase_study(study_path)
        except Exception as error:
            if args.debug:
                raise
            else:
                sys.exit(error)

    elif args.upload_case:
        abs_remote_path = os.path.abspath(args.upload_case)
        r = opts_get_remote(os.path.dirname(abs_remote_path), args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, case_path=abs_remote_path)
        try:
            sm.upload_case(force=args.force)
        except Exception as error:
            r.close()
            sys.exit(error)
        r.close()

    elif args.upload_study:
        abs_remote_path = os.path.abspath(args.upload_study)
        r = opts_get_remote(abs_remote_path, args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, study_path=abs_remote_path)
        case_selector = args.cases
        cases_idx = _decode_case_selector(case_selector)
        try:
            sm.upload_study(cases_idx=cases_idx, array_job=args.array_job, force=args.force)
        except Exception as error:
            r.close()
            sys.exit(error)
        r.close()

    elif args.submit_case:
        abs_remote_path = os.path.abspath(args.submit_case)
        r = opts_get_remote(os.path.dirname(abs_remote_path), args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, case_path=abs_remote_path)
        try:
            sm.submit_case()
        except Exception as error:
            r.close()
            sys.exit(error)
        r.close()

    elif args.submit_study:
        abs_remote_path = os.path.abspath(args.submit_study)
        r = opts_get_remote(abs_remote_path, args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, study_path=abs_remote_path)
        sm.submit_study(force=args.force)
        # try:
        #     sm.submit_study()
        # except Exception as error:
        #     r.close()
        #     sys.exit(error)
        r.close()

    elif args.download_study:
        abs_remote_path = os.path.abspath(args.download_study)
        r = opts_get_remote(abs_remote_path, args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, study_path=abs_remote_path)
        sm.download_study(force=args.force)
        # try:
        #     sm.download_study()
        # except Exception as error:
        #     r.close()
        #     sys.exit(error)
        r.close()

    elif args.status:
        abs_remote_path = os.path.abspath(args.status)
        r = opts_get_remote(abs_remote_path, args)
        if r.available():
            passwd = getpass.getpass("Password: ")
        else:
            sys.exit("Error: Remote '%s' not available." % r.name)
        try:
            r.connect(passwd)
        except Exception as error:
            sys.exit(error)
        sm = remote.StudyManager(r, study_path=abs_remote_path)
        sm.update_status()
        # try:
        #     sm.status()
        # except Exception as error:
        #     r.close()
        #     sys.exit(error)
        r.close()








