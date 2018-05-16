#!/usr/bin/env python2
import time
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
                        raise Exception("Parameter '%s' not defined in 'params.yaml' (Found in '%s')." % (opt, os.path.basename(path)))
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
    def __init__(self, path='.', allowed_sections=None, fname='params.yaml'):
        self.ALLOWED_SECTIONS = {"STUDY": StudySection, 
                                 "PARAMETERS": ParamsSection,
                                 "DOWNLOAD": DownloadSection,
                                 "FILES": FilesSection}
        self.study_path = os.path.abspath(path)
        self.fname = fname
        self.path = os.path.join(self.study_path, fname)
        self.loaded = False
        self.params_data = {}
        self.sections ={}

    def load(self):
        try:
            with open(self.path, 'r') as paramfile:
                self.params_data = yaml.load(paramfile)
        except IOError as e:
            raise Exception("Problem opening 'params.yaml' file - %s." % e.strerror)
        except Exception as error:
            raise Exception("Parsing error in 'params.yaml': \n" + str(error))
        self._load_sections()
        cases = []
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
                    # Insert study path to load generators
                    sys.path.insert(0, self.study_path)
                    import generators
                except Exception as err:
                    pass
                for param_name, param_fields in section_opts.items():
                    if isinstance(param_fields, dict):
                        if str(param_fields["value"]).startswith("g:"):
                            gen_name = param_fields["value"].split(":")[1]
                            try:
                                param_fields["value"] = getattr(generators, gen_name)()
                            except AttributeError as error:
                                raise Exception("Generator '%s' not found in 'generators.py'.")
                            except Exception as error:
                                raise Exception("Error in 'genenerators.py(%s)' - %s" %  (param_name, str(error)))
                            if type(param_fields["value"]) is not list:
                                raise Exception("Generators must return a list of values. Got '%s'."\
                                                % type(param_fields["value"]))
                    else:
                        #NOTE: This could be refactored, common code here
                        if str(param_fields).startswith("g:"):
                            gen_name = param_fields.split(":")[1]
                            try:
                                param_fields = getattr(generators, gen_name)
                            except AttributeError as error:
                                raise Exception("Generator '%s' not found in 'generators.py'.")
                            except Exception as error:
                                raise Exception("Error in 'genenerators.py - '" + str(error))
                        section_opts[param_name] = {"value": [param_fields], "mode": "linear"} 
                # Remove the path 
                del sys.path[0]

            
    #TODO: Add this to the specific DownloadSection object
    def get_download_paths(self, case):
        path_list = []
        case_path = os.path.join(self.study_path, case.name)
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
            raise Exception("File 'params.yaml' not loaded.")

JOB_STATES = ["CREATED", "UPLOADED", "SUBMITTED", "FINISHED", "DOWNLOADED"]

class Case:
    def __init__(self, id=None, params=None, name=None, short_name=False,
                 job_id=None, status="CREATED", submission_date=None, remote=None): 
        self.id = id
        self.params = params 
        self.short_name = short_name
        self.name = name
        self.job_id = job_id
        self.status = status
        self.submission_date = submission_date
        self.remote = remote
        self.creation_date = time.strftime("%c")

    def init_from_dict(self, attrs):
        for key in attrs:
            setattr(self, key, attrs[key])

    def reset(self):
        self.job_id = None
        self.status = "CREATED"
        self.sub_date = None
        self.remote = None

    def __getitem__(self, key):
        return self.__dict__[key]


class InfoFile:
    def __init__(self, path='.', fname="cases.info"):
        self.fname = fname
        self.file_path = os.path.join(os.path.abspath(path), fname)
        self.loaded = False

    def backup(self, dest):
        shutil.copy(self.file_path, os.path.join(dest, "cases.info.bak"))

    def restore(self, orig):
        shutil.copy(os.path.join(orig, "cases.info.bak"), self.file_path)
    
    
    def load(self):
        cases = []
        try:
            with open(self.file_path, 'r') as rfile:
                json_data = rfile.read()
                json_data = json.loads(json_data)
        except IOError as e:
            raise Exception("Problem opening 'cases.info' file - %s." % e.strerror)
        for case_dict in json_data["cases"]:
            c = Case()
            c.init_from_dict(case_dict)
            cases.append(c)
        self.loaded = True
        return cases

    def remove(self):
        os.remove(self.file_path)

    def save(self, cases):
        json_data = {"cases" : []}
        with open(self.file_path, 'w') as wfile:
            for i, case in enumerate(cases):
                json_data["cases"].append(case.__dict__)
            wfile.write(json.dumps(json_data, indent=4, sort_keys=True))


class Study:
    def __init__(self, name, path):
        self.path = path
        self.name = name
        self.study_file = InfoFile(path=path) 
        self.param_file = ParamFile(path=path)
        self.param_file.load()
        self.cases = []
        self.case_selection = []
        self.nof_cases = 0

    def get_cases(self, search_vals, field, sortby=None):
        if sortby == None:
            match_list = []
        else:
            match_list = {}
        for case in self.cases:
            if case[field] in search_vals:
                if sortby == None:
                    match_list.append(case)
                else:
                    try:
                        match_list[case[sortby]].append(case)
                    except KeyError:
                        match_list[case[sortby]] = []
                        match_list[case[sortby]].append(case)
        return match_list


    def load(self):
        self.cases = self.study_file.load()
        self.case_selection = self.cases
        self.nof_cases = len(self.cases)

    def save(self):
        self.study_file.save(self.cases)

    def clean(self):
        for case in self.case_selection:
            case.reset()
            d = self.param_file.get_download_paths(case)
            # print d
            #TODO: Remove submit.sh from case
            #TODO: Remove files for real
            # shutil.rmtree()
        self.save()

    def set_selection(self, cases_idx):
        self.case_selection = self.get_cases(cases_idx, "id")

    def delete(self):
        for case in self.case_selection:
            try:
                shutil.rmtree(os.path.join(self.path, case["name"]))
            except Exception as error:
                pass
        self.study_file.remove()

    def add_case(self, case_name, params, short_name=False):
        case = Case(self.nof_cases, params.copy(), case_name, short_name)
        self.cases.append(case)
        self.nof_cases += 1


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
        self.params = self.study.param_file["PARAMETERS"]
        #Include build.sh to files to replace placeholders
        self.study.param_file["FILES"].append({"path": ".", "files": ["build.sh"]})
        self.linear_param_size = 0
        self.linear_param_list = self._get_params_by_mode("linear")
        self.combinatoric_param_list = self._get_params_by_mode("combinatoric")
        linear_multival_param_list = self._check_linear_params()
        self.multival_param_list = linear_multival_param_list + self.combinatoric_param_list
        self.multival_keys = [d["name"] for d in self.multival_param_list] 
        self.template_path = os.path.join(self.study.path, "template")
        self.build_script_path = os.path.join(self.template_path, "build.sh")
        self.instances = []

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

    # def _compute_nof_instances(self):
    #     nof_instances = 1
    #     if self.linear_param_list:
    #         nof_instances = len(self.linear_param_list[0]["value"])
    #     for param in self.combinatoric_param_list:
    #         nof_instances *= len(param["value"])
    #     return nof_instances
    #     
    

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
                self.print_msg("--|Building...", verbose=True, end="")
                build_script_path = os.path.join(casedir, "build.sh")
                os.chmod(build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(build_script_path).st_mode))
                self.execute_build_script(build_script_path)
                self.print_msg("Done", verbose=True)
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


    def _call_generators(self, instance):
        for pname, pval in instance.items():
            if callable(pval):
                try:
                    instance[pname] = pval(instance)
                except Exception as error:
                    raise Exception("Error in 'genenerators.py(%s)' - %s" %  (pname, str(error)))

        return instance

    #TODO: Decouple state and behaviour of instances into a new class
    def generate_cases(self):
        self._generate_param_combinations()
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
            instance = self._call_generators(instance)
            multival_params = self._get_multival_params(instance)
            instance_name = self._instance_directory_string(instance_id, multival_params,
                                                      nof_instances, self.short_name)
            self._create_instance(instance_name, instance)
            self.study.add_case(instance_name, multival_params, short_name=self.short_name)

        self.study.save()
        self.print_msg("Success: Created %d cases." % nof_instances)

    def _generate_param_combinations(self):
        instance = {}
        self.instances = []
        for _ in xrange(self.linear_param_size):
            for lp in self.linear_param_list:
                param = {lp["name"]: lp["value"][_]}
                instance.update(param)
            self._gen_comb_instance(instance, self.combinatoric_param_list)
            instance = {}

    def _gen_comb_instance(self, instance, params):
        if params:
            pname = params[0]["name"]
            for pval in params[0]["value"]:
                param = {pname: pval}
                instance_copy = instance.copy()
                instance_copy.update(param)
                self._gen_comb_instance(instance_copy, params[1:])
        else:
            self.instances.append(instance)
            # multival_params = self._get_multival_params(instance)
            # self.study.study_file.add_case(multival_params, short_name=self.short_name)
    



    def _get_multival_params(self, instance):
        return {k:v for k,v in instance.items() if k in self.multival_keys}

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
        r.available()
    except remote.ConnectionTimeout as error:
        sys.exit("Error: " + str(error))
    except Exception as error: 
        sys.exit("Error: " + str(error))
    return r

def connect(remote, passwd):
    try:
        remote.connect(passwd)
    except Exception as error:
        if args.debug:
            raise
        else:
            sys.exit("Error: " + str(error))




if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Program to generate parameter studies.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", default=False)
    group.add_argument("-q", "--quiet", action="store_true", default=False)
    actions_group = parser.add_mutually_exclusive_group()
    # actions_group.add_argument("--check", action="store_true", help="Check if the study is consistent with 'params.yaml' file.")
    # actions_group.add_argument("-u", "--upload-case", metavar="case_name", help="Upload case to remote.")
    # actions_group.add_argument("-s", "--submit-case", metavar="case_name", help="Submit case to execution.")
    # actions_group.add_argument("-d", "--download-case", metavar="case_name", help="Download case from remote.")
    actions_group.add_argument("--create", metavar="study_name", help="Create an empty study template.")
    actions_group.add_argument("--generate", nargs="?", metavar="study_name", const=".", help="Generate the instances of the study based on the 'params.yaml' file.")
    actions_group.add_argument("--init-remote", nargs="?", metavar="remote_name", const="default", help="Get a resumed info of the study.")
    actions_group.add_argument("--qstatus", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("--qdelete", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("--qsubmit", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    actions_group.add_argument("--info", action="store_true", help="Get a resumed info of the study.")
    actions_group.add_argument("--clean", nargs="?", metavar="cases_list", const='*', help="Clean the instances of a study.")
    actions_group.add_argument("--delete", action="store_true", help="Delete all the instances of a study.")
    actions_group.add_argument("--upload", nargs="?", const="*", metavar="study_name", help="Upload study to remote.")
    actions_group.add_argument("--submit", nargs="?", const="*", metavar="study_name", help="Submit case to execution.")
    actions_group.add_argument("--download", nargs="?", const="*", metavar="study_name", help="Download study from remote.")
    parser.add_argument("--shortname", action="store_true", default=False, help="Study instances are short named.")
    # parser.add_argument("--cases", default='*', metavar="case_indices", nargs="?", const="*", help="Case selector.")
    parser.add_argument("--array-job", action="store_true", default=False, help="Submit the study as a array of jobs.")
    parser.add_argument("--remote", nargs="?", const=None, metavar="remote_name", help="Specify remote for an action.")
    parser.add_argument("--force", action="store_true", default=False, help="Specify remote for an action.")
    parser.add_argument("--keep-on-error", action="store_true", default=False, help="Keep files in case of an error during generation.")
    parser.add_argument("--build-once", action="store_true", default=False, help="Study instances are short named.")
    parser.add_argument("--debug", action="store_true", default=False, help="Debug mode.")
    args = parser.parse_args()



    if args.create:
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

    elif args.generate:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        print study_path, study_name
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
                sys.exit("Error: " + str(error))

    elif args.clean:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        try:
            study = Study(study_name, study_path)
            study.load()
            case_selector = args.clean
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

    elif args.delete:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        try:
            study = Study(study_name, study_path)
            try:
                study.load()
            except Exception:
                sys.exit("File 'cases.info' not found. Nothing to delete.")
            print "Deleting %d cases..." % study.nof_cases
            print "Deleting file 'cases.info'..."
            study.delete()
            print "Done."
        except Exception as error:
            if args.debug:
                raise
            else:
                sys.exit("Error: " + str(error))

    elif args.upload:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        r = opts_get_remote(study_path, args.remote)
        passwd = getpass.getpass("Password(%s): " % r.name)
        connect(r, passwd)
        study = Study(study_name, study_path)
        study.load()
        case_selector = args.upload
        cases_idx = decode_case_selector(case_selector, study.nof_cases)
        study.set_selection(cases_idx)
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

    elif args.download:
        study_path = os.path.abspath('.')
        study_name = os.path.basename(study_path)
        study = Study(study_name, study_path)
        study.load()
        case_selector = args.download
        cases_idx = decode_case_selector(case_selector, study.nof_cases)
        study.set_selection(cases_idx)
        cases_remote = study.get_cases(cases_idx, "id", sortby="remote")
        print "[Selected %d cases to download]" % len(cases_idx)
        no_remote_cases = cases_remote.pop(None, None)
        if no_remote_cases is not None:
            print "Warning: %d selected cases has not been uploaded anywhere. Ignoring them..." % len(no_remote_cases)
        nof_remote_downloads = {}
        for remote_name, remote_cases in cases_remote.items():
            ucases = set(study.get_cases(["UPLOADED"], "status"))
            nof_uploaded = len(set(remote_cases).intersection(set(ucases)))  
            dcases = set(study.get_cases(["DOWNLOADED"], "status"))
            nof_downloaded = len(set(remote_cases).intersection(set(dcases)))  
            print "  -'%s': %d cases (Warning: %d downloaded, %d not submitted)" % (remote_name,\
                    len(remote_cases), nof_downloaded, nof_uploaded)
            nof_remote_downloads.update({remote_name: len(remote_cases) - (nof_downloaded + nof_uploaded)})
        opt = raw_input("Continue?['Y','y']:")
        if opt in ['y', 'Y']:
            for remote_name, remote_cases in cases_remote.items():
                r = opts_get_remote(study_path, remote_name)
                print "Downloading %d cases from remote '%s'..." % (nof_remote_downloads[remote_name], remote_name)
                passwd = getpass.getpass("Password(%s): " % remote_name)
                connect(r, passwd)
                sm = remote.StudyManager(study, quiet=args.quiet, verbose=args.verbose)
                try:
                    sm.update_status(r)
                    # Get cases in remote and "FINISHED" from the selection
                    fcases = set(study.get_cases(["FINISHED"], "status"))
                    new_selection = set(study.case_selection).intersection(remote_cases, fcases)
                    new_selection_idx = [c.id for c in new_selection]
                    study.set_selection(new_selection_idx)
                    sm.download(r, force=args.force)
                    sys.exit()
                except Exception as error:
                    if args.debug:
                        raise
                    else:
                        r.close()
                        sys.exit("Error:" + str(error))
                r.close()


    elif args.qsubmit:
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

    elif args.init_remote:
        if args.init_remote == "default":
            remote_name = None
        else:
            remote_name = args.init_remote
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
                sys.exit("Error: " + str(error))
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








