import yaml
import os
import glob
import shutil
import re
import sys
import remote
import getpass

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")

class ConfigDirectory:
    def __init__(self):
        self.default_dir = os.path.join(os.getenv("HOME"), ".parampy")
        self.default_remotes_dir = os.path.join(self.default_dir, "remotes")

class StudySection:
    pass
class ParamsSection:
    pass
class FilesSection:
    pass

#TODO: Decouple allowed sections from Param file to make it general
class ParamFile:
    def __init__(self, allowed_sections=None):
        self.ALLOWED_SECTIONS = {"STUDY": StudySection, 
                                 "PARAMETERS": ParamsSection,
                                 "FILES": FilesSection}
        self.params_fname = ""
        self.loaded = False
        self.params_data = {}
        self.sections ={}

    @staticmethod
    def paramfile_exists(path):
        p = os.path.join(path, "params.yaml")
        return os.path.exists(p)
        

    def load(self, fname="params.yaml"):
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
                print "Error: Section '%s' is mandatory in 'params.yaml'." % section_name
            
    def add_section(self, section, opts):
        self.config_data[section] = opts

    def __getitem__(self, key):
        if self.loaded:
            return self.params_data[key]
        else:
            raise Exception()


class StudyBuilder:
    DEFAULT_DIRECTORIES = ["template/build", "template/exec", "template/output", "template/postproc"]
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, study_case_path, only_one=False, short_name=False):
        #TODO: Check if the study case directory is empty and in good condition
        self.study_case_path = study_case_path
        os.chdir(study_case_path)
        self.only_one = only_one
        self.short_name = short_name
        self.instance_counter = 0
        self.param_file = None
        self._load_config_file()
        # self.build_params = self._build_param_list("BUILD")
        # self.params = self._build_param_list()
        self.params = self.param_file["PARAMETERS"]
        self.linear_param_size = 0
        # self.exec_params = self._build_param_list("EXEC")
        self.linear_param_list = self._get_params_by_mode("linear")
        self.combinatoric_param_list = self._get_params_by_mode("combinatoric")
        linear_multival_param_list = self._check_linear_params()
        self.nof_instances = self._compute_nof_instances()
        self.multival_param_list = linear_multival_param_list + self.combinatoric_param_list
        self.manifest_lines = []

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
    def clean_study():
        try:
            for f in glob.glob("./[0-9]*"):
                shutil.rmtree(f)
        except Exception as error:
            raise Exception("Error:\n" + str(error))


    def _create_instance_infofile(self, instance):
        f = os.path.join(self._build_instance_string(instance), "instance.info")
        open(f, 'a').close()

    def _load_config_file(self):
        try:
            self.param_file = ParamFile()
            self.param_file.load()
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

    # def _build_param_list(self):
    #     params_out = []
    #     # Sections can be not present
    #     try:
    #         for p_name, p_values in self.param_file["PARAMETERS"].items():
    #             new_p = {}
    #             new_p.update(p_values)
    #             new_p.update({"name": p_name})
    #             params_out.append(new_p)
    #     except KeyError:
    #         pass
    #     # print params_out
    #     return params_out
    #
            
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
            for val in params[0]["value"]:
                param = params[0].copy()
                param.pop("mode")
                param["value"] = val
                self._gen_comb_instance(instance.update(param), params[1:])
        else:
            self.instance_counter += 1
            self._create_instance(instance)
            self._add2manifest(instance)

    def _add2manifest(self, instance):
        self.manifest_lines.append("%s : [CREATED]\n" % self._build_instance_string(instance))

    #TODO: Create a file with instance information
    def _create_instance(self, instance):
        dirname = self._build_instance_string(instance)
        shutil.copytree("template", dirname)
        self._create_instance_infofile(instance)
        self._replace_placeholders(dirname, instance)


    def _replace_placeholders(self, dirname, instance):
        # Find files to modify. Append name of the section as the parent folder.
        # files = reduce(lambda r, d: r.update({os.path.join(dirname, d["section"], d["filename"]):{}}) or r, instance, {})
        # for param in instance:
        #     fname = os.path.join(dirname, param["section"], param["filename"])
        #     files[fname].update({param["name"]: param["value"]})
        file_paths =  []
        build_string = self._build_instance_string(instance)
        for path in self.param_file["FILES"]:
            for f in path["files"]:
                p = os.path.join(os.path.abspath(build_string), path["path"])
                p = os.path.join(p, f)
                file_paths.append(p)

        # print file_paths
        # Parampy params useful to build paths.
        parampy_params = {"PARAMPY-CASENAME": self._build_instance_string(instance),
                          "PARAMPY-STUDYNAME": self.param_file["STUDY"]["name"]}
        for path in file_paths:
            try:
                lines = []
                with open(path, 'r') as placeholder_file:
                    lines = placeholder_file.readlines()                                                                                                                                                                                                 
                for ln, line in enumerate(lines):
                    line_opts = set(re.findall(r'\$\[([a-zA-Z0-9\-]+?)\]', line))
                    # print path, line_opts
                    for opt in line_opts:
                        try:
                            lines[ln] = lines[ln].replace("$[" + opt + "]", str(instance[opt]))
                        except KeyError as error:
                            try:
                                lines[ln] = lines[ln].replace("$[" + opt + "]", str(parampy_params[opt]))
                            except KeyError as error:
                                # All placeholders has to be replaced and must be in params.
                                raise Exception("Parameter '%s' not present." % opt)
                with open(path, 'w+') as replaced_file:
                    replaced_file.writelines(lines)

             
            except Exception as error:
                raise error
                # print error
            #print "".join(lines)

    #TODO: Decouple state and behaviour of instances into a new class
    def generate_instances(self):
        instance = {}
        self.instance_counter = 0
        for _ in xrange(self.linear_param_size):
            for lp in self.linear_param_list:
                param = {lp["name"]: lp["value"][_]}
                instance.update(param)
            self._gen_comb_instance(instance, self.combinatoric_param_list)
            instance = {}

        with open("cases.txt", 'w') as manifest_file:
            manifest_file.writelines(self.manifest_lines)
            
    def _build_instance_string(self, instance):
        print instance
        nof_figures = len(str(self.nof_instances))
        instance_string = "%0*d" % (nof_figures, self.instance_counter)
        multivalued_keys = [d["name"] for d in self.multival_param_list] 
        if not self.short_name:
            for pname, pval in instance.items():
                if pname in multivalued_keys:
                    instance_string += "_%s%s" % (pname, pval)
        return instance_string

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

def opts_get_remote(abs_remote_path, args):
    r = remote.Remote()
    if args.remote:
        remote_yaml_path = os.path.join(config_dir.default_remotes_dir, args.remote + ".yaml")
        try:
            r.load(remote_yaml_path)
        except Exception as error:
            sys.exit("Error: Remote '%s' not found." % args.remote)
            
    else:
        remote_yaml_path = os.path.join(abs_remote_path, "remote.yaml")
        try:
            r.load(remote_yaml_path)
        except Exception as error:
            try:
                r.load(remote_yaml_path)
                # TODO: default remote here instead of repeating this
            except Exception:
                sys.exit("Error: File 'remote.yaml' not found and default remote not defined.")
    return r


if __name__ == "__main__":
    import argparse
    config_dir = ConfigDirectory()
    parser = argparse.ArgumentParser(description="Program to generate parameter studies.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true")
    group.add_argument("-q", "--quiet", action="store_true")
    actions_group = parser.add_mutually_exclusive_group()
    actions_group.add_argument("-c", "--create", metavar="study_name", help="Create an empty study template.")
    actions_group.add_argument("-g", "--generate", action="store_true", help="Generate the instances of the study based on the 'params.yaml' file.")
    actions_group.add_argument("--check", action="store_true", 
			help="Check if the study is consistent with 'params.yaml' file.")
    actions_group.add_argument("--clean", action="store_true", help="Clean the instances of the study.")
    actions_group.add_argument("-i", "--info", action="store_true", help="Get a resumed info of the study.")
    actions_group.add_argument("--createremote", action="store_true", help="Create a remote template.")
    actions_group.add_argument("--addremote", action="store_true", help="Add a remote.")
    actions_group.add_argument("--delremote", action="store_true", help="Delete a remote.")
    actions_group.add_argument("--listremote", action="store_true", help="List all saved remotes.")
    actions_group.add_argument("-u", "--upload-case", metavar="case_name", help="Upload case to remote.")
    actions_group.add_argument("-U", "--upload-study", metavar="study_name", help="Upload study to remote.")
    actions_group.add_argument("-s", "--submit-case", metavar="case_name", help="Submit case to execution.")
    actions_group.add_argument("-S", "--submit-study", metavar="study_name", help="Submit case to execution.")

    parser.add_argument("--shortname", action="store_true", default=False, help="Study instances are short named.")
    parser.add_argument("--remote", metavar="remote_name", help="Study instances are short named.")
    args = parser.parse_args()

    if args.quiet:
	print "quiet"
    elif args.verbose:
        print "verbose"
    elif args.create:
        study_name = args.create
        try:
            StudyBuilder.create_dir_structure("/home/eduardo/Desktop/repositories/parampy", study_name)
        except Exception as error:
            sys.exit(error)
    elif args.generate:
        if not ParamFile.paramfile_exists("."):
            sys.exit("Error:\nParameter file 'params.yaml' do not exists!")
        if not StudyBuilder.templatedir_exists("."):
            sys.exit("Error:\nTemplate directory do not exists!")

        study =  StudyBuilder(".", short_name=args.shortname)
        # try:
        #     study =  StudyBuilder(".", short_name=args.shortname)
        # except Exception as error:
        #     sys.exit(error)
        study.generate_instances()
    elif args.clean:
        if not ParamFile.paramfile_exists("."):
            sys.exit("Error:\nParameter file 'params.yaml' do not exists!")
        if not StudyBuilder.templatedir_exists("."):
            sys.exit("Error:\nTemplate directory do not exists!")
        try:
            StudyBuilder.clean_study()
        except Exception as error:
            sys.exit(error)
    elif args.createremote:
        remote = remote.Remote()
        try:
            remote.create_remote_template(".")
        except Exception as error:
            sys.exit(error)

    elif args.upload_case:
        r = remote.Remote()
        abs_remote_path = os.path.abspath(args.upload_case)
        r = opts_get_remote(abs_remote_path, args)
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
            sm.upload_case()
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
        try:
            sm.upload_study()
        except Exception as error:
            r.close()
            sys.exit(error)
        r.close()


        
