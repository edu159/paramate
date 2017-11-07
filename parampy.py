import yaml
import os
import glob
import shutil
import re
import sys

SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")

class StudySection:
    pass
class ExecSection:
    pass
class PostprocSection:
    pass
class BuildSection:
    pass

#TODO: Decouple allowed sections from Param file to make it general
class ParamFile:
    def __init__(self, allowed_sections=None):
        self.ALLOWED_SECTIONS = {"STUDY": StudySection, 
                                 "EXEC": ExecSection,
                                 "POSTPROC": PostprocSection,
                                 "BUILD": BuildSection}
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
        #print self.params_data
        self.params_fname = fname
        self.loaded = True

    def _load_sections(self):
        for section_name, section_opts in  self.params_data.items():
            try:
                section_class =  self.ALLOWED_SECTIONS[section_name]
                self.sections[section_name] = section_class(section_name, section_opts)
            except Exception as error:
                print "Error: section not found ", error 
            
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
        self.build_params = []
        self.exec_params = []
        self.build_params = self._build_param_list("BUILD")
        self.linear_param_size = 0
        self.exec_params = self._build_param_list("EXEC")
        self.linear_params = self._get_params_by_mode(self.build_params + 
                                                      self.exec_params, 
                                                      "linear")
        self.combinatoric_params = self._get_params_by_mode(self.build_params +
                                                            self.exec_params,
                                                            "combinatoric")
        self._check_params_validity()
        self.nof_instances = self._compute_nof_instances()

    def _compute_nof_instances(self):
        nof_instances = 1
        if self.linear_params:
            nof_instances = len(self.linear_params[0]["value"])
        for param in self.combinatoric_params:
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
            

    def _get_params_by_mode(self, params, mode):
        return [p for p in params if p["mode"] == mode]

    def _build_param_list(self, section):
        params_out = []
        for f in self.param_file[section]["files"]:
            params  = list(f["params"])
            for p in params:
                p.update({"filename": f["name"]})
                p.update({"section": section.lower()})
            params_out.extend(params)
        return params_out

            
    def _check_params_validity(self):
        param_size = len(self.linear_params[0]["value"])
        for p in self.linear_params[1:]:
            p_size = len(p["value"])
            if p_size != param_size:
                raise Exception("All linear style param values list should have the same size.")
        self.linear_param_size = param_size


    def _gen_comb_instance(self, instance, params):
        if params:
            for val in params[0]["value"]:
                param = params[0].copy()
                param.pop("mode")
                param["value"] = val
                self._gen_comb_instance(instance + [param], params[1:])
        else:
            self.instance_counter += 1
            self._create_instance(instance)

    #TODO: Create a file with instance information
    def _create_instance(self, instance):
        dirname = self._build_instance_string(instance)
        shutil.copytree("template", dirname)
        self._create_instance_infofile(instance)
        self._replace_placeholders(dirname, instance)


    def _replace_placeholders(self, dirname, instance):
        # Find files to modify. Append name of the section as the parent folder.
        files = reduce(lambda r, d: r.update({os.path.join(dirname, d["section"], d["filename"]):{}}) or r, instance, {})
        # Add param:value pairs
        for param in instance:
            fname = os.path.join(dirname, param["section"], param["filename"])
            files[fname].update({param["name"]: param["value"]})

        for fname, params in files.items():
            try:
                lines = []
                with open(fname, 'r') as placeholder_file:
                    lines = placeholder_file.readlines()                                                                                                                                                                                                 
                    for ln, line in enumerate(lines):
                        line_opts = set(re.findall(r'\$\[([a-zA-Z0-9\-]+?)\]', line))
                        for opt in line_opts:
                            try:
                               lines[ln] = lines[ln].replace("$[" + opt + "]", str(params[opt]))
                            except KeyError as error:
                                # All placeholders has to be replaced and must be in params.
                                raise Exception("Parameter '%s' not present." % opt)
                with open(fname, 'w+') as replaced_file:
                    replaced_file.writelines(lines)

             
            except Exception as error:
                print "ENTRO"
                print error
            #print "".join(lines)

    #TODO: Decouple state and behaviour of instances into a new class
    def generate_instances(self):
        instance = []
        self.instance_counter = 0
        for _ in xrange(self.linear_param_size):
            for lp in self.linear_params:
                param = lp.copy()
                param.pop("mode")
                param["value"] = param["value"][_]
                instance.append(param)

            self._gen_comb_instance(instance, self.combinatoric_params)
            instance = []
        
    def _build_instance_string(self, instance):
        nof_figures = len(str(self.nof_instances))
        instance_string = "%0*d" % (nof_figures, self.instance_counter)
        
        if not self.short_name:
            for param in instance:
                instance_string += "_%s%s" % (param["name"], param["value"])
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


if __name__ == "__main__":
    import argparse

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
    parser.add_argument("--shortname", action="store_true", default=False, help="Study instances are short named.")
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
        try:
            study =  StudyBuilder(".", short_name=args.shortname)
        except Exception as error:
            sys.exit(error)
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
        



    else:
        pass
    #study =  StudyBuilder("study_test")
    #study.generate_instances()
