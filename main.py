import yaml
import os
import glob
import shutil

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

    def load(self, fname="params.yaml"):
        try:
            with open(fname, 'r') as paramfile:
                self.params_data = yaml.load(paramfile)
        except Exception as error:
            print "Error in parameters file:"
            print "\t", error
        self._load_sections()
        print self.params_data
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



class StudyBuilder:
    DEFAULT_DIRECTORIES = ["build", "exec", "output", "postproc"]
    DEFAULT_FILES = ["exec.sh", "build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, param_file, only_one=False, long_name=False):
        self.param_file = param_file
        self.build_params = param_file[

    def _check_params_validity():
        pass

    def _get_next_instance():
        pass

    def _build_name_string():
        pass

    def generate_instances():
        while (_get_next_instance is not None):
            pass


    @classmethod 
    def create_dir_structure(cls, path, study_name):
        study_path = os.path.join(path, study_name)
        if not os.path.exists(study_path):
            os.makedirs(study_path)
            for directory in cls.DEFAULT_DIRECTORIES:
                directory_path = os.path.join(study_path, directory) 
                if not os.path.exists(directory_path):
                    os.makedirs(directory_path)

            os.chdir(DEFAULTS_DIR)
            for f in glob.glob("*"):
                shutil.copyfile(os.path.join(DEFAULTS_DIR, f), 
                                os.path.join(study_path, f))
        else:
            print "Error: Directory could not be created!"


class ConfigSection:
    def __init__(self, section_name, section_opts, required_opts):
        self.section_name = section_name
        self.section_opts = section_opts
        self.required_opts = required_opts
        self._check_opts()

    def _check_opts(self):
        def _check_opts_recursive(opts_dict, opts_types_dict):
            for opt_name, opt_val in opts_dict.items():
                try:
                    types = opts_types_dict[opt_name]
                except Exception as error:
                    print "Option not found: ", error
                if type(types) is dict:
                    _check_opts_recursive(opts_dict[opt_name], opts_types_dict[opt_name])
                elif type(types) is tuple:
                    if type(opt_val) not in types:
                        print "Type not correct in option ", opt_name, " value: ", opt_val
                elif type(types) is str and opt_val not in types.split('|'):
                    print "Type incorrect in option ", opt_name, " value: ", opt_val
        _check_opts_recursive(self.section_opts, self.required_opts)

if __name__ == "__main__":
    folder = "./"
    param_file = ParamFile()
    param_file.load()
    StudyBuilder.create_dir_structure("/home/eduardo/Desktop/repositories/parampy", "study_test")
