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
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "template/README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, param_file, only_one=False, long_name=False):
        self.only_one = only_one
        self.long_name = long_name
        self.param_file = param_file
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

    def _get_params_by_mode(self, params, mode):
        return [p for p in params if p["mode"] == mode]

    def _build_param_list(self, section):
        params_out = []
        for f in self.param_file[section]["files"]:
            params  = list(f["params"])
            for p in params:
                p.update({"filename": f["name"]})
            params_out.extend(params)
        return params_out

            
    def _check_params_validity(self):
        param_size = len(self.linear_params[0]["value"])
        for p in self.linear_params[1:]:
            p_size = len(p["value"])
            if p_size != param_size:
                raise Exception("All linear style param values list should have the same size.")
        self.linear_param_size = param_size

    def _build_name_string():
        pass

    def _gen_comb_instance(self, instance, params):
        if params:
            for val in params[0]["value"]:
                param = params[0].copy()
                param.pop("mode")
                param["value"] = val
                self._gen_comb_instance(instance + [param], params[1:])
        else:
            self._create_instance(instance)
            print instance

    def _create_instance(self, instance):
        pass

        

    def generate_instances(self):
        instance = []
        for _ in xrange(self.linear_param_size):
            for lp in self.linear_params:
                param = lp.copy()
                param.pop("mode")
                param["value"] = param["value"][_]
                instance.append(param)

            self._gen_comb_instance(instance, self.combinatoric_params)
            instance = []
        
    def _build_instance_string(self, instance):
        instance_string = ""
        if self.long_name:
            for param in instance:
                instance_string += "_%s%s" % (param["name"], param["value"])
            instance_string = instance_string[1:]
        else:
            pass

            
            


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
    StudyBuilder.create_dir_structure("/home/edu/Desktop/repositories/parampy", "study_test")
    study =  StudyBuilder(param_file)
    study.generate_instances()
     

