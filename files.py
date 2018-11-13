import time
import yaml
import stat
import os
import glob
import shutil
import re
import sys
import json
from anytree import Node, PreOrderIter
from anytree.importer import DictImporter
from study import Case
from UserDict import UserDict

class ParamInstance(UserDict):
    def __init__(self, initial_data={}):
        UserDict.__init__(self, initial_data)
        self.backtrace = []
        self.current_generator = None

    def resolve_params(self):
        for pname, pval in self.items():
            if callable(pval):
                try:
                    #TODO: Fix name of the generator.
                    self.current_generator = pval
                    self[pname] = pval(self)
                except Exception as error:
                    raise
                    # print "Error in 'genenerators.py(%s)':" %  pname
                    # raise error
                
    def __getitem__(self, key):
        try:
            item = UserDict.__getitem__(self, key)
        except KeyError:
            raise Exception("Parameter '{}' not found in generator '{}'.".format(key, self.current_generator.__name__))
        if callable(item):
            if key in self.backtrace:
                self.backtrace.append(key)
                decorated_backtrace = ["({})".format(call) for call in self.backtrace]
                bt_str = "->".join(decorated_backtrace)
                raise Exception("Error: Circular dependency of parameter '{}' found [{}]".format(key, bt_str))
            self.backtrace.append(key)
            self[key] = item(self) 
            # print "K:", key, "val:", item(key)
        return UserDict.__getitem__(self, key)



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

class Section(object):
    def __init__(self, sections, data, study_path, example_str, name):
        self.data = data
        self.example_str = example_str
        self.name = name
        self.study_path = study_path
        # Pointer to the ParamFile object to get access to other sections
        self.sections = sections
        self.load_priority = 0
        self.check = False
        self.load = False
        self._check()
        self._load()


    def _check(self):
        self.check = True

    def _load(self):
        self.load = True

    def _check_value_dict(self, field, v, vtype):
        actual_type = type(v)
        if type(vtype) != tuple: 
            allowed_types = (vtype, )
            type_str = vtype.__name__
        else:
            allowed_types = vtype
            type_str = "' or '".join([t.__name__ for t in allowed_types])
        if actual_type not in allowed_types:
            raise Exception(("Section '{}' invalid. Field '{}' must be of type: '{}', but got '{}' instead.\nExample:\n {}")\
                    .format(self.name, field, type_str, actual_type.__name__, self.example_str))

    def _check_value_list(self, v, vtype):
        if type(v) != vtype:
            raise Exception(("Section '{}' invalid. List element '{}' must be of type '{}' but got '{}' instead.\nExample:\n {}")\
                            .format(self.name, v, vtype.__name__, v, self.example_str))

    def _check_list(self, field, plist, elem_type):
        self._check_value_list(plist, list)
        for e in plist:
            self._check_value_list(e, elem_type)

    def _check_dict(self, field, pdict, allowed, value_type, required, mutual_exc=[]):
        self._check_value_dict(field, pdict, dict)

        # Check for mutual exclusive groups
        mutual_exc_sets = [set(g) for g in mutual_exc]
        for g in mutual_exc_sets:
            intersect = g.intersection(set(pdict.keys()))
            if len(intersect) > 1:
                raise Exception("Invalid combination of {} fields.\nExample:\n {}".format(tuple(intersect), self.example_str))

        # Check no other fields are present and type is correct
        for  k in pdict.keys():
            if k not in allowed:
                raise Exception("Invalid field '{}' in section '{}'.\nExample:\n {}"\
                                .format(k, self.name, self.example_str))
            else:
                i = allowed.index(k)
                data_t = value_type[i]
                self._check_value_dict(k, pdict[k], data_t)

        # Check all mandatory fields are present and types
        for k in required:
            if k not in pdict.keys():
                raise Exception("Required field '{}' not present in section '{}'.\nExample:\n {}"\
                                .format(k, self.name, self.example_str))



class StudySection(Section):
    def __init__(self, sections, data, study_path):
        example_str =  "STUDY:\n" +\
                       "    name: mystudy\n" +\
                       "    version: 1.1\n" +\
                       "    description: 'Amazing study.'"
        super(StudySection, self).__init__(sections, data, study_path, example_str, "STUDY")

    def _check(self):
        self._check_value_dict("STUDY", self.data, dict)
        self._check_dict("STUDY", self.data, ["name", "description", "version"], [str, str, float], ["name"])
        self.check = True
                

class FilesSection(Section):
    def __init__(self, sections, data, study_path):
        example_str =  "FILES:\n" +\
                       "    - path: mypath1\n" +\
                       "      files:\n" +\
                       "        - myfile1.txt\n" +\
                       "        - myfile2.txt\n" +\
                       "    - path: mypath2\n" +\
                       "      files:\n" +\
                       "        - myfile.*\n" +\
                       "        - myfile3.txt"
        super(FilesSection, self).__init__(sections, data, study_path, example_str, "FILES")


    def _check(self):
        self._check_list("FILES", self.data, dict)
        for e1 in self.data:
            self._check_dict("FILES", e1, ["path", "files"],  [str, list], ["path", "files"])
            self._check_list("files", e1["files"], str)
        self.check = True

class DownloadSection(Section):
    def __init__(self, sections, data, study_path):
        example_str = "DOWNLOAD:\n"
        super(DownloadSection, self).__init__(sections, data, study_path, example_str, "DOWNLOAD")

    def _check(self):
        self._check_list("DOWNLOAD", self.data, dict)
        for e1 in self.data:
            self._check_dict("FILES", e1, ["path", "include", "exclude"],\
                             [str, list, list], ["path"], [("include", "exclude")])
            if "include" in e1.keys():
                self._check_list("include", e1["include"], str)
            elif "exclude" in e1.keys():
                self._check_list("exclude", e1["include"], str)
        self.check = True

class ParamsSection(Section):
    def __init__(self, sections, data, study_path, example_str, name):
        super(ParamsSection, self).__init__(sections, data, study_path, example_str, name) 

    def _check_param_value(self, name, value):
        allowed_types = [list, bool, str, float, int]
        value_type = type(value)
        if value_type not in allowed_types:
            raise Exception("Type '{}' of parameter '{}' is not one in '{}'.".\
                            format(value_type.__name__, name, [e.__name__ for e in allowed_types]))

    def _check_param_name(self, name):
        if re.match("^[a-z]{1}[a-z0-9]*(\-[a-z0-9]+)*$", name) is None:
            raise Exception("Malformed parameter name '{}' in section '{}'.".format(name, self.name))
    
    def _check_generator_name(self, name, gen_type):
        assert gen_type in ["list", "scalar"]
        regexp_scalar = "(gsc|gsv)"
        regexp_list = "(glc|gls|gld)\(([0-9]+)\)" 
        if gen_type == "scalar":
            regexp = regexp_scalar
            empty = (None, None)
        elif gen_type == "list":
            regexp = regexp_list
            empty = (None, None, None)
        starts_with = re.match("^((gsc|gsv)|(glc|gls|gld)\(.+)\:", name) is not None or \
                      re.match("^{}\:".format(regexp_list), name) is not None
                            
        if starts_with:
            groups = re.match("^%s\:([a-zA-Z_]{1}[a-zA-Z0-9_]*$)" % regexp, name)
            if groups is not None:
                return groups.groups()
            else:
                raise Exception("Malformed {} generator name '{}' in section '{}'."\
                                .format(gen_type, name, self.name))

        else:
            return empty

    def _import_generators(self):
        try:
            # Insert study path to load generators
            sys.path.insert(0, self.study_path)
            import generators
        except Exception as err:
            #TODO: THis should be a warning message not an exception
            raise Exception("File 'generators.py' not found in study directory.")
        return generators
 


class ParamsMultivalSection(ParamsSection):
    def __init__(self, sections, data, study_path):
        example_str = ""
        self.tree = DictImporter().import_(data)
        super(ParamsMultivalSection, self).__init__(sections, data, study_path, example_str, "PARAMS-MULTIVAL")
        self.multival_keys = [node.name for node in PreOrderIter(self.tree)]

    def _check(self):
        self._check_value_dict("PARAMS-MULTIVAL", self.data, dict)
        allowed  = ["name", "mode", "values", "defaults"]
        required  = ["name", "mode", "values"]
        fields_type = [str, str, (list, str), dict]
        allowed_root = ["name", "values", "defaults"]
        required_root = ["name", "values"]
        root_type = [str, (list, str), dict]
        for node in PreOrderIter(self.tree):
            node_dict = {}
            # Search for fields defined in each node
            for key, value in filter(lambda item: not item[0].startswith("_"),
                                     sorted(node.__dict__.items(), key=lambda item: item[0])):
                node_dict[key] = value
            if node.is_root:
                self._check_dict("PARAMS-MULTIVAL(node='{}')".format(node.name), node_dict, allowed_root, root_type, required_root )
            else:
                self._check_dict("PARAMS-MULTIVAL(node='{}')".format(node.name), node_dict, allowed, fields_type, required)

            self._check_param_name(node.name)
            for pvalue in node.values:
                self._check_param_value(node.name, pvalue)
            if type(pvalue) == str:
                a = self._check_generator_name(node.values, "list")
        self.check = True

    def _load(self):
        # Replace generator strings for function objects
        generators = self._import_generators()
        for node in PreOrderIter(self.tree):
            if type(node.values) == str:
                gen_type, list_size, gen_name = self._check_generator_name(node.values, "list") 
                list_size = int(list_size)
                if gen_name:
                    try:
                        pvalue = getattr(generators, gen_name)
                    except AttributeError as error:
                        raise Exception("Generator '%s' not found in 'generators.py'." % gen_name)
                    pvalue = pvalue(self.sections["PARAMS-SINGLEVAL"].get_constant_params(), list_size)
                    print pvalue
                    # except Exception as error:
                    #     raise Exception("Error in 'genenerators.py - '" + str(error))
                    # if pvalue.__name__ not in ["gen_scalar_const_f", "gen_scalar_var_f"]:
                    #     raise Exception("Generator '{}:{}' in section '{}' can only be of '@gen_scalar_const' or '@gen_scalar_var' type.".format(gen_type, gen_name, self.name))
                    # elif pvalue.__name__ == "gen_scalar_const_f" and gen_type != "gsc":
                    #     raise Exception("Generator '{}:{}' do not match type '@gen_scalar_const'.".format(gen_type, gen_name))
                    # elif pvalue.__name__ == "gen_scalar_var_f" and gen_type != "gsv":
                    #     raise Exception("Generator '{}:{}' do not match type '@gen_scalar_var'.".format(gen_type, gen_name))
                    node.values = pvalue
        self.load = True
        # print [(k.name, k.values) for k in PreOrderIter(self.tree)]

        


class ParamsSinglevalSection(ParamsSection):
    def __init__(self, sections, data, study_path):
        super(ParamsSinglevalSection, self).__init__(sections, data, study_path, "", "PARAMS-SINGLEVAL")
        self.singleval_keys = self.data.keys()
        self.load_priority = 1

    def get_constant_params(self):
        pconst = {k:v for k,v in self.data.items() 
                          if (callable(v) and  v.__name__ == "gen_scalar_const_f") or\
                              not callable(v)}
        return pconst


    def _check(self):
        self._check_value_dict("PARAMS-SINGLEVAL", self.data, dict)
        for pname, pvalue in self.data.items():
            self._check_param_name(pname)
            self._check_param_value(pname, pvalue)
            if type(pvalue) == str:
                self._check_generator_name(pvalue, "scalar")
        self.check = True


    def _load(self):
        # Replace generator strings for function objects
        generators = self._import_generators()
        for pname, pvalue in self.data.items():
            if type(pvalue) == str:
                gen_type, gen_name = self._check_generator_name(pvalue, "scalar") 
                if gen_name:
                    try:
                        pvalue = getattr(generators, gen_name)
                    except AttributeError as error:
                        raise Exception("Generator '%s' not found in 'generators.py'." % gen_name)
                    except Exception as error:
                        raise Exception("Error in 'genenerators.py - '" + str(error))
                    if pvalue.__name__ not in ["gen_scalar_const_f", "gen_scalar_var_f"]:
                        raise Exception("Generator '{}:{}' in section '{}' can only be of '@gen_scalar_const' or '@gen_scalar_var' type.".format(gen_type, gen_name, self.name))
                    elif pvalue.__name__ == "gen_scalar_const_f" and gen_type != "gsc":
                        raise Exception("Generator '{}:{}' do not match type '@gen_scalar_const'.".format(gen_type, gen_name))
                    elif pvalue.__name__ == "gen_scalar_var_f" and gen_type != "gsv":
                        raise Exception("Generator '{}:{}' do not match type '@gen_scalar_var'.".format(gen_type, gen_name))
                    self.data[pname] = pvalue
        # Remove the path 
        del sys.path[0]
        # Get constant parameters and resolve generators
        const_params = ParamInstance(self.get_constant_params()) 
        const_params.resolve_params()
        self.data.update(const_params.data)
        self.load = True



                



class BuildSection(Section):
    def __init__(self, sections, data, study_path):
        # self.ALLOWED_FIELDS = ["name", "description", "version"]
        super(BuildSection, self).__init__(sections, data, study_path, "", "BUILD")


#TODO: Decouple allowed sections from Param file to make it general
class ParamFile:
    def __init__(self, path='.', allowed_sections=None, fname='params.yaml'):
        self.ALLOWED_SECTIONS = {"STUDY": StudySection, 
                                 "PARAMS-MULTIVAL": ParamsMultivalSection,
                                 "PARAMS-SINGLEVAL": ParamsSinglevalSection,
                                 "DOWNLOAD": DownloadSection,
                                 "BUILD": BuildSection,
                                 "FILES": FilesSection}
        self.study_path = os.path.abspath(path)
        self.fname = fname
        self.path = os.path.join(self.study_path, fname)
        self.loaded = False
        self.params_data = {}
        self.sections = {}

    def load(self):
        try:
            with open(self.path, 'r') as paramfile:
                self.params_data = yaml.load(paramfile)
        except IOError as e:
            raise Exception("Problem opening 'params.yaml' file - %s." % e.strerror)
        except Exception as error:
            raise Exception("Parsing error in 'params.yaml': \n" + str(error))
        self._load_sections()
        self.loaded = True


    def _load_sections(self):
        #TODO: Fix this. Make ParamFile inherit from Section as well.
        self.sections["PARAMS-SINGLEVAL"] = self.ALLOWED_SECTIONS["PARAMS-SINGLEVAL"](self.sections,\
                                            self.params_data["PARAMS-SINGLEVAL"], self.study_path)
        for section_name, section_data in  self.params_data.items():
            if section_name != "PARAMS-SINGLEVAL":
                try:
                    section_class =  self.ALLOWED_SECTIONS[section_name]
                except Exception as error:
                    raise
                #TODO: Enforce that PARAMS-SINGLEVAL is parsed before multival
                self.sections[section_name] = section_class(self.sections, section_data, self.study_path)
                    # raise Exception("Error: Section '%s' is mandatory in 'params.yaml'." % section_name)

                #TODO: Rework this and check for correct format of params.yaml. Add this into ParamSection
        # Check that there are no clash between multival and singleval params
        common_params = set(self.sections["PARAMS-SINGLEVAL"].singleval_keys).\
                intersection(self.sections["PARAMS-MULTIVAL"].multival_keys)
        if common_params:
            raise Exception("Parameter(s) '{}'  with same name.".format(tuple(common_params)))

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


