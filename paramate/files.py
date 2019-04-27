import time
import yaml
import stat
import os
import glob
import shutil
import re
import sys
import json
from anytree import Node, PreOrderIter, RenderTree
from anytree.importer import DictImporter
from anytree.render import AsciiStyle 
from study import Case
from common import ParamInstance, _printer


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
        self.checked = False
        self.loaded = False
        self._check()
        self._load()

    def _check(self):
        self.checked = True

    def _load(self):
        self.loaded = True

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

    def _check_dict(self, field, pdict, allowed_fields, mutual_exc=[], example_str_in=None):
        if example_str_in is not None:
            example_str = example_str_in
        else:
            example_str = self.example_str
        self._check_value_dict(field, pdict, dict)

        # Check for mutual exclusive groups
        mutual_exc_sets = [set(g) for g in mutual_exc]
        for g in mutual_exc_sets:
            intersect = g.intersection(set(pdict.keys()))
            if len(intersect) > 1:
                raise Exception("Invalid combination of {} fields.\nExample:\n {}".format(tuple(intersect), example_str))

        # Check no other fields are present and type is correct
        for  k, v in pdict.items():
            if k not in allowed_fields.keys():
                raise Exception("Invalid field '{}' in section '{}'.\nExample:\n {}"\
                                .format(k, self.name, example_str))
            else:
                allowed_types = allowed_fields[k][0]
                allowed_values = allowed_fields[k][2]
                if allowed_values is not None and v not in allowed_values:
                    raise Exception("Invalid field '{}' with value '{}' in section '{}'. Only '{}' values are allowed.\nExample:\n {}"\
                                    .format(k, v, self.name, allowed_values, example_str))
                else:
                    self._check_value_dict(k, pdict[k], allowed_types)

        # Check all mandatory fields are present and types
        required_fields = [f for f in allowed_fields.keys() if allowed_fields[f][1]]
        for k in required_fields:
            if k not in pdict.keys():
                raise Exception("Required field '{}' not present in section '{}'.\nExample:\n {}"\
                                .format(k, self.name, example_str))



class StudySection(Section):
    def __init__(self, sections, data, study_path):
        example_str =  "STUDY:\n" +\
                       "    name: mystudy\n" +\
                       "    version: 1.1\n" +\
                       "    description: 'Amazing study.'"
        super(StudySection, self).__init__(sections, data, study_path, example_str, "STUDY")

    def _check(self):
        self._check_value_dict("STUDY", self.data, dict)
        allowed_fields = {"name": (str, True, None),
                          "description": (str, False, None),
                          "version": (float, False, None)}
        self._check_dict("STUDY", self.data, allowed_fields)
        self.checked = True
                

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
        allowed_fields = {"path": (str, True, None), "files": (list, True, None)}
        self._check_list("FILES", self.data, dict)
        for e1 in self.data:
            self._check_dict("FILES", e1, allowed_fields)
            self._check_list("files", e1["files"], str)
        self.checked = True

class DownloadSection(Section):
    def __init__(self, sections, data, study_path):
        example_str = "DOWNLOAD:\n"
        super(DownloadSection, self).__init__(sections, data, study_path, example_str, "DOWNLOAD")

    def _check(self):
        self._check_list("DOWNLOAD", self.data, dict)
        allowed_fields = {"path": (str, True, None),
                          "include": (list, False, None),
                          "exclude": (list, False, None)}
        for e1 in self.data:
            self._check_dict("FILES", e1, allowed_fields, [("include", "exclude")])
            if "include" in e1.keys():
                self._check_list("include", e1["include"], str)
            elif "exclude" in e1.keys():
                self._check_list("exclude", e1["include"], str)
        self.checked = True

class ParamsSection(Section):
    def __init__(self, sections, data, study_path, example_str, name):
        super(ParamsSection, self).__init__(sections, data, study_path, example_str, name) 
        self.param_names = self._get_param_namelist()

    # Override
    def _get_param_namelist(self):
        return []

    def get_common_params(self, param_section):
        return set(self.param_names).intersection(set(param_section.param_names))

    def _check_param_value(self, name, value):
        allowed_types = [dict, list, bool, str, float, int]
        value_type = type(value)
        if value_type not in allowed_types:
            raise Exception("Type '{}' of parameter '{}' is not one in '{}'.".\
                            format(value_type.__name__, name, [e.__name__ for e in allowed_types]))

    def _check_param_name(self, name):
        if type(name) == str:
            names_in = (name,)
        else:
            names_in = name
        for n in names_in:
            if re.match("^[a-z]{1}[a-z0-9]*(\-[a-z0-9]+)*$", n) is None:
                raise Exception("Malformed parameter name '{}' in section '{}'.".format(n, self.name))
    
    def _check_generator_name(self, name, gen_type):
        assert gen_type in ["list", "scalar"]
        regexp_scalar = "(gsc|gsv)"
        regexp_list = "(glc|glv|gld)\(([0-9]+)\)" 
        if gen_type == "scalar":
            regexp = regexp_scalar
            empty = (None, None)
        elif gen_type == "list":
            regexp = regexp_list
            empty = (None, None, None)
        starts_with = re.match("^g.*\:", name) is not None or \
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
            raise
            #TODO: THis should be a warning message not an exception
            raise Exception("File 'generators.py' not found in study directory.")
        return generators
 


class ParamsMultivalSection(ParamsSection):
    def __init__(self, sections, data, study_path):
        example_str = ""
        self.tree = DictImporter().import_(data)
        super(ParamsMultivalSection, self).__init__(sections, data, study_path, example_str, "PARAMS-MULTIVAL")

    def _get_param_namelist(self):
        return [node.name for node in PreOrderIter(self.tree)]

    def _check(self):
        self._check_value_dict("PARAMS-MULTIVAL", self.data, dict)
        allowed_fields = {"name": (str, True, None), "mode": (str, True, ('*','+')),
                          "values": ((list, str), True, None), "defaults": (dict, False, None)}
        allowed_root_fields = {"name": (str, True, None), "values": ((list, str), True, None),
                               "defaults": (dict, False, None)}
        for node in PreOrderIter(self.tree):
            node_dict = {}
            # Search for fields defined in each node
            for key, value in filter(lambda item: not item[0].startswith("_"),
                                     sorted(node.__dict__.items(), key=lambda item: item[0])):
                node_dict[key] = value
            if node.is_root:
                self._check_dict("PARAMS-MULTIVAL(node='{}')".format(node.name), node_dict, allowed_root_fields)
            else:
                self._check_dict("PARAMS-MULTIVAL(node='{}')".format(node.name), node_dict, allowed_fields)
            # Check parameters
            self._check_param_name(node.name)
            for pvalue in node.values:
                self._check_param_value(node.name, pvalue)
            # Check for generators
            if type(node.values) == str:
                ret = self._check_generator_name(node.values, "list")
                # If it is not a generator str is not allowed
                if None in ret:
                    raise Exception("Values of parameter '{}' can only be of type 'list' or 'generator' but 'str' was found instead."\
                                    .format(node.name))
            # Add label for printing
            if node.is_root:
                node.label = node.name
            else:
                node.label = "({}){}".format(node.mode, node.name)

        self.checked = True

    def _load(self):
        # Replace generator strings for function objects
        generators = self._import_generators()
        for node in PreOrderIter(self.tree):
            if type(node.values) == str:
                gen_type, list_size, gen_name = self._check_generator_name(node.values, "list") 
                if gen_name:
                    list_size = int(list_size)
                    try:
                        pvalue = getattr(generators, gen_name)
                    except AttributeError as error:
                        raise Exception("Generator '%s' not found in 'generators.py'." % gen_name)
                    except Exception as error:
                        raise Exception("Error in 'genenerators.py - '" + str(error))
                    if pvalue.__name__ not in ["gen_list_const_f", "gen_list_dynamic_f", "gen_list_static_f"]:
                        raise Exception("Generator '{}:{}' in section '{}' can only be of '@gen_list_const','@gen_list_variable' or '@gen_list_dynamic' type."\
                                        .format(gen_type, gen_name, self.name))
                    elif pvalue.__name__ == "gen_list_const_f" and gen_type != "glc":
                        raise Exception("Generator '{}:{}' do not match type '@gen_list_const'.".format(gen_type, gen_name))
                    elif pvalue.__name__ == "gen_list_var_f" and gen_type != "glv":
                        raise Exception("Generator '{}:{}' do not match type '@gen_list_var'.".format(gen_type, gen_name))
                    # Call generator
                    singleval_const_params = {}
                    if "PARAMS-SINGLEVAL" in self.sections.keys():
                        singleval_const_params = self.sections["PARAMS-SINGLEVAL"].get_constant_params() 
                    node.values = pvalue(singleval_const_params, list_size)
        self.loaded = True

        

class ParamsSinglevalSection(ParamsSection):
    def __init__(self, sections, data, study_path):
        data_in = self._unfold_dict_params(data)
        super(ParamsSinglevalSection, self).__init__(sections, data_in, study_path, "", "PARAMS-SINGLEVAL")

    def _get_param_namelist(self):
        return self.data.keys()

    def _unfold_dict_params(self, data):
        unfolded_params = {}
        for pname, pvalue in data.items():
            if type(pvalue) == dict:
                for sub_pname, sub_pvalue in pvalue.items():
                        unfolded_params.update({(pname, sub_pname):sub_pvalue})
            unfolded_params.update({pname: pvalue})

        return unfolded_params

    def get_constant_params(self):
        pconst = {}
        for pname, pvalue in self.data.items():
            # if type(pvalue) == dict:
            #     pconst.update({pname:pvalue})
            #     for sub_pname, sub_pvalue in pvalue.items():
            #         if (callable(sub_pvalue) and  sub_pvalue.__name__ == "gen_scalar_const_f") or not callable(sub_pvalue):
            #             pconst.update({(pname, sub_pname):sub_pvalue})
            # else:
            if (callable(pvalue) and  pvalue.__name__ == "gen_scalar_const_f") or not callable(pvalue):
                pconst.update({pname:pvalue})
        return pconst


    def _check(self):
        self._check_value_dict("PARAMS-SINGLEVAL", self.data, dict)
        for pname, pvalue in self.data.items():
            self._check_param_name(pname)
            self._check_param_value(pname, pvalue)
            if type(pvalue) == str:
                self._check_generator_name(pvalue, "scalar")
        self.checked = True


    def _load(self):
        def get_generators(pvalue, generators):
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
            return pvalue

        # Replace generator strings for function objects
        generators = self._import_generators()
        for pname, pvalue in self.data.items():
            if type(pvalue) == dict:
                for sub_pname, sub_pvalue in pvalue.items():
                    self.data[pname][sub_pname] = get_generators(sub_pvalue, generators)
            self.data[pname] = get_generators(pvalue, generators)
        # Remove the path 
        del sys.path[0]
        # Get constant parameters and resolve generators
        const_params = ParamInstance(self.get_constant_params()) 
        const_params.resolve_params()
        self.data.update(const_params.data)
        self.loaded = True



class BuildSection(Section):
    def __init__(self, sections, data, study_path):
        # self.ALLOWED_FIELDS = ["name", "description", "version"]
        pass
        # super(BuildSection, self).__init__(sections, data, study_path, "", "BUILD")


class RemoteSection(Section):
    def __init__(self, sections, data, study_path, remote_name):
        example_str =  "FILES:\n" +\
                       "    - path: mypath1\n" +\
                       "      files:\n" +\
                       "        - myfile1.txt\n" +\
                       "        - myfile2.txt\n" +\
                       "    - path: mypath2\n" +\
                       "      files:\n" +\
                       "        - myfile.*\n" +\
                       "        - myfile3.txt"
        self.remote_name = remote_name
        super(RemoteSection, self).__init__(sections, data, study_path, example_str, remote_name)


    def _check(self):
        allowed_fields = {"user": (str, False, None),
                          "hostname": (str, False, None),
                          "port": (int, False, None),
                          "ssh-key": (dict, False, None),
                          "remote-workdir": (str, True, None),
                          "shell": (str, False, ["bash", "csh"]),
                          "resource-manager": (str, True, ["pbs", "sge", "slurm"]),
                          "jobs-commands": (dict, False, None),
                          "config-host": (str, False, None),
                          }
        mutual_exc = [("user", "config-host"), ("hostname", "config-host"),
                      ("port", "config-host"), ("ssh-key", "config-host")] 
        self._check_dict(self.remote_name, self.data, allowed_fields, mutual_exc=mutual_exc)

        allowed_fields_sshkey = {"file": (str, True, None),
                                 "allow-agent": (bool, False, None),
                                 "lookup-keys": (bool, False, None),
                                }
        allowed_fields_commands = {"submit": (str, False, None),
                                   "status": (str, False, None),
                                   "delete": (str, False, None),
                                  }
        if "ssh-key" in self.data:
            self._check_dict("ssh-key", self.data["ssh-key"], allowed_fields_sshkey)

        if "jobs-commands" in self.data:
            self._check_dict("jobs-commands", self.data["jobs-commands"], allowed_fields_commands)
        self.checked = True



class RemotesFile(Section):
    def __init__(self, path='.', allowed_sections=None, fname='remotes.yaml'):
        study_path = os.path.abspath(path)
        #NOTE: _check() and _load() not overriden here. _check_sections() and _load_sections() implemented.
        #      This is to allow explicit load() call after constructor is called.
        super(RemotesFile, self).__init__({}, {}, study_path, "", "Remotes file")
        self.fname = fname
        self.path = os.path.join(self.study_path, fname)
        self.default_remote = None
        self.loaded = False

    def load(self):
        try:
            with open(self.path, 'r') as  remotefile:
                self.data = yaml.load(remotefile)
        except IOError as e:
            raise Exception("Problem opening file '{}' - {}.".format(self.fname, e.strerror))
        except yaml.YAMLError as error:
            raise Exception("Wrong YAML format in file '{}' - {}.".format(self.fname, str(error).capitalize()))
        self._check_remotes()
        self._load_sections()

    def _check_remotes(self):
        allowed_fields = {"default": (str, False, None)}
        for remote_name in self.data.keys():
            if remote_name == "default": continue
            allowed_fields.update({remote_name: (dict, True, None)})
        self._check_value_dict("Remotes file", self.data, dict)
        self._check_dict("Remotes file", self.data, allowed_fields)
        if "default" in self.data.keys():
            if not self.data["default"] in self.data.keys():
                raise Exception("Default remote '{}' not found in '{}'".format(self.data["default"], self.fname))
        self.checked = True

    def _load_sections(self):
        for remote_name, remote in self.data.items():
            if remote_name == "default": continue
            self.sections[remote_name] = RemoteSection(self.sections, self.data[remote_name], self.study_path, remote_name)

        # Select default if not defined
        if "default" in self.data.keys():
            new_default = self.data["default"]
        else:
            new_default = self.data.keys()[0] 
        self.default_remote = new_default
        self.data["default"] = self.data[new_default]
        self.loaded = True

    def __getitem__(self, key):
            if self.loaded:
                return self.data[key]
            else:
                raise Exception("File '{}' not loaded.".format(self.fname))


#TODO: Decouple allowed sections from Param file to make it general
class ParamFile(Section):
    def __init__(self, path='.', allowed_sections=None, fname='params.yaml'):
        # Map from section name to (Class, loading priority)
        self.SECTIONS_CLASS    = {"STUDY": (StudySection, 0), 
                                 "PARAMS-MULTIVAL": (ParamsMultivalSection, 0),
                                 "PARAMS-SINGLEVAL":(ParamsSinglevalSection, 1) ,
                                 "DOWNLOAD": (DownloadSection, 0),
                                 "BUILD": (BuildSection, 0),
                                 "FILES": (FilesSection, 0)}
        study_path = os.path.abspath(path)
        #NOTE: _check() and _load() not overriden here. _check_sections() and _load_sections() implemented.
        #      This is to allow explicit load() call after constructor is called.
        super(ParamFile, self).__init__({}, {}, study_path, "", "Param file")
        self.fname = fname
        self.path = os.path.join(self.study_path, fname)
        self.loaded = False

    def load(self):
        try:
            with open(self.path, 'r') as paramfile:
                self.data = yaml.load(paramfile)
        except IOError as e:
            raise Exception("Problem opening '{}' file - {}.".format(self.fname, e.strerror))
        except yaml.YAMLError as error:
            raise Exception("Wrong YAML format in file '{}' - {}.".format(self.fname, str(error).capitalize()))
        self._check_sections()
        self._load_sections()
        self.loaded = True

    def _check_sections(self):
        allowed_sections = {"STUDY": (dict, True, None),
                            "PARAMS-MULTIVAL": (dict, True, None),
                            "PARAMS-SINGLEVAL": (dict, False, None),
                            "DOWNLOAD": (list, False, None),
                            "FILES": (list, True, None)}
                            # "BUILD": (dict, False, None)}
        self._check_value_dict("Parameter file", self.data, dict)
        self._check_dict("Parameter file", self.data, allowed_sections)
        self.checked = True
        

    def _load_sections(self):
        sections_sortby_priority  = sorted(self.SECTIONS_CLASS.items(), key=lambda kv: kv[1][1])[::-1]
        for section_name, (section_class, section_priority) in sections_sortby_priority:
            # self.data.keys() always contains valid sections as they have been checked previously in _check_sections()
            if section_name in self.data.keys():
                self.sections[section_name] = section_class(self.sections, self.data[section_name], self.study_path)
        if "PARAMS-SINGLEVAL" in self.sections.keys():
            common_params = self.sections["PARAMS-MULTIVAL"].get_common_params(self.sections["PARAMS-SINGLEVAL"])
            if common_params:
                raise Exception("Parameter(s) '{}'  with same name.".format(tuple(common_params)))

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

    def print_tree(self):
        root = self.sections["PARAMS-MULTIVAL"].tree
        # print ""
        _printer.print_msg("", "blank")
        for l in str(RenderTree(root, AsciiStyle()).by_attr("label")).split('\n'):
            _printer.print_msg(l)
        _printer.print_msg("", "blank")


    def __getitem__(self, key):
        if self.loaded:
            return self.data[key]
        else:
            raise Exception("File 'params.yaml' not loaded.")


