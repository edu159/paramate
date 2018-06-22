import time
import yaml
import stat
import os
import glob
import shutil
import re
import sys
import json
from study import Case


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
                                raise Exception("Generator '%s' not found in 'generators.py'." % gen_name)
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
                                raise Exception("Generator '%s' not found in 'generators.py'." % gen_name)
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


