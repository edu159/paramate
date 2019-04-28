import os
import shutil
from case import Case
from files import InfoFile, ParamFile
import itertools
from files import ParamInstance
from common import replace_placeholders, _printer
import subprocess
import shutil
import glob
import stat


class Study:
    #TODO: Look into better loading of parameter and info files.
    def __init__(self, name, path, load_param_file=True):
        self.path = path
        self.name = name
        self.study_file = InfoFile(path=path) 
        self.param_file = ParamFile(path=path)
        if load_param_file:
            self.param_file.load()
        self.cases = []
        self.case_selection = []
        self.nof_cases = 0

    def group_by_param(self, case_list, params):
        param_vals = []
        for p in params:
            param_vals.append([])
        for i, p in enumerate(params):
            for case in case_list:
                param_vals[i].append(case.params[p])
            param_vals[i] = list(set(param_vals[i]))
        pairs = itertools.product(*param_vals)
        groups = {tuple(p): [] for p in pairs}
        for case in case_list:
            for gk in groups.keys():
                belong = True
                for i, p in enumerate(params):
                    if gk[i] != case.params[p]:
                        belong = False
                        break
                if belong:
                    groups[gk].append(case)
        return groups

             
    def sort_by_param(self, case_list_in, param):
        case_list = list(case_list_in)
        for index in range(1,len(case_list)):
            current_case = case_list[index]
            current_val = current_case.params[param]
            position = index
            while position > 0 and case_list[position-1].params[param] > current_val:
                 case_list[position] = case_list[position-1]
                 position = position-1
            case_list[position] = current_case
        return case_list


    def get_cases(self, search_vals, field, sortby=None, selection_on=True):
        if selection_on:
            cases_list = self.case_selection
        else:
            cases_list = self.cases
        if sortby == None:
            match_list = []
        else:
            match_list = {}
        for case in cases_list:
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

    def get_cases_byparams(self, params, mode="all", selection_on=True):
        if selection_on:
            cases_list = self.case_selection
        else:
            cases_list = self.cases
        assert mode == "all" or mode == "one"
        match_list = []
        for case in cases_list:
            case_match = False
            if mode == "all":
                case_match = True
            for param, value in params.items():
                if mode == "all":
                    if case["params"][param] != value:
                        case_match = False
                        break
                elif mode == "one":
                    if case["params"][param] == value:
                        case_match = True 
                        break
            if case_match:
                match_list.append(case)
        return match_list

    def load(self):
        self.cases = self.study_file.load()
        self.case_selection = self.cases
        self.nof_cases = len(self.cases)

    def save(self):
        self.study_file.save(self.cases)

    # TODO: Convert into reset
    def clean(self, selection_on=True):
        if selection_on:
            cases_list = self.case_selection
        else:
            cases_list = self.cases
        for case in cases_selection:
            case.reset()
            d = self.param_file.sections["DOWNLOAD"].get_download_paths(case)
            # print d
            #TODO: Remove submit.sh from case
            #TODO: Remove files for real
            # shutil.rmtree()
        self.save()

    # Accept a list of Case objects or case indices
    def set_selection(self, cases_list):
        if cases_list:
            assert type(cases_list[0]) is int or isinstance(cases_list[0], Case)
            if isinstance(cases_list[0], Case):
                cases_idx = [c.id for c in cases_list]
            else:
                cases_idx = cases_list
            self.case_selection = self.get_cases(cases_idx, "id", selection_on=False)
        else:
            sel.case_selection = []

    def delete(self, selection_on=True):
        if selection_on:
            cases_list = self.case_selection
        else:
            cases_list = self.cases
        for case in cases_list:
            try:
                shutil.rmtree(os.path.join(self.path, case["name"]))
            except Exception as error:
                pass
        
        self.study_file.remove()
        os.remove(os.path.join(self.path, "build.log"))
        os.remove(os.path.join(self.path, "generators.pyc"))

    def add_case(self, case_name, params, short_name=False):
        case = Case(self.nof_cases, params.copy(), case_name, short_name)
        self.cases.append(case)
        self.nof_cases += 1

class StudyGenerator(Study):
    DEFAULT_DIRECTORIES = ["template/build", "template/input", "template/output", "template/postproc"]
    DEFAULT_FILES = ["template/exec.sh", "template/build.sh", "README", "params.yaml", 
                     "generators.py"] 
    def __init__(self, study, short_name=False, build_once=False,
                 keep_onerror=False, abort_undefined=True):
        #TODO: Check if the study case directory is empty and in good condition
        self.study = study
        self.short_name = short_name
        self.build_once = build_once
        self.keep_onerror = keep_onerror
        self.abort_undefined = abort_undefined 
        self.multiv_params = self.study.param_file.sections["PARAMS-MULTIVAL"].tree
        # Not mandatory to have this section
        try:
            self.singlev_params = self.study.param_file.sections["PARAMS-SINGLEVAL"].data
        except KeyError:
            pass
        # Include build.sh to files to replace placeholders
        self.study.param_file["FILES"].append({"path": ".", "files": ["build.sh"]})
        self.template_path = os.path.join(self.study.path, "template")
        self.build_script_path = os.path.join(self.template_path, "build.sh")
        self.instances = []

    def execute_build_script(self, build_script_path):
        output = ""
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(build_script_path))
            output = subprocess.check_output(["bash", "-e", build_script_path], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            output = error.output
            raise Exception("Error while running 'build.sh' script. Check 'build.log' file.")
        finally:
            os.chdir(cwd)
            with open("build.log", "a+") as log:
                case = os.path.basename(os.path.dirname(build_script_path))
                log.write(case + ':\n')
                log.writelines(output)
                log.write('\n')


    #TODO: Create a file with instance information
    def _create_instance(self, instance_name, instance):
        _printer.print_msg("Creating instance '%s'..." % instance_name, verbose=True)
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
            replace_placeholders(file_paths, params, self.abort_undefined)
            if not self.build_once:
                # Force execution permissions to 'build.sh'
                _printer.print_msg("Building...", verbose=True, end="")
                build_script_path = os.path.join(casedir, "build.sh")
                os.chmod(build_script_path, stat.S_IXUSR | 
                         stat.S_IMODE(os.lstat(build_script_path).st_mode))
                self.execute_build_script(build_script_path)
                _printer.print_msg("Done.", verbose=True, msg_type="unformated")
        except Exception:
            if not self.keep_onerror:
                shutil.rmtree(casedir)
            raise
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
        _printer.print_msg("Generating cases...")
        if not os.path.exists(self.template_path):
            raise Exception("Cannot find 'template' directory!")
        if os.path.exists(self.build_script_path):
            if self.build_once:
                _printer.print_msg("Building once from 'build.sh'...")
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
        _printer.print_msg("Success: Created %d cases." % nof_instances)

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
        #TODO: Check for generators gen_list_const the sizes of the two lists properly 
        # avoid "IndexError: index 21 is out of bounds for axis 0 with size 21" type of errors
        def span_add(child):
            instance[node.name] = node.values[val_idx]
            self._gen_comb_instances(instance, child, defaults=defaults.copy())

        try:
            defaults_node = node.defaults
        except:
            defaults_node = {}
        common_params = set(defaults.keys()).intersection(set(defaults_node.keys()))
        if common_params:
            # print defaults.keys(), defaults_node.keys(), node
            raise Exception("Parameter(s) '{}'  with same name.".format(tuple(common_params)))
        defaults.update(defaults_node)
        if node.is_root:
            if node.children:
                for child in node.children:
                    span_mult(child)
            else:
                span_mult(None)
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


