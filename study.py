import os
import shutil

from case import Case
from files import InfoFile, ParamFile
import itertools


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
        import copy
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

    def get_cases_byparams(self, params, mode="all"):
        assert mode == "all" or mode == "one"
        match_list = []
        for case in self.cases:
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


