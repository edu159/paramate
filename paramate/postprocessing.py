import pandas as pd
from paramate.study import Study
import os

# POSTPROC_TABLE_FIELDS = {"results": 
#                         {"cols": {
#                             ("Cv", "P", "D", "Visc", "U"): f1,
#                             ("tauV", "tauS", "Psi"): f2,},
#                         "post-func": f_post,
#                         "output-type": "csv",
#                         "output-directory": results,
#                         "param-cols": True,
#                         "keep-empty-values": True,}
#                    }

def create_results_table(postproc_struct, study):
    for table_name, table_data in postproc_struct.items():
        cols = []
        if table_data["param-cols"]:
            cols.extend(study.params)
        for cols_group in table_data["cols"]:
            cols.extend(cols_group)
        group_sets = [set(g) for g in table_data["cols"]]
        if len(group_sets) > 1:
            common_cols = set.intersection(*group_sets)
            if common_cols:
                raise Exception("Common columns found between the groups specified -> {}.".format(tuple(common_cols)))
        table_rows = {}
        for cols_group, group_func in table_data["cols"].items():
            for case in study.case_selection:
                row = group_func(case)
                if row is None:
                    if table_data["keep-empty-values"]:
                        row = {key: None for key in cols}
                    else:
                        continue
                group_diff = set(cols_group).difference(set(row.keys()))
                if group_diff:
                    raise Exception("Error in keys differ in {}.".format(group_diff))
                else:
                    try:
                        table_rows[case.name].update(row)
                    except KeyError:
                        table_rows[case.name] = row
                    if table_data["param-cols"]:
                        table_rows[case.name].update({pname: case.params[pname] for pname in study.params})
        data_frame = pd.DataFrame.from_dict(table_rows, columns=cols, orient="index")
        output_path = study.path
        if "output-directory" in table_data.keys():
            output_path = os.path.join(output_path, table_data["output-directory"])
        output_path = os.path.join(output_path, "{}.csv".format(table_name))
        data_frame.to_csv(output_path)
        if "post-func" in table_data.keys():
            if not callable(table_data["post-func"]):
                raise Exception("Postprocessing error - 'post-func' for table '{}' is not callable.".format(table_name))
            table_data["post-func"](data_frame)

