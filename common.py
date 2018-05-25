import re
import os

# Parampy params useful to build paths.
def replace_placeholders(file_paths, params):
    for path in file_paths:
        try:
            lines = []
            with open(path, 'r') as placeholder_file:
                lines = placeholder_file.readlines()                                                                                                                                                                                                 
            for ln, line in enumerate(lines):
                line_opts = set(re.findall(r'\$\[([a-zA-Z0-9\-]+?)\]', line))
                for opt in line_opts:
                    try:
                        lines[ln] = lines[ln].replace("$[" + opt + "]", str(params[opt]))
                    except KeyError as error:
                        raise Exception("Parameter '%s' not defined in 'params.yaml' (Found in '%s')." % (opt, os.path.basename(path)))
            with open(path, 'w+') as replaced_file:
                replaced_file.writelines(lines)
        except Exception as error:
            raise error


