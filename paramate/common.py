from __future__ import print_function
import re
import os
import progressbar
import colorama as color
import time
import sys

from UserDict import UserDict

class ParamInstance(UserDict):
    def __init__(self, initial_data={}):
        UserDict.__init__(self, initial_data)
        self.backtrace = []
        self.current_generator = None

    def resolve_params(self):
        for pname, pval in self.items():
            # Empty backtrace
            self.backtrace = []
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
            # if it is a tuple then the parameter is a dictionary
            if type(key) == tuple:
                item = UserDict.__getitem__(self, key[0])[key[1]]
            else:
                item = UserDict.__getitem__(self, key)
            # if type(item) == dict:
            #     item
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

        item_out = UserDict.__getitem__(self, key)
        if type(key) == tuple:
            UserDict.__getitem__(self, key[0])[key[1]] = item_out
        return item_out



# Parampy params useful to build paths.
# TODO: Currently only 1 level of nesting allowed for dictionaries. This provide the possibility 
#       to return multiple values from a generator. Ideally an arbitrary level of nesting levels like
#       YAML support would be the way to go. Nevertheless error checking become more convoluted.
def replace_placeholders(file_paths, params, warn_undefined=True):
    def get_param_value(params, pname, pname_in_file, undefined_dict, warn_undefined):
        pvalue = None
        try:
            pvalue = params[pname]
        except KeyError as error:
            if warn_undefined:
                raise Exception("Parameter '%s' not defined in 'params.yaml' (Found in '%s')." % (pname_in_file, os.path.basename(path)))
            file_path = os.path.basename(path)
            if file_path in undefined_dict.keys():
                undefined_dict[file_path].append(pname_in_file)
            else:
                undefined_dict[file_path] = [pname_in_file]
        return pvalue


    for path in file_paths:
        lines = []
        with open(path, 'r') as placeholder_file:
            lines = placeholder_file.readlines()                                                                                                                                                                                                 
        undefined_dict = {}
        for ln, line in enumerate(lines):
            # Find all candidate to placeholders
            line_opts = re.findall(r'\$\[([^\[^\]]+)\]', line)
            param_value = "$[UNDEFINED]"
            param_not_found = False
            for opt in line_opts:
                dict_params = re.match(r'(.+)\.(.+)', opt)
                if dict_params is not None:
                    dict_params = dict_params.groups()
                    sub_pvalue = get_param_value(params, dict_params[0], opt, undefined_dict, warn_undefined)
                    if sub_pvalue is not None:
                        paramtype = type(sub_pvalue) 
                        if paramtype != dict:
                            raise Exception("Parameter '{}' is defined as a '{}', but 'dict' type found.' (Found in '{}').".format(opt, str(paramtype.__name__), os.path.basename(path)))

                        param_value = get_param_value(sub_pvalue, dict_params[1], opt, undefined_dict, warn_undefined)
                else:
                    list_params = re.match(r'([^\(^\)]+)\(([0-9]+)\)', opt)
                    if list_params is not None:
                        list_params = list_params.groups()
                        param_value = get_param_value(params, list_params[0], opt, undefined_dict, warn_undefined)
                        if param_value is not None:
                            try:
                                param_value = param_value[int(list_params[1])]
                            except IndexError:
                                raise Exception("Parameter '%s' of type 'list' is out of range.' (Found in '%s')." % (opt, os.path.basename(path)))
                            paramtype = type(params[list_params[0]]) 
                            if paramtype != list:
                                raise Exception("Parameter '{}' is defined as a '{}', but 'list' type found.' (Found in '{}').".format(opt, str(paramtype.__name__), os.path.basename(path)))
                    else:
                        param_value = get_param_value(params, opt, opt, undefined_dict, warn_undefined)
                lines[ln] = lines[ln].replace("$[" + opt + "]", str(param_value))

        if undefined_dict:
            _printer.print_msg(str(undefined_dict))

        with open(path, 'w+') as replaced_file:
            replaced_file.writelines(lines)


class MessagePrinter(object):

    def __init__(self):
        # Shared attribute to fix the indentation level
        self.indent_level = 0
        self.max_len_msg = 0
        self.quiet = False 
        self.verbose = False 
        self.colormap = {"info": color.Fore.GREEN,
                        "warning": color.Fore.YELLOW,
                        "error": color.Fore.RED,
                        "input": color.Fore.CYAN,
                        "blank": color.Fore.WHITE,
                        "unformated": None
                       }

    def configure(self, verbose, quiet):
        self.quiet = quiet
        self.verbose = verbose 
        color.init()

    def _indent_spaces(self):
        return "    " * self.indent_level

    def formatted_str(self, message, msg_type, end="\n"):
        max_len = max([len(k) for k in self.colormap.keys()])
        if msg_type == "unformated":
            formatted_msg = message
        else:
            # Do not show Blank string
            if msg_type != "blank":
                msg_type_str = msg_type.capitalize()
            else:
                msg_type_str = ""
            indent = self._indent_spaces()
            formatted_msg = self.colormap[msg_type] + "[ " + msg_type_str.center(max_len) + " ] " + indent + color.Fore.WHITE +  message + color.Fore.RESET
        return formatted_msg
            
    #Logic: a) --quiet option can be ignored.
    #       b) if verbose=True then check --verbose flag 
    def print_msg(self, message, msg_type="info", ignore_quiet=False, verbose=False, end="\n"):
        print_flag = False
        formatted_msg = self.formatted_str(message, msg_type, end)
        if len(formatted_msg) > self.max_len_msg:
            self.max_len_msg = len(formatted_msg)
        if not self.quiet:
            if verbose:
                if self.verbose:
                    print_flag = True
            else:
                print_flag = True
        else:
            if ignore_quiet:
                print_flag = True
        if print_flag:
            print(formatted_msg, end=end)
            sys.stdout.flush()
            sys.stderr.flush()

# Instance of printer, to be configured in __main__
_printer = MessagePrinter()

class ProgressBar:
    def __init__(self, label):
        self.label = label
        self.reset()
      
    def callback(self, filename, size, sent):
        # Initialize
        if not self.updating:
            self._init(filename, size, sent)
            self.updating = True
        # End of transfer
        if sent == size:
            time.sleep(0.5)
            self.bar.update(self.max_bar_value)
            print("")
        # While transferring. Update every second.
        elif time.time() - self.prev_time > 1.0:
            chunk_size = size / self.max_bar_value
            self.bar.update(sent/chunk_size)
            self.prev_sent = sent
            self.prev_time = time.time()

    def reset(self):
        self.prev_sent = 0
        self.prev_time = 0.0
        self.max_bar_value = 0
        self.updating = False
        self.bar = None

    def _init(self, filename, size, sent):
        # Taken from DataSize class in progressbar
        scaled, power = progressbar.utils.scale_1024(size, 9)
        formatting = '%(scaled)5.1f %(prefix)s%(unit)s / ' + '%5.1f' % scaled + ' %(prefix)s%(unit)s'
        line_label = _printer.formatted_str(self.label, "info", end="")
        self.widgets = [
            line_label, '[', progressbar.DataSize(format=formatting), '] ',
            progressbar.Bar(),
            ' (', progressbar.ETA(), ') ',
            ' (', progressbar.FileTransferSpeed(), ')']
        self.prev_time = time.time()
        self.max_bar_value = size
        self.bar = progressbar.ProgressBar(max_value=size, widgets=self.widgets, term_width=2*_printer.max_len_msg)
        self.bar.update(0)



