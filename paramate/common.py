from __future__ import print_function
import re
import os
import progressbar
import colorama as color
import time
import sys

# Parampy params useful to build paths.
# TODO: Currently only 1 level of nesting allowed for dictionaries. This provide the possibility 
#       to return multiple values from a generator. Ideally an arbitrary level of nesting levels like
#       YAML support would be the way to go. Nevertheless error checking become more convoluted.
def replace_placeholders(file_paths, params):
    for path in file_paths:
        lines = []
        with open(path, 'r') as placeholder_file:
            lines = placeholder_file.readlines()                                                                                                                                                                                                 
        for ln, line in enumerate(lines):
            # Find all candidate to placeholders
            line_opts = re.findall(r'\$\[([^\[^\]]+)\]', line)
            param_value = ""
            param_not_found = False
            param_not_found_Exception = lambda opt: Exception("Parameter '%s' not defined in 'params.yaml' (Found in '%s')." % (opt, os.path.basename(path)))
            for opt in line_opts:
                dict_params = re.match(r'(.+)\.(.+)', opt)
                if dict_params is not None:
                    dict_params = dict_params.groups()
                    try:
                        param_value = str(params[dict_params[0]][dict_params[1]])
                    except KeyError as error:
                        raise param_not_found_Exception(opt)
                    paramtype = type(params[dict_params[0]]) 
                    if paramtype != dict:
                        raise Exception("Parameter '{}' is defined as a '{}', but 'dict' type found.' (Found in '{}').".format(opt, str(paramtype.__name__), os.path.basename(path)))
                else:
                    list_params = re.match(r'([^\(^\)]+)\(([0-9]+)\)', opt)
                    if list_params is not None:
                        list_params = list_params.groups()
                        try:
                            param_value = str(params[list_params[0]])
                        except KeyError as error:
                            raise param_not_found_Exception(opt)
                        try:
                            param_value = str(param_value[int(list_params[1])])
                        except IndexError:
                            raise Exception("Parameter '%s' of type 'list' is out of range.' (Found in '%s')." % (opt, os.path.basename(path)))
                        paramtype = type(params[list_params[0]]) 
                        if paramtype != list:
                            raise Exception("Parameter '{}' is defined as a '{}', but 'list' type found.' (Found in '{}').".format(opt, str(paramtype.__name__), os.path.basename(path)))
                    else:
                        try:
                            param_value = str(params[opt])
                        except KeyError as error:
                            raise param_not_found_Exception(opt)
                lines[ln] = lines[ln].replace("$[" + opt + "]", param_value)
        with open(path, 'w+') as replaced_file:
            replaced_file.writelines(lines)


class MessagePrinter(object):
    def __init__(self, quiet, verbose):
        self.quiet = quiet
        self.verbose = verbose
        color.init()
        self.colormap = {"info": color.Fore.GREEN,
                         "warning": color.Fore.YELLOW,
                         "error": color.Fore.RED,
                         "input": color.Fore.CYAN,
                         "unformated": None
                         }
    #Logic: a) --quiet option can be ignored.
    #       b) if verbose=True then check --verbose flag 
            
    def print_msg(self, message, msg_type="info", ignore_quiet=False, verbose=False, end="\n"):
        max_len = max([len(k) for k in self.colormap.keys()])
        if msg_type == "unformated":
            formatted_msg = message
        else:
            formatted_msg = self.colormap[msg_type] + "[ " + msg_type.capitalize().center(max_len) + " ] " + color.Fore.WHITE +  message + color.Fore.RESET
        if not self.quiet:
            if verbose:
                if self.verbose:
                    print(formatted_msg, end=end)
            else:
                print(formatted_msg, end=end)
        else:
            if ignore_quiet:
                print(formatted_msg, end=end)
        sys.stdout.flush()
        sys.stderr.flush()


class ProgressBar:
    def __init__(self):
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
        self.widgets = [
            ' [', progressbar.DataSize(format=formatting), '] ',
            progressbar.Bar(),
            ' (', progressbar.ETA(), ') ',
            ' (', progressbar.FileTransferSpeed(), ') ']
        self.prev_time = time.time()
        self.max_bar_value = size
        self.bar = progressbar.ProgressBar(max_value=size, widgets=self.widgets)
        self.bar.update(0)



