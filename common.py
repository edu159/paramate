from __future__ import print_function
import re
import os
import progressbar
import colorama as color
import time

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


class MessagePrinter(object):
    def __init__(self, quiet, verbose):
        self.quiet = quiet
        self.verbose = verbose
        color.init()
        self.colormap = {"info": color.Fore.GREEN,
                         "warning": color.Fore.YELLOW,
                         "error": color.Fore.RED,
                         "input": color.Fore.BLUE,
                         }
    #Logic: a) --quiet option can be ignored.
    #       b) if verbose=True then check --verbose flag 
            
    def print_msg(self, message, msg_type="info", ignore_quiet=False, verbose=False, end="\n"):
        max_len = max([len(k) for k in self.colormap.keys()])
        formatted_msg = self.colormap[msg_type] + "[ " + msg_type.capitalize().ljust(max_len) + " ] " + color.Fore.WHITE +  message
        if not self.quiet:
            if verbose:
                if self.verbose:
                    print(formatted_msg, end=end)
            else:
                print(formatted_msg, end=end)
        else:
            if ignore_quiet:
                print(formatted_msg, end=end)


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


