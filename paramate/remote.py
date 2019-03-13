import paramiko
import sys
import yaml
import shutil
import os
import time
from paramiko import SSHClient
import getpass
import tarfile
from scp import SCPClient
import socket
from common import replace_placeholders, MessagePrinter
import re


#TODO: Refactor Remote to separate configuration-related stuff
SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")
DEFAULT_DOWNLOAD_DIRS = ["output", "postproc"]

class CommandExecuter:

    def __init__(self, ssh):
        self.ssh = ssh
        channel = self.ssh.invoke_shell()
        self.stdin = channel.makefile('wb')
        self.stdout = channel.makefile('r')



    def exec_command(self, cmd):
        """

        :param cmd: the command to be executed on the remote computer
        :examples:  execute('ls')
                    execute('finger')
                    execute('cd folder_name')
        """
        cmd = cmd.strip('\n')
        self.stdin.write(cmd + '\n')
        finish = 'end of stdOUT buffer. finished with exit status'
        echo_cmd = 'echo {} $?'.format(finish)
        self.stdin.write(echo_cmd + '\n')
        shin = self.stdin
        self.stdin.flush()

        shout = []
        sherr = []
        exit_status = 0
        for line in self.stdout:
            if str(line).startswith(cmd) or str(line).startswith(echo_cmd):
                # up for now filled with shell junk from stdin
                shout = []
            elif str(line).startswith(finish):
                # our finish command ends with the exit status
                exit_status = int(str(line).rsplit(None, 1)[1])
                if exit_status:
                    # stderr is combined with stdout.
                    # thus, swap sherr with shout in a case of failure.
                    sherr = shout
                    shout = []
                break
            else:
                # get rid of 'coloring and formatting' special characters
                shout.append(re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]').sub('', line).
                             replace('\b', '').replace('\r', ''))

        # first and last lines of shout/sherr contain a prompt
        if shout and echo_cmd in shout[-1]:
            shout.pop()
        if shout and cmd in shout[0]:
            shout.pop(0)
        if sherr and echo_cmd in sherr[-1]:
            sherr.pop()
        if sherr and cmd in sherr[0]:
            sherr.pop(0)

        return shin, shout, sherr, exit_status


class CmdExecutionError(Exception):
    pass

class ConnectionTimeout(Exception):
    pass

class Remote(MessagePrinter):
    def __init__(self, name="", workdir=None, addr=None,\
            port=22, username=None, key_login=False, shell="bash",\
            quiet=False, verbose=False):
        super(Remote, self).__init__(quiet, verbose)
        self.name = name
        self.remote_yaml = None
        self.key_login = key_login
        self.addr = addr
        self.port = port
        self.username = username
        self.workdir = workdir
        self.shell = shell
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.command_status = None
        self.scp = None
        self._progress_callback = None
        self.cmd = None

    def __del__(self):
        self.ssh.close()

    @staticmethod
    def create_remote_template(path):
        filepath = os.path.join(DEFAULTS_DIR, "remote.yaml")
        try:
            if os.path.exists("remote.yaml"):
                raise Exception("Template 'remote.yaml' already exists.")
            else:
                shutil.copy(filepath, path)
        except Exception as error:
            raise Exception("Error:\n" + str(error))


    def _check_file(self):
        pass

    def load(self, path, remote_name=None):
        with open(path, 'r') as remotefile:
            try:
                self.remote_yaml = yaml.load(remotefile)
            except yaml.YAMLError as error:
                raise Exception("YAML format wrong in 'remote.yaml' file - %s." % str(error).capitalize())
            try:
                if remote_name is None:
                    if len(self.remote_yaml) == 1:
                        self.name = self.remote_yaml.keys()[0]
                    else:
                        self.name = self.remote_yaml["default"]
                else:
                    self.name = remote_name
            except KeyError as error:
                raise Exception("Default remote not specified. Add 'default: remote_name' to remote.yaml file.")
            try:
                self.remote_yaml = self.remote_yaml[self.name]
            except KeyError as error:
                raise Exception("Remote '%s' not found in 'remote.yaml' file." % self.name)
        self._unpack_remote_yaml(self.remote_yaml)

    def _unpack_remote_yaml(self, yaml_remote):
        try:
            self.addr = yaml_remote["address"]
            self.port = yaml_remote["port"]
            self.workdir = yaml_remote["remote-workdir"]
            self.username = yaml_remote["username"]
            self.resource_manager = yaml_remote["resource-manager"]
        except KeyError as e:
            raise Exception("Field %s not found in remote file." % str(e))
        # Optional params
        try:
            self.key_login = yaml_remote["key-login"]
        except Exception:
            pass


    def save(self, path):
        remotedata = {"remote": {}}
        try:
            remotedata["remote"]["name"] = self.name
            remotedata["remote"]["address"] = self.addr
            remotedata["remote"]["port"] = self.port
            remotedata["remote"]["remote-workdir"] = self.workdir
            remotedata["remote"]["username"] = self.username
            remotedata["remote"]["shell"] = self.shell
        except Exception:
            raise Exception("Field %s not defined." % str(e))
        try:
            remotedata["remote"]["key-login"] = self.key_login
        except Exception:
            pass

        with open('%s.yaml', 'w') as remotefile:
            yaml.dump(remotedata, remotefile, default_flow_style=False)

    def available(self, timeout=60):
        try:
            self.connect(passwd="", timeout=timeout)
        except paramiko.AuthenticationException:
            pass
        except socket.timeout:
            raise ConnectionTimeout("Connection time-out after 60 seconds.")
        finally:
            self.close()

    #TODO: Check python version (>2.7.* ???)
    def init_remote(self):
        if not self.remote_dir_exists(self.workdir):
            time.sleep(1)
            self.command("mkdir -p %s" % self.workdir)
            self.print_msg("Remote workdir created.")
        else:
            raise Exception("Directory %s already exists in remote '%s'." % (self.workdir, self.name))
        cmd_not_available = False
        self.print_msg("Checking remote dependencies...")
        if not self.cmd_avail("qsub"):
            self.print_msg("Warning: Command 'qsub' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if not self.cmd_avail("qstat"):
            self.print_msg("Warning: Command 'qstat' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if not self.cmd_avail("qdel"):
            self.print_msg("Warning: Command 'qdel' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if cmd_not_available:
            self.print_msg("Info: Sometimes it is necessary to add the path where the\n" +\
                  "      binaries qsub/qstat/qdel are located in the remote to the\n" +\
                  "      ~/.bashrc or ~/.cshrc files.", ignore_quiet=True)
        else:
            self.print_msg("Done.")

    def connect(self, passwd=None, timeout=None, progress_callback=None):
        self._progress_callback = progress_callback
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.key_login:
            self.ssh.connect(self.addr, port=self.port, timeout=timeout)
        else:
            self.ssh.connect(self.addr, port=self.port, username=self.username,\
                             password=passwd, timeout=timeout)
        self.scp = SCPClient(self.ssh.get_transport(), socket_timeout=60.0, progress=self._progress_callback)
        self.cmd = CommandExecuter(self.ssh)

    def command(self, cmd, timeout=None, fail_on_error=True):
        stdin, stdout, stderr, exit_status = self.cmd.exec_command(cmd) #, timeout=timeout)
        self.command_status = exit_status #stdout.channel.recv_exit_status()
        # stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=timeout)
        # self.command_status = stdout.channel.recv_exit_status()
        if fail_on_error:
            if self.command_status != 0:
                # error = stderr.readlines()
                error = stderr
                raise CmdExecutionError("".join([l for l in error if l]))
        # print "lines:" , stdout.readlines()
        # return stdout.readlines()
        return stdout

    def cmd_avail(self, cmd_name): 
        try:
            output = self.command("which  %s" % cmd_name, timeout=60)
        except CmdExecutionError:
            return False
        return True
    
    def upload(self, path_orig, path_dest):
        self.scp.put(path_orig, path_dest)

    def download(self, path_orig, path_dest):
        self.scp.get(path_orig, path_dest)

    def remote_file_exists(self, f):
        try:
            out = self.command("[ -f %s ]" % f)
        except CmdExecutionError:
            return False
        return True

    def remote_dir_exists(self, d):
        try:
            out = self.command("[ -d %s ]" % d)
        except CmdExecutionError:
            return False
        return True

    #TODO: Add debug flag GLOBALLY so remote can access it
    def close(self):
        if self.scp is not None:
            self.scp.close()
        self.ssh.close()

class RemoteDirExists(Exception):
    pass

class RemoteFileExists(Exception):
    pass


class StudyManager(MessagePrinter):
    def __init__(self, study, verbose=False, quiet=False):
        super(StudyManager, self).__init__(quiet, verbose)
        self.DEFAULT_UPLOAD_FILES = ["manage.py", "cases.info", "README"]
        self.tmpdir = "/tmp"
        self.study = study
        self.verbose = verbose
        self.quiet = quiet
     
    def _upload(self, remote, name, base_path, upload_cases, keep_targz=False, force=False):
        if not remote.remote_dir_exists(remote.workdir):
            raise Exception("Remote work directory '%s' do not exists. Use '--init-remote' command to create it." % remote.workdir)
        remotedir = os.path.join(remote.workdir, name)
        if remote.remote_dir_exists(remotedir):
            raise Exception("Remote study directory '%s' already exists. Use 'remote-delete' command to delete it." % remotedir)
        self.print_msg("Checking remote state...")
        for case in upload_cases:
            remote_casedir = os.path.join(remotedir, case)
            if remote.remote_dir_exists(remote_casedir) and not force:
                raise RemoteDirExists("Study '%s' - Case directory '%s' already exists in remote '%s'."\
                                      % (self.study.name, case, remote.name))
        upload_files = upload_cases + self.DEFAULT_UPLOAD_FILES
        self.print_msg("Compressing study...")
        tar_name = self._compress(name, base_path, upload_files)
        upload_src = os.path.join(self.tmpdir, tar_name)
        upload_dest = remote.workdir
        self.print_msg("Uploading study...")
        remote.upload(upload_src, upload_dest)
        extract_src = os.path.join(upload_dest, tar_name)
        extract_dest = upload_dest
        self.print_msg("Extracting study in remote...")
        try:
            out = remote.command("tar -xzf %s --directory %s --warning=no-timestamp" % (extract_src, extract_dest))
            # For older versions of tar. Not sure how they will handle the timestamp issue though.
        except Exception as error:
            try:
                out = remote.command("tar -xzf %s --directory %s" % (extract_src, extract_dest))
            except Exception:
                raise Exception("Unable to decompress '%s.tar.gz' in remote. Check version of 'tar' command in the remote." % tar_name)
        self.print_msg("Cleaning...")
        os.remove(upload_src)
        if not keep_targz:
            out = remote.command("rm -f %s" % extract_src)

    
    def upload(self, remote, array_job=False, keep_targz=True, force=False):
        params = {"PARAMPY-CD": "",
                  "PARAMPY-CN": "", 
                  "PARAMPY-RWD": remote.workdir, 
                  "PARAMPY-LWD": os.path.dirname(self.study.path), 
                  "PARAMPY-SN": self.study.name,
                  "PARAMPY-SD": self.study.path}
        template_script_path = os.path.join(self.study.path, "submit.%s.sh" % remote.name)
        submit_script_path = ""
        upload_cases = self.study.case_selection
        # Check if the case state is compatible with uploading
        # TODO: There is something not alright here

        # for case in upload_cases:
        #     if not (case.remote is None and case.status == "CREATED"):
        #         msg = ""
        #         if case.status == "UPLOADED":
        #             msg = "Case '%s' has already been uploaded to remote '%s'." % (case.name, remote.name)
        #         elif case.status == "SUBMITTED":
        #             msg = "Case '%s' has already been submitted to remote '%s'." % (case.name, remote.name)
        #         elif case.status == "FINISH":
        #             msg = "Case '%s' has already finished execution in remote '%s'." % (case.name, remote.name)
        #         elif case.status == "DOWNLOADED":
        #             msg = "Case '%s' has already been downloaded from remote '%s'." % (case.name, remote.name)
        #         msg += "\nInfo: Use '--clean' option to reset case to a creation state."
        #         raise Exception(msg)
        #
        # Create submission scripts
        if os.path.exists(template_script_path):
            if array_job:
                submit_script_path = os.path.join(self.study.path, "submit_arrayjob.sh")
                shutil.copy(template_script_path, submit_script_path)
                remote_study_path = os.path.join(remote.workdir, self.study.name)
                params["PARAMPY-CN"] = "$(python %s/manage.py case-param $PBS_ARRAY_INDEX name)" % remote_study_path
                params["PARAMPY-CD"] = os.path.join(self.study.path, params["PARAMPY-CN"])
                try:
                    replace_placeholders([submit_script_path], params)
                except Exception:
                    os.remove(submit_script_path)
                    raise
            for case in upload_cases:
                case_path = os.path.join(self.study.path, case.name)
                submit_script_path = os.path.join(case_path, "submit.sh")
                shutil.copy(template_script_path, submit_script_path)
                params["PARAMPY-CN"] = case.name
                params["PARAMPY-CD"] = case_path
                params.update(case.params)
                # TODO: ADD here params which are single valued in params.yaml
                try:
                    replace_placeholders([submit_script_path], params)
                except Exception:
                    os.remove(submit_script_path)
                    raise
        else:
            raise Exception("Submission script 'submit.%s.sh' not found in study directory." % self.remote.name)


        # Set cases as uploaded
        for case in upload_cases:
            case.status = "UPLOADED"
            case.remote = remote.name
        self.study.study_file.backup(self.tmpdir)
        # Modify the study file so it is uploaded updated.
        try:
            self.study.save()
            upload_paths = [case.name for case in upload_cases]
            if array_job:
                upload_paths.append("submit_arrayjob.sh")

            self._upload(remote, self.study.name, self.study.path, upload_paths, keep_targz, force)
        except Exception:
            self.study.study_file.restore(self.tmpdir)
            raise


    def _compress(self, name, base_path, upload_paths):
        tar_name = name + ".tar.gz"
        with tarfile.open(os.path.join(self.tmpdir, tar_name), "w:gz") as tar:
            for path in upload_paths:
                tar.add(os.path.join(base_path, path), arcname=os.path.join(name, path))
        return tar_name

    def _decompress(self, src_path, dest_path):
        with tarfile.open(src_path, "r:gz") as tar:
            tar.extractall(dest_path)
        
    def _cases_regexp(self):
        regexp = ""
        for case in self.study.case_selection:
            regexp += "%0*d" % (len(str(self.study.nof_cases-1)), case.id)
            if not case.short_name:
                regexp += "_"
            regexp += "*,"
        if regexp:
            regexp =  "{" + regexp.rstrip() + "}"
        return regexp


    def submit(self, remote, force=False, array_job=False):
        remote_studydir = os.path.join(remote.workdir, self.study.name)
        if not remote.remote_dir_exists(remote_studydir):
            error = "Study '%s' not found in remote '%s'. Upload it first.\n" % (self.study_name, self.remote.name)
            error += "NOTE: Sometimes NFS filesystems take a while to syncronise.\n" +\
                     "      If you are sure the study is uploaded, wait a bit and retry submission."
            raise Exception(error)
        if not remote.cmd_avail("qsub"):
            raise Exception("Command 'qsub' not available in remote '%s'." % remote.name)
        if array_job:
           pass
        else:
            nof_submitted = 0
            awk_cmd = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
            for case in self.study.case_selection:
                time.sleep(0.01)
                try:
                    remote_casedir = os.path.join(remote_studydir, case.name)
                    output = remote.command("cd %s && qsub submit.sh | %s" % (remote_casedir, awk_cmd), timeout=10)
                    case.job_id = output[0].rstrip() 
                    case.status = "SUBMITTED"
                    case.submission_date = time.strftime("%c")
                    nof_submitted += 1
                    print "Submitted case '%s' (%d/%d)." % (case.name, nof_submitted, len(self.study.case_selection))
                except Exception:
                    # Save if some jobs has been submitted before the error
                    self.study.save()
                    raise

            self.study.save()

    def update_status(self, remote):
        if not remote.cmd_avail("qstat"):
            raise Exception("Command 'qstat' not available in remote '%s'." % self.remote.name)
        awk = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
        output = remote.command("qstat | %s" % awk, timeout=60)
        job_ids  = [jid.rstrip() for jid in output]
        for case in self.study.case_selection:
            if not (case.id in job_ids) and case.status == "SUBMITTED":
                case.status = "FINISHED"
        self.study.save()


    def download(self, remote, force=False):
        remote_studydir = os.path.join(remote.workdir, self.study.name)
        if not remote.remote_dir_exists(remote_studydir):
            raise Exception("Study '%s' does not exists in remote '%s'." % (self.study_name, self.remote.name))
        compress_dirs = ""
        cases_regexp = self._cases_regexp()
        for path in self.study.param_file["DOWNLOAD"]:
            include_list = []
            exclude_list = []
            path_name = path["path"]
            # TODO: Move checks of params.yaml to the Sections checkers in parampy.py
            include_exists = "include" in path
            exclude_exists = "exclude" in path
            path_wildcard = os.path.join(cases_regexp, path["path"])
            if exclude_exists and include_exists:
                raise Exception("Both 'exclude' and 'include' defined for download path '%s'."\
                                % path["path"])
            else:
                if include_exists:
                    include_list = [os.path.join(path_wildcard, f) for f in path["include"]]
                    compress_dirs += " " + " ".join(include_list)
                elif exclude_exists:
                    exclude_list = path["exclude"]
                    for f in exclude_list:
                        compress_dirs += " --exclude=%s" % f
                    compress_dirs += " " + path_wildcard
                else:
                    compress_dirs += " " + path_wildcard

        compress_src = os.path.join(remote_studydir, self.study.name + ".tar.gz")
        tar_cmd = "tar -czf %s %s" % (compress_src, compress_dirs)
        #TODO: REMOVE THIS
        force = True
        self.print_msg("Compressing study...")
        try:
            if force:
                tar_cmd += " --ignore-failed-read"
            remote.command("cd %s && %s" % (remote_studydir, tar_cmd) ,\
                           fail_on_error=False, timeout=60)
        except Exception as error:
            if remote.command_status != 0:
                remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=60)
                raise Exception(error)
        self.print_msg("Downloading study...")
        remote.download(compress_src, self.study.path)
        self.print_msg("Decompressing study...")
        tar_path = os.path.join(self.study.path, self.study.name) + ".tar.gz"
        self._decompress(tar_path, self.study.path)
        for case in self.study.case_selection:
            case.status = "DOWNLOADED"
        self.study.save()
        self.print_msg("Cleaning...")
        remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=60)
