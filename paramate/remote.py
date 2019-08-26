import paramiko
from paramiko.dsskey import DSSKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from paramiko.hostkeys import HostKeys
from paramiko.rsakey import RSAKey
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
from common import replace_placeholders, _printer
import re


#TODO: Refactor Remote to separate configuration-related stuff
SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")
DEFAULT_DOWNLOAD_DIRS = ["output", "postproc"]

class CommandExecuter:

    def __init__(self, ssh):
        self.ssh = ssh
        channel = self.ssh.invoke_shell(width=2000)
        self.stdin = channel.makefile('wb')
        self.stdout = channel.makefile('r')
        # This is to avoid welcome messages of SSH servers to interfere in output
        self.exec_command("echo Paramate SSH session started.")

    def exec_command(self, cmd):
        """

        :param cmd: the command to be executed on the remote computer
        :examples:  execute('ls')
                    execute('finger')
                    execute('cd folder_name')
        """
        cmd = cmd.strip('\n')
        cmd_end  = "\\x4"
        echo_cmd = "{} ; echo $? ; printf '{}\\n'".format(cmd, cmd_end)
        self.stdin.write(echo_cmd + '\n')
        shin = self.stdin
        self.stdin.flush()
        shout = []
        sherr = []
        exit_status = 0
        for line in self.stdout:
            rep_line = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]').sub('', line).replace('\b', '').replace('\r', '')
            # print rep_line
            if u"\u0004" in rep_line:  
                exit_status = int(str(shout.pop()).rsplit(None, 1)[0])
                if exit_status:
                    sherr = shout
                    shout = []
                break
            elif cmd not in rep_line:
                shout.append(rep_line)
        return shin, shout, sherr, exit_status


class CmdExecutionError(Exception):
    pass

class ConnectionTimeout(Exception):
    pass

class Remote():
    def __init__(self, name="", workdir=None, hostname=None,\
            port=22, user=None, ssh_key_file=None, shell="bash",\
            lookup_keys=False, allow_agent=True, resource_manager=None):
        self.name = name
        self.ssh_key_file = ssh_key_file
        self.lookup_keys = lookup_keys
        self.allow_agent = allow_agent 
        self.hostname = hostname
        self.port = port
        self.user = user
        self.workdir = workdir
        self.shell = shell
        self.resource_manager = resource_manager
        self.ssh = SSHClient()
        try:
            self.ssh.load_system_host_keys()
        except:
            pass
        self.command_status = None
        self.scp = None
        self._progress_callback = None
        self.cmd = None
        self.auth_type = "password"
        self.remote_linux_commands = ["mkdir", "rm", "cd", "tar", "which", "qstat", "qdel", "qsub"]

    def __del__(self):
        self.ssh.close()

    def passphrase_required(self):
        if self.ssh_key_file is None:
            return False
        for key_type in (DSSKey, ECDSAKey, Ed25519Key, RSAKey):
            try:
                pkey = key_type.from_private_key_file(self.ssh_key_file, password=None)
            except paramiko.ssh_exception.PasswordRequiredException:
                return True
            except paramiko.ssh_exception.SSHException:
                pass
        return False
        
    def configure(self, remote_name, yaml_remote):
        # Mandatory fields
        self.name = remote_name
        self.workdir = os.path.normpath(yaml_remote["remote-workdir"])
        self.resource_manager = yaml_remote["resource-manager"]
        if "config-host" in yaml_remote:
            host = yaml_remote["config-host"]
            host_data = {}
            with open("/home/eduardo/.ssh/config", 'r') as config_file:
                sshconfig = paramiko.config.SSHConfig()
                sshconfig.parse(config_file)
                if host in sshconfig.get_hostnames():
                    host_data = sshconfig.lookup(host)
                else:
                    raise Exception("Host '{}' not found in '$HOME/.ssh/config'.".format(host))
            # Mandatory
            try:
                self.user = host_data["user"]
                self.hostname = host_data["hostname"]
            except KeyError as e:
                raise Exception("Field %s not found in host '{}' at '$HOME/.ssh/config'.".format(str(e)))
            # Optional
            if "port" in host_data.keys():
                self.port = host_data["port"]
            if "identityfile" in host_data.keys():
                self.auth_type = "key"
                self.ssh_key_file = host_data["identityfile"][0]
            if "identitiesonly" in host_data.keys():
                self.lookup_keys = not host_data["identitiesonly"]

        else:
            # Mandatory
            self.hostname = yaml_remote["hostname"]
            self.user = yaml_remote["user"]
            # Optional
            if "port" in yaml_remote.keys():
                self.port = yaml_remote["port"]
            if "ssh-key" in yaml_remote.keys():
                self.auth_type = "key"
                self.ssh_key_file = yaml_remote["ssh-key"]["file"]
                if "lookup-keys" in yaml_remote["ssh-key"].keys():
                    self.lookup_keys = yaml_remote["ssh-key"]["lookup-keys"]
                if "allow-agent" in yaml_remote["ssh-key"].keys():
                    self.allow_agent = yaml_remote["ssh-key"]["allow-agent"]
        # Mandatory
        self.workdir = os.path.normpath(yaml_remote["remote-workdir"])
        self.resource_manager = yaml_remote["resource-manager"]
        self.shell = yaml_remote["shell"]

        # Optional
        if "shell" in yaml_remote.keys():
            self.shell = yaml_remote["shell"]
        if "jobs-commands" in yaml_remote.keys():
            self.jobs_commands = yaml_remote["jobs-commands"]


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
            _printer.print_msg("Remote workdir created.")
        else:
            raise Exception("Directory %s already exists in remote '%s'." % (self.workdir, self.name))
        cmd_not_available = False
        _printer.print_msg("Checking remote dependencies...")
        if not self.cmd_avail("qsub"):
            _printer.print_msg("Warning: Command 'qsub' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if not self.cmd_avail("qstat"):
            _printer.print_msg("Warning: Command 'qstat' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if not self.cmd_avail("qdel"):
            _printer.print_msg("Warning: Command 'qdel' not available in '%s'." % self.name, ignore_quiet=True)
            cmd_not_available = True 
        if cmd_not_available:
            _printer.print_msg("Info: Sometimes it is necessary to add the path where the\n" +\
                  "      binaries qsub/qstat/qdel are located in the remote to the\n" +\
                  "      ~/.bashrc or ~/.cshrc files.", ignore_quiet=True)

    def connect(self, passwd=None, timeout=None, progress_callback=None):
        self._progress_callback = progress_callback
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.hostname, port=self.port, password=passwd, timeout=timeout, username=self.user,\
                         key_filename=self.ssh_key_file, look_for_keys=self.lookup_keys)
        self.scp = SCPClient(self.ssh.get_transport(), socket_timeout=60.0, progress=self._progress_callback)
        self.cmd = CommandExecuter(self.ssh)
        # Unalias all the commands to avoid unexpected behaviour
        for remote_cmd in self.remote_linux_commands:
            cmd = "unalias {}".format(remote_cmd)
            self.command(cmd, timeout=60, fail_on_error=False)

    # def command(self, cmd, timeout=None, fail_on_error=True):
    #     stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=timeout)
    #     self.command_status = stdout.channel.recv_exit_status()
    #     self.command_status = stdout.channel.recv_exit_status()
    #     if fail_on_error:
    #         if self.command_status != 0:
    #             error = stderr.readlines()
    #             raise CmdExecutionError("".join([l for l in error if l]))
    #     return stdout.readlines()

    def command(self, cmd, timeout=None, fail_on_error=True):
        stdin, stdout, stderr, exit_status = self.cmd.exec_command(cmd)
        self.command_status = exit_status
        if fail_on_error:
            if self.command_status != 0:
                error = stderr
                raise CmdExecutionError("".join([l for l in error if l]))
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

class StudyManager():
    def __init__(self, study):
        self.DEFAULT_UPLOAD_FILES = ["cases.info", "README", "generators.py", "params.yaml", "postproc.py", "upload"]
        self.tmpdir = "/tmp"
        self.study = study
     
    def _upload(self, remote, name, base_path, upload_cases, keep_targz=False, force=False):
        if not remote.remote_dir_exists(remote.workdir):
            raise Exception("Remote work directory '%s' do not exists. Use 'remote-init' command to create it." % remote.workdir)
        remotedir = os.path.join(remote.workdir, name)
        _printer.print_msg("Checking remote state...")
        for case in upload_cases:
            remote_casedir = os.path.join(remotedir, case)
            if remote.remote_dir_exists(remote_casedir) and not force:
                raise RemoteDirExists("Study '%s' - Case directory '%s' already exists in remote '%s'."\
                                      % (self.study.name, case, remote.name))
        upload_files = upload_cases + self.DEFAULT_UPLOAD_FILES
        _printer.print_msg("Compressing study...")
        tar_name = self._compress(name, base_path, upload_files)
        upload_src = os.path.join(self.tmpdir, tar_name)
        upload_dest = remote.workdir
        remote.upload(upload_src, upload_dest)
        extract_src = os.path.join(upload_dest, tar_name)
        extract_dest = upload_dest
        _printer.print_msg("Extracting study in remote...")
        try:
            out = remote.command("tar -xzf %s --directory %s --warning=no-timestamp" % (extract_src, extract_dest))
            # For older versions of tar. Not sure how they will handle the timestamp issue though.
        except Exception as error:
            try:
                out = remote.command("tar -xzf %s --directory %s" % (extract_src, extract_dest))
            except Exception:
                raise Exception("Unable to decompress '%s.tar.gz' in remote. Check version of 'tar' command in the remote." % tar_name)
        _printer.print_msg("Cleaning...")
        os.remove(upload_src)
        if not keep_targz:
            out = remote.command("rm -f %s" % extract_src)

    
    def upload(self, remote, array_job=False, keep_targz=False, force=False):
        params = {"PARAMATE-CD": "",
                  "PARAMATE-CN": "", 
                  "PARAMATE-RWD": remote.workdir, 
                  "PARAMATE-REMOTE": remote.name, 
                  "PARAMATE-LWD": os.path.dirname(self.study.path), 
                  "PARAMATE-SN": self.study.name,
                  "PARAMATE-SD": self.study.path}
        template_script_path = os.path.join(self.study.path, "submit.%s.sh" % remote.name)
        submit_script_path = ""
        upload_cases = self.study.case_selection

        # Create submission scripts
        if os.path.exists(template_script_path):
            if array_job:
                submit_script_path = os.path.join(self.study.path, "submit_arrayjob.sh")
                shutil.copy(template_script_path, submit_script_path)
                remote_study_path = os.path.join(remote.workdir, self.study.name)
                params["PARAMATE-CN"] = "$(python %s/manage.py case-param $PBS_ARRAY_INDEX name)" % remote_study_path
                params["PARAMATE-CD"] = os.path.join(self.study.path, params["PARAMATE-CN"])
                try:
                    replace_placeholders([submit_script_path], params)
                except Exception:
                    os.remove(submit_script_path)
                    raise
            for case in upload_cases:
                case_path = os.path.join(self.study.path, case.name)
                submit_script_path = os.path.join(case_path, "submit.sh")
                shutil.copy(template_script_path, submit_script_path)
                params["PARAMATE-CN"] = case.name
                params["PARAMATE-CD"] = case_path
                params.update(case.params)
                params.update(case.singleval_params)
                try:
                    replace_placeholders([submit_script_path], params)
                except Exception:
                    os.remove(submit_script_path)
                    raise
        else:
            raise Exception("Submission script 'submit.%s.sh' not found in study directory." % remote.name)


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


    def job_submit(self, remote, array_job=False):
        remote_studydir = os.path.join(remote.workdir, self.study.name)
        if not remote.remote_dir_exists(remote_studydir):
            error = "Study '%s' not found in remote '%s'. Upload it first.\n" % (self.study.name, remote.name)
            error += "NOTE: Sometimes NFS filesystems take a while to syncronise.\n" +\
                     "      If you are sure the study is uploaded, wait a bit and retry submission."
            raise Exception(error)
        if not remote.cmd_avail("qsub"):
            raise Exception("Command 'qsub' not available in remote '%s'." % remote.name)
        if array_job:
           pass
        else:
            nof_submitted = 0
            # awk_cmd = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
            for case in self.study.case_selection:
                try:
                    remote_casedir = os.path.join(remote_studydir, case.name)
                    output = remote.command("cd {} && qsub submit.sh".format(remote_casedir), timeout=10)
                except Exception as err:
                    # Save if some jobs has been submitted before the error
                    self.study.save()
                    raise Exception("While submitting case '{}': '{}'.".format(case.id, str(err).replace('\n', '')))
                try:
                    case.job_id = self._extract_job_id(output, case.id)
                    print "out:", output, case.job_id
                    case.status = "SUBMITTED"
                    case.submission_date = time.strftime("%c")
                    nof_submitted += 1
                    _printer.print_msg("Submitted case '%s' (%d/%d)." % (case.name, nof_submitted, len(self.study.case_selection)))
                except Exception as err:
                    # Save if some jobs has been submitted before the error
                    self.study.save()
                    raise
            self.study.save()

    def _extract_job_id(self, output, case_id=None):
        id_extracted = True
        # Output should not be empty and integer job id should be able to be extracted
        if output:
            job_id = re.search('\d+', output[0])
            if job_id is not None:
                job_id = job_id.group()
            else:
                id_extracted = False
        else:
            id_extracted = False
        if not id_extracted:
            if case_id is not None:
                raise Exception("Job id from case '{}' could not be extracted from: '{}'".format(case_id, output))
            else:
                raise Exception("Job id could not be extracted from: '{}'".format(output))
        return job_id


    def update_status(self, remote):
        if not remote.cmd_avail("qstat"):
            raise Exception("Command 'qstat' not available in remote '%s'." % remote.name)
        awk = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
        output = remote.command("qstat | {}".format(awk), timeout=60)
        job_ids  = [self._extract_job_id([line]) for line in output]
        remote_case_list = self.study.get_cases([remote.name], "remote")
        for case in remote_case_list:
            if not (case.job_id in job_ids) and case.status == "SUBMITTED":
                case.status = "FINISHED"
        self.study.save()
        # return jobs_updated
        return job_ids

    def job_status(self, remote):
        if not remote.cmd_avail("qstat"):
            raise Exception("Command 'qstat' not available in remote '%s'." % remote.name)
        awk = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
        output = remote.command("qstat | %s" % awk, timeout=60)
        job_ids  = [jid.rstrip() for jid in output]
        output = remote.command("qstat", timeout=60)
        selected_cases_idx = [case.job_id for case in self.study.case_selection]
        filter_idx = [job_ids.index(jid) for jid in job_ids if jid in selected_cases_idx]
        header_lines = 2
        filtered_output = [output[j+header_lines] for j in filter_idx]
        filtered_output.insert(0, output[0])
        filtered_output.insert(1, output[1])
        return filtered_output

    def job_delete(self, remote):
        if not remote.cmd_avail("qdel"):
            raise Exception("Command 'qdel' not available in remote '%s'." % remote.name)
        jobid_list_str = " ".join([c.job_id for c in self.study.case_selection])
        # print "jobids:", jobid_list_str
        output = remote.command("qdel {}".format(jobid_list_str), timeout=60)
        while self.update_status(remote):
            time.sleep(1)
        for case in self.study.case_selection:
            case.status = "DELETED"
        self.study.save()
        # Return the number of cases marked for deletion
        return len(self.study.case_selection)

    def download(self, remote, force=False, compress_only=False):
        remote_studydir = os.path.join(remote.workdir, self.study.name)
        if not remote.remote_dir_exists(remote_studydir):
            raise Exception("Study '%s' does not exists in remote '%s'." % (self.study.name, remote.name))
        compress_dirs = ""
        cases_regexp = self._cases_regexp()
        for path in self.study.param_file["DOWNLOAD"]:
            include_list = []
            exclude_list = []
            path_name = path["path"]
            # TODO: Move checks of params.yaml to the Sections checkers in PARAMATE.py
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
        _printer.print_msg("Compressing study...")
        try:
            if force:
                tar_cmd += " --ignore-failed-read"
            remote.command("cd %s && %s" % (remote_studydir, tar_cmd) ,\
                           fail_on_error=False, timeout=60)
        except Exception as error:
            if remote.command_status != 0:
                remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=60)
                raise Exception(error)
        if not compress_only:
            remote.download(compress_src, self.study.path)
            _printer.print_msg("Decompressing study...")
            tar_path = os.path.join(self.study.path, self.study.name) + ".tar.gz"
            self._decompress(tar_path, self.study.path)
            for case in self.study.case_selection:
                case.status = "DOWNLOADED"
            self.study.save()
            _printer.print_msg("Cleaning...")
            remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=60)
