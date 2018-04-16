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
from parampy import StudyFile, ParamFile, replace_placeholders


#TODO: Refactor Remote to separate configuration-related stuff
SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")
CONFIG_DIR = os.path.join(os.getenv("HOME"), ".parampy")
DEFAULT_DOWNLOAD_DIRS = ["output", "postproc"]

class Remote:
    def __init__(self, name="", workdir=None, addr=None,\
                 port=22, username=None, key_login=False,shell="bash"):
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

    def available(self, timeout=5):
        try:
            self.connect(passwd="", timeout=5)
        except paramiko.AuthenticationException:
            return True
        except socket.timeout:
            return False
        else:
            return False

    def connect(self, passwd=None, timeout=None):
        if self.key_login:
            self.ssh.connect(self.addr, port=self.port, timeout=timeout)
        else:
            self.ssh.connect(self.addr, port=self.port, username=self.username,\
                             password=passwd, timeout=timeout)
        self.scp = SCPClient(self.ssh.get_transport())

    def command(self, cmd, timeout=None, close_on_error=True):
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=timeout)
        self.command_status = stdout.channel.recv_exit_status()
        error = stderr.readlines()
        if error:
            if close_on_error:
                self.close()
            raise Exception("".join([l for l in error if l]))
        return stdout.readlines()

    def cmd_avail(self, cmd_name): 
        output = self.command("whereis %s" % cmd_name, timeout=10)
        result = output[0].rstrip().split(":")
        if len(result) == 1:
            return False
        return True
    
    def upload(self, path_orig, path_dest):
        self.scp.put(path_orig, path_dest)

    def download(self, path_orig, path_dest):
        self.scp.get(path_orig, path_dest)

    def remote_file_exists(self, f):
        out = self.command("[ -f %s ]" % f)
        return not self.command_status

    def remote_dir_exists(self, d):
        out = self.command("[ -d %s ]" % d)
        return not self.command_status

    def close(self):
        if self.scp is not None:
            self.scp.close()
        self.ssh.close()

    def check_connection(self):
        pass

class RemoteDirExists(Exception):
    pass

class RemoteFileExists(Exception):
    pass


class StudyManager:
    def __init__(self, remote, study_path=None, case_path=None):
        assert study_path is not None or case_path is not None
        self.remote = remote
        if study_path is not None:
            self.study_path = os.path.abspath(study_path)
            self.study_file = StudyFile(path=self.study_path)
        else:
            self.study_path = None
        self.case_path = case_path
        self.case_name = None
        self.DEFAULT_UPLOAD_FILES = ["manage.py", "cases.info", "README", "submit_arrayjob.sh"]
        if case_path is not None:
            self.case_path = os.path.abspath(self.case_path)
            self.case_name = os.path.basename(self.case_path)
            self.study_path = os.path.dirname(self.case_path)
            self.study_name = os.path.basename(self.study_path)
            self.study_file = StudyFile(path=self.study_path)
            # Check if the cases.info has been generated. Meaning it is a study.
            if not self.study_file.exists(self.study_path):
                self.study_name = "default"
        else:
            self.study_name = os.path.basename(self.study_path)
        self.tmpdir = "/tmp"
        self.param_file = ParamFile()
        self.study_file.read()
     
    def _upload(self, name, base_path, upload_paths, keep_targz=False, force=False, workdir=None):
        print upload_paths
        if workdir is None:
            workdir = self.remote.workdir
        remotedir = os.path.join(workdir, name)
        if self.remote.remote_dir_exists(remotedir):
            if not force:
                raise RemoteDirExists("")
        tar_name = self._compress(name, base_path, upload_paths)
        upload_src = os.path.join(self.tmpdir, tar_name)
        upload_dest = workdir
        self.remote.upload(upload_src, upload_dest)
        extract_src = os.path.join(upload_dest, tar_name)
        extract_dest = upload_dest
        try:
            out = self.remote.command("tar -xzf %s --directory %s --warning=no-timestamp" % (extract_src, extract_dest), close_on_error=False)
            # For older versions of tar. Not sure how they will handle the timestamp issue though.
        except Exception as error:
            out = self.remote.command("tar -xzf %s --directory %s" % (extract_src, extract_dest))

        
        os.remove(upload_src)
        if not keep_targz:
            out = self.remote.command("rm -f %s" % extract_src)

    def upload_case(self, keep_targz=False, force=False):
        # self._case_clean()
        workdir = os.path.join(self.remote.workdir, self.study_name)
        if not self.remote.remote_dir_exists(workdir):
            out = self.remote.command("mkdir %s" % workdir)
        try:
            self._upload(self.case_name, self.case_path, keep_targz, force, workdir)
        except RemoteDirExists:
            raise RemoteDirExists("Case '%s' already exists in study '%s' in the remote '%s'." % (self.case_name,self.study_name, self.remote.name))


    # def _get_arrayjob_shell_cmd(path, nof, shortname=False, shell="bash", queue="pbs"):
    #     nof = len(str(self.nof_instances))
    #     array_idx_var = ""
    #     if queue == "pbs":
    #         array_idx_var = "PBS_ARRAY_INDEX"
    #     s = ""
    #     if shell == "bash":
    #         if shortname:
    #             s = '$(printf "%%0*d" %s $%s)' % (nof, array_idx_var)
    #         else:
    #             s = '$(printf "%%0*d" %s $%s)_$(cat cases.info| grep -e ' % (nof, array_idx_var) +\
    #                     'grep -e "^$(printf "%%0*d" %s $%s)"  |' +\
    #                     'sed -r "s/\(\"([^\"]+)\"\)/_\1/g" | cut -d : -f2)' % (nof, array_idx_var)
    #     return s
    #

    def upload_study(self, cases_idx=None, array_job=False, keep_targz=False, force=False):
        try:
            params = {"PARAMPY-CD": "",
                      "PARAMPY-CN": "", 
                      "PARAMPY-RWD": self.remote.workdir, 
                      "PARAMPY-LWD": os.path.dirname(self.study_name), 
                      "PARAMPY-SN": self.study_name,
                      "PARAMPY-SD": self.study_path}
            template_script_path = os.path.join(self.study_path, "submit.%s.sh" % self.remote.name)
            submit_script_path = ""
            if cases_idx is None:
                cases_idx = list(xrange(1, self.study_file.nof_cases+1))
            upload_cases = self.study_file.get_cases(cases_idx, "id")
            # Check if the case state is compatible with uploading
            for case in upload_cases:
                if not (case["remote"] is None and case["status"] == "CREATED"):
                    msg = ""
                    if case["status"] == "UPLOADED":
                        msg = "Case '%s' has already been uploaded to remote '%s'." % (case["name"], self.remote.name)
                    elif case["status"] == "SUBMITTED":
                        msg = "Case '%s' has already been submitted to remote '%s'." % (case["name"], self.remote.name)
                    elif case["status"] == "FINISH":
                        msg = "Case '%s' has already finished execution in remote '%s'." % (case["name"], self.remote.name)
                    elif case["status"] == "DOWNLOADED":
                        msg = "Case '%s' has already been downloaded from remote '%s'." % (case["name"], self.remote.name)
                    msg += "\nINFO: Use '--clean' option to reset case to a creation state."
                    raise Exception(msg)
            if os.path.exists(template_script_path):
                if array_job:
                    submit_script_path = os.path.join(self.study_path, "submit_array.sh")
                    shutil.copy(template_script_path, submit_script_path)
                    remote_study_path = os.path.join(self.remote.workdir, self.study_name)
                    params["PARAMPY-CN"] = "$(python2 %s/manage.py case-param $PBS_ARRAY_INDEX name)" % remote_study_path
                    params["PARAMPY-CD"] = os.path.join(self.study_path, params["PARAMPY-CN"])
                    try:
                        replace_placeholders([submit_script_path], params)
                    except Exception:
                        os.remove(submit_script_path)
                        raise
                else:
                    for case in upload_cases:
                        case_path = os.path.join(self.study_path, case["name"])
                        submit_script_path = os.path.join(case_path, "submit.sh")
                        shutil.copy(template_script_path, submit_script_path)
                        params["PARAMPY-CN"] = case["name"]
                        params["PARAMPY-CD"] = case_path
                        params.update(case["params"])
                        try:
                            replace_placeholders([submit_script_path], params)
                        except Exception:
                            os.remove(submit_script_path)
                            raise
                        
            else:
                raise Exception("Submission script 'submit.%s.sh' not found in study directory." % self.remote.name)
            # Set cases as uploaded
            for case in upload_cases:
                case["status"] = "UPLOADED"
                case["remote"] = self.remote.name
            self.study_file.backup(self.tmpdir)
            # Modify the study file so it is uploaded updated.
            try:
                self.study_file.write()
                upload_paths = [case["name"] for case in upload_cases]
                upload_paths.extend(self.DEFAULT_UPLOAD_FILES)
                # self._upload(self.study_name, self.study_path, upload_paths, keep_targz, force)
            except Exception:
                self.study_file.restore(self.tmpdir)
                raise

        except RemoteDirExists:
            raise RemoteDirExists("Study '%s' already exists in remote '%s'." % (self.study_name, self.remote.name))

    def _compress(self, name, base_path, upload_paths):
        tar_name = name + ".tar.gz"
        with tarfile.open(os.path.join(self.tmpdir, tar_name), "w:gz") as tar:
            for path in upload_paths:
                tar.add(os.path.join(base_path, path), arcname=os.path.join(name, path))
        return tar_name
        
    def submit_case(self, force=False):
        workdir = os.path.join(self.remote.workdir, self.study_name)
        remotedir = os.path.join(workdir, self.case_name)
        if not self.remote.remote_dir_exists(remotedir):
            self.upload_case()
        try:
            time.sleep(1)
            self._check_case(self.case_name)
            self.remote.command("cd %s && qsub submit.sh" % remotedir)
        except Exception as error:
            if self.remote.command_status == 127:
                raise Exception("Command 'qsub' not found in remote '%s'." % self.remote.name)

    def submit_study(self, force=False, array_job=False):
        remote_studydir = os.path.join(self.remote.workdir, self.study_name)
        if not self.remote.remote_dir_exists(remote_studydir):
            error = "Study '%s' not found in remote '%s'. Upload it first.\n" % (self.study_name, self.remote.name)
            error += "NOTE: Sometimes NFS filesystems take a while to syncronise.\n" +\
                     "      If you are sure the study is uploaded, wait a bit and retry submission."
            raise Exception(error)
        if self.study_file.is_empty():
            raise Exception("File 'cases.info' is empty. Cannot submit case.")
        else:
            if array_job:
               pass
            for case in self.study_file.cases:
                time.sleep(0.1)
                try:
                    remote_casedir = os.path.join(remote_studydir, case["name"])
                    if not self.remote.cmd_avail("qsub"):
                        raise Exception("Command 'qsub' not found in remote '%s'." % self.remote.name)
                    awk = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
                    output = self.remote.command("cd %s && qsub submit.sh | %s" % (remote_casedir, awk), timeout=10)
                    case["jid"] = output[0].rstrip() 
                    case["status"] = "SUBMITTED"
                    case["sub_date"] = time.strftime("%c")
                    print "Submitted case '%s' with job id '%s'." % (case["name"], case["jid"])
                except Exception:
                    raise
                finally:
                    # Save if some jobs has been submitted before the error
                    self.study_file.write()
        print "Submitted %d cases." % len(self.study_file.cases)


    def update_status(self):
        try:
            time.sleep(0.1)
            awk = "awk 'match($0,/[0-9]+/){print substr($0, RSTART, RLENGTH)}'"
            output = self.remote.command("qstat -t | %s" % awk, timeout=10)
            print output
        except Exception as error:
            if self.remote.command_status == 127:
                raise Exception("Command 'qstat' not found in remote '%s'." % self.remote.name)
            else:
                raise Exception(error)

    def download_study(self, cases_idx=None, force=False):
        if cases_idx is None:
            cases_idx = list(xrange(1, self.study_file.nof_cases+1))
        download_cases = self.study_file.get_cases(cases_idx, "id")
        remote_cases = {}
        for case in download_cases:
            try:
                remote_cases[case["remote"]].append(case)
            except KeyError:
                remote_case[case["remote"]] = []
                remote_cases[case["remote"]].append(case)
        print remote_cases
        sys.exit()
        remote_studydir = os.path.join(self.remote.workdir, self.study_name)
        # if not self.remote.remote_dir_exists(remote_studydir):
        #     raise Exception("Study does not exists in remote '%s'." % self.remote.name)
        if self.study_file.is_empty():
            raise Exception("File 'cases.info' is empty. Cannot download case.")
        else:
            self.param_file.load(self.study_path)
            compress_dirs = ""
            for path in self.param_file["DOWNLOAD"]:
                include_list = []
                exclude_list = []
                path_name = path["path"]
                # TODO: Move checks of params.yaml to the Sections checkers in parampy.py
                include_exists = "include" in path
                exclude_exists = "exclude" in path
                #TODO: CHANGE to "{name1, name2}/path"
                path_wildcard = os.path.join("[0-9]*", path["path"])
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

            compress_src = os.path.join(remote_studydir, self.study_name + ".tar.gz")
            tar_cmd = "tar -czf %s %s" % (compress_src, compress_dirs)
            force = True
            # print "Compressing study..."
            # try:
            #     if force:
            #         tar_cmd += " --ignore-failed-read"
            #     self.remote.command("cd %s && %s" % (remote_studydir, tar_cmd) ,\
            #                         close_on_error=False, timeout=10)
            # except Exception as error:
            #     if self.remote.command_status != 0:
            #         self.remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=10)
            #         raise Exception(error)
            print tar_cmd
            sys.exit()
            print "Downloading study..."
            self.remote.download(compress_src, self.study_path)
            print "Cleaning..."
            self.remote.command("cd %s && rm -f %s" % (remote_studydir, compress_src), timeout=10)
            print "Done."


                



if __name__ == "__main__":
    pass
