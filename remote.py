import paramiko
import yaml
import shutil
import os
from paramiko import SSHClient
import getpass
import tarfile
from scp import SCPClient
import socket
from parampy import StudyFile


#TODO: Refactor Remote to separate configuration-related stuff
SRC_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULTS_DIR = os.path.join(SRC_DIR, "defaults")
CONFIG_DIR = os.path.join(os.getenv("HOME"), ".parampy")

class Remote:
    def __init__(self, name="", remote_workdir=None, addr=None,\
                 port=22, username=None, key_login=False,shell="bash"):
        self.name = name
        self.remote_yaml = None
        self.key_login = key_login
        self.addr = addr
        self.port = port
        self.username = username
        self.remote_workdir = remote_workdir
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

    def load(self, path):
        with open(path, 'r') as remotefile:
            try:
                self.remote_yaml = yaml.load(remotefile)["remote"]
            except yaml.YAMLError as exc:
                print(exc)
        self._unpack_remote_yaml(self.remote_yaml)

    def _unpack_remote_yaml(self, yaml_remote):
        try:
            self.name = yaml_remote["name"]
            self.addr = yaml_remote["address"]
            self.port = yaml_remote["port"]
            self.remote_workdir = yaml_remote["remote-workdir"]
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
            remotedata["remote"]["remote-workdir"] = self.remote_workdir
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

    def command(self, cmd, timeout=None):
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=timeout)
        self.command_status = stdout.channel.recv_exit_status()
        error = stderr.readlines()
        if error:
            self.close()
            raise Exception("".join([l for l in error if l]))
        return stdout.readlines()
    
    def upload(self, path_orig, path_dest):
        self.scp.put(path_orig, path_dest)

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
        if case_path is not None:
            self.case_path = os.path.abspath(self.case_path)
            self.case_name = os.path.basename(self.case_path)
            self.study_path = os.path.dirname(self.case_path)
            self.study_name = os.path.basename(self.study_path)
            self.study_file = StudyFile(path=self.study_path)
            # Check if the cases.txt has been generated. Meaning it is a study.
            if not self.study_file.exists():
                self.study_name = "default"
        else:
            self.study_name = os.path.basename(self.study_path)
        self.tmp_dir = "/tmp"


    def _upload(self, name, path, keep_targz=False, force=False, remote_workdir=None):
        if remote_workdir is None:
            remote_workdir = self.remote.remote_workdir
        remotedir = os.path.join(remote_workdir, name)
        if self.remote.remote_dir_exists(remotedir):
            if not force:
                raise RemoteDirExists("")
        tar_name = self._compress(name, path)
        upload_src = os.path.join(self.tmp_dir, tar_name)
        upload_dest = remote_workdir
        self.remote.upload(upload_src, upload_dest)
        extract_src = os.path.join(upload_dest, tar_name)
        extract_dest = upload_dest
        out = self.remote.command("tar -xzf %s --directory %s" % (extract_src, extract_dest))
        os.remove(upload_src)
        if not keep_targz:
            out = self.remote.command("rm -f %s" % extract_src)

    def upload_case(self, keep_targz=False, force=False):
        # self._case_clean()
        remote_workdir = os.path.join(self.remote.remote_workdir, self.study_name)
        if not self.remote.remote_dir_exists(remote_workdir):
            out = self.remote.command("mkdir %s" % remote_workdir)
        try:
            self._upload(self.case_name, self.case_path, keep_targz, force, remote_workdir)
        except RemoteDirExists:
            raise RemoteDirExists("Case '%s' already exists in study '%s' in the remote '%s'." % (self.case_name,self.study_name, self.remote.name))


    def upload_study(self, keep_targz=False, force=False):
        try:
            self._upload(self.study_name, self.study_path, keep_targz, force)
        except RemoteDirExists:
            raise RemoteDirExists("Study '%s' already exists in remote '%s'." % (self.study_name, self.remote.name))

    def _compress(self, name, path):
        tar_name = name + ".tar.gz"
        with tarfile.open(os.path.join(self.tmp_dir, tar_name), "w:gz") as tar:
            tar.add(path, arcname=os.path.basename(path))
        return tar_name
        
    def submit_case(self):
        remote_workdir = os.path.join(self.remote.remote_workdir, self.study_name)
        remotedir = os.path.join(remote_workdir, self.case_name)
        if not self.remote.remote_dir_exists(remotedir):
            self.upload_case()
        try:
            self.remote.command("cd %s && qsub exec.sh" % remotedir)
        except Exception as error:
            if self.remote.command_status == 127:
                raise Exception("Command 'qsub' not found in remote '%s'." % self.remote.name)

    def submit_study(self):
        remote_studydir = os.path.join(self.remote.remote_workdir, self.study_name)
        if not self.remote.remote_dir_exists(remote_studydir):
            self.upload_study()
        self.study_file.read()
        if self.study_file.is_empty():
            raise Exception("File 'cases.txt' is empty. Cannot submit case.")
        else:
            for case in self.study_file.cases:
                remote_casedir = os.path.join(remote_studydir, case[0])
                try:
                    self.remote.command("cd %s && qsub exec.sh" % remote_casedir, timeout=10)
                except Exception as error:
                    if self.remote.command_status == 127:
                        raise Exception("Command 'qsub' not found in remote '%s'." % self.remote.name)
                    else:
                        raise Exception(error)


    # def stat_study(self):
    #     try:
    #         self.remote.command("cd %s && qstat" % remote_casedir, timeout=10)
    #     except Exception as error:
    #      


    def decompress_study(self):
        pass

    def decompress_case(self):
        pass


class StudyRetriever:
    pass

class StudyMonitor:
    pass

if __name__ == "__main__":
    r = Remote() 
    r.load("./remote.yaml")
    passwd = getpass.getpass("Password: ")
    r.connect(passwd)
    # sm = StudyManager(r, study_path="test2")
    # sm.upload_study()
    sm = StudyManager(r, case_path="study1/test2")
    try:
        sm.upload_case()
    except Exception as e:
        r.close()
        raise e
    else:
        r.close()
