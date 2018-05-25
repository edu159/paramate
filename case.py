import time

JOB_STATES = ["CREATED", "UPLOADED", "SUBMITTED", "FINISHED", "DOWNLOADED"]
class Case:
    def __init__(self, id=None, params=None, name=None, short_name=False,
                 job_id=None, status="CREATED", submission_date=None, remote=None): 
        self.id = id
        self.params = params 
        self.short_name = short_name
        self.name = name
        self.job_id = job_id
        self.status = status
        self.submission_date = submission_date
        self.remote = remote
        self.creation_date = time.strftime("%c")

    def init_from_dict(self, attrs):
        for key in attrs:
            setattr(self, key, attrs[key])

    def reset(self):
        self.job_id = None
        self.status = "CREATED"
        self.sub_date = None
        self.remote = None

    def __getitem__(self, key):
        return self.__dict__[key]



