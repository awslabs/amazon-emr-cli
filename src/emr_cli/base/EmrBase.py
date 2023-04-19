import boto3

class EmrBase:

    """
    Base class for defining authentication profile and any future common
    EMR operations

    Constructor
    ----------
    profile : the name of the profile as store in the aws config file

    Attributes
    ----------
    aws_session : the object return by for the session initialized 
                using the profile passed through the extending class
        
    """

    aws_session = ""

    def __init__(self, profile):

        self.aws_session = boto3.session.Session(profile_name=profile)
