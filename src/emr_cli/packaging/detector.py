class PackageDetector():
    """
    Detects the type of package used for Spark deployment.
    - Single PySpark file
    - setuptools-based project
    - poetry project
    - requirements.txt
    """