#! /usr/bin/env python

import importlib
import os
import platform
import shutil
import sys
from os.path import join
import platform

from setuptools import Command, setup

DISTNAME = "haredis"
DESCRIPTION = "Python Module which wraps Redis ...."
with open("README.md") as f:
    LONG_DESCRIPTION = f.read()
MAINTAINER = "Cevat Batuhan Tolon"
MAINTAINER_EMAIL = "cevat.batuhan.tolon@cern.ch"
#URL = ""
DOWNLOAD_URL = "https://pypi.org/project/haredis/#files"
LICENSE = "Apache 2.0"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/haredis/haredis/issues",
    "Documentation": "https://www.haredis.com",
    "Source Code": "https://github.com/ctolon/haredis",
}
VERSION = "0.0.1"

class InvalidVersion(ValueError):
    """
    An invalid version was found, users should refer to PEP 440.
    """

def parse(version: str):
    """Parse the given version from a string to an appropriate class."""

    try:
        version.split(".")
    except InvalidVersion:
        return InvalidVersion("Version is invalid: {version}".format(version=version))

# Custom clean command to remove build artifacts
class CleanCommand(Command):
    description = "Remove build artifacts from the source tree"

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # Remove c files if we are not within a sdist package
        cwd = os.path.abspath(os.path.dirname(__file__))
        remove_c_files = not os.path.exists(os.path.join(cwd, "PKG-INFO"))
        if remove_c_files:
            print("Will remove generated .c files")
        if os.path.exists("build"):
            shutil.rmtree("build")
        for dirpath, dirnames, filenames in os.walk("haredis"):
            for filename in filenames:
                root, extension = os.path.splitext(filename)

                if extension in [".so", ".pyd", ".dll", ".pyc"]:
                    os.unlink(os.path.join(dirpath, filename))

                if remove_c_files and extension in [".c", ".cpp"]:
                    pyx_file = str.replace(filename, extension, ".pyx")
                    if os.path.exists(os.path.join(dirpath, pyx_file)):
                        os.unlink(os.path.join(dirpath, filename))

                if remove_c_files and extension == ".tp":
                    if os.path.exists(os.path.join(dirpath, root)):
                        os.unlink(os.path.join(dirpath, root))

            for dirname in dirnames:
                if dirname == "__pycache__":
                    shutil.rmtree(os.path.join(dirpath, dirname))
                    
cmdclass = {
    "clean": CleanCommand,
}

            
def setup_package():
    python_requires = ">=3.6"
    required_python_version = (3, 6)

    metadata = dict(
        name=DISTNAME,
        maintainer=MAINTAINER,
        author=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        author_email=MAINTAINER_EMAIL,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        license=LICENSE,
        #url=URL,
        download_url=DOWNLOAD_URL,
        project_urls=PROJECT_URLS,
        version=VERSION,
        package_dir={"haredis": "haredis"},
        keywords=["redis", "aioredis", "lock-relase", "high availability", "distributed systems", "streaming api"],
        # packages=find_packages(),
        classifiers=[
            "Intended Audience :: Developers",
            'Natural Language :: English'
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Topic :: Software Development",
            'Topic :: System :: Distributed Computing',
            "Topic :: Scientific/Engineering",
            "Development Status :: 4 - Beta",
            "Operating System :: Microsoft :: Windows",
            "Operating System :: POSIX",
            "Operating System :: Unix",
            "Operating System :: MacOS",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            'Framework :: AsyncIO'
        ],
        cmdclass=cmdclass,
        python_requires=python_requires,
        install_requires=['redis>=4'],
        package_data={"": ["*.csv", "*.gz", "*.txt", "*.pxd", "*.md", "*.jpg"]},
        zip_safe=False,  # the package can run out of an .egg file
        # extras_require={"with_celery": ["celery"]}
    )
    
    commands = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
    if not all(
        command in ("egg_info", "dist_info", "clean", "check") for command in commands
    ):
        if sys.version_info < required_python_version:
            required_version = "%d.%d" % required_python_version
            raise RuntimeError(
                "haredis requires Python %s or later. The current"
                " Python version is %s installed in %s."
                % (required_version, platform.python_version(), sys.executable)
            )
            
    setup(**metadata)

if __name__ == "__main__":
    setup_package()


