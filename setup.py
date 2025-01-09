from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install
import subprocess


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        subprocess.check_call(["pre-commit", "install"])
        super().run()


setup(
    cmdclass={
        "develop": PostDevelopCommand,
    },
)
