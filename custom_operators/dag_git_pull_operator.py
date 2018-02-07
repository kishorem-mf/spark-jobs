import os
import logging
import subprocess

from airflow import settings
from airflow.models import BaseOperator


class DagsGitPuller(BaseOperator):
    """Pull new commits from origin/master in the DAGs folder.

    Parameters
    ----------
    dags_folder : str
        Path to DAGs folder (default: get them from Airflow)
    reset : bool
        Remove any unstaged changes before pulling.
    clean : bool
        Remove any untracked files before pulling.
    pull : bool
        Pull the commits.
    """

    def __init__(self, dags_folder=None, reset=True, clean=True, pull=True,
                 **kwargs):
        super(DagsGitPuller, self).__init__(**kwargs)
        if dags_folder is None:
            dags_folder = settings.DAGS_FOLDER
        self.dags_folder = os.path.abspath(dags_folder)
        self.reset = reset
        self.clean = clean
        self.pull = pull

    def execute(self, context):
        logging.info('Updating DAGs repository at %s', self.dags_folder)
        try:
            status = subprocess.check_output(['git', 'status'],
                                             cwd=self.dags_folder)
            logging.info('Repository status is at:\n%s', status.strip())
            commit = subprocess.check_output(['git', 'log', '-1', '--oneline'],
                                             cwd=self.dags_folder)
            logging.info('Repository is at commit:\n%s', commit.strip())
            if self.reset:
                reset = subprocess.check_output(['git', 'reset', 'HEAD',
                                                 '--hard'],
                                                cwd=self.dags_folder)
                logging.info('Resetting changes:\n%s', reset.strip())
            if self.clean:
                clean = subprocess.check_output(['git', 'clean', '-d',
                                                 '--force'],
                                                cwd=self.dags_folder)
                logging.info('Removing untracked files:\n%s', clean.strip())
            if self.pull:
                pull = subprocess.check_output(['git', 'pull', 'origin',
                                                'master'],
                                               cwd=self.dags_folder)
                logging.info('Pulling changes:\n%s', pull.strip())
            commit = subprocess.check_output(['git', 'log', '-1', '--oneline'],
                                             cwd=self.dags_folder)
            logging.info('After refresh repository is at commit:\n%s',
                         commit.strip())
        except subprocess.CalledProcessError as e:
            logging.info('Error refreshing repository:\n%s', e.message)
            raise e