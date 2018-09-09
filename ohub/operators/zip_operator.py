# https://github.com/rssanders3/airflow-zip-operator-plugin/

__author__ = "rssanders3"

from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from zipfile import ZipFile
import os

"""
Documentation References:
 - https://docs.python.org/2/library/zipfile.html
 - https://pymotw.com/2/zipfile/
"""


class ZipOperator(BaseOperator):
    """
    An operator which takes in a path to a file and zips the contents to a location you define.

    :param path_to_file_to_zip: Full path to the file you want to Zip
    :type path_to_file_to_zip: string
    :param path_to_save_zip: Full path to where you want to save the Zip file
    :type path_to_save_zip: string
    """

    template_fields = ("path_to_file_to_zip", "path_to_save_zip")
    template_ext = []
    ui_color = "#ffffff"  # ZipOperator's Main Color: white  # todo: find better color

    @apply_defaults
    def __init__(self, path_to_file_to_zip, path_to_save_zip, *args, **kwargs):
        super(ZipOperator, self).__init__(*args, **kwargs)
        self.path_to_file_to_zip = path_to_file_to_zip
        self.path_to_save_zip = path_to_save_zip

    def execute(self, context):
        self.log.info("Executing ZipOperator.execute(context)")

        self.log.info("Path to the File to Zip (path_to_file_to_zip): %s", str(self.path_to_file_to_zip))
        self.log.info("Path to save the Zip File (path_to_save_zip) : %s", str(self.path_to_save_zip))

        dir_path_to_file_to_zip = os.path.dirname(
            os.path.abspath(self.path_to_file_to_zip)
        )
        self.log.info("Absolute path to the File to Zip: %s", str(dir_path_to_file_to_zip))

        zip_file_name = os.path.basename(self.path_to_save_zip)
        self.log.info("Zip File Name: %s", str(zip_file_name))

        file_to_zip_name = os.path.basename(self.path_to_file_to_zip)
        self.log.info("Name of the File or Folder to be Zipped: %s", str(file_to_zip_name))

        os.chdir(dir_path_to_file_to_zip)
        self.log.info("Current Working Directory: %s", str(os.getcwd()))

        with ZipFile(zip_file_name, "w") as zip_file:
            self.log.info("Created zip file object '%s' with name '%s'", str(zip_file), str(zip_file_name))
            is_file = os.path.isfile(self.path_to_file_to_zip)
            self.log.info("Is the File to Zip a File (else its a folder): %s", str(is_file))
            if is_file:
                self.log.info("Writing '%s to zip file", str(file_to_zip_name))
                zip_file.write(file_to_zip_name)
            else:  # is folder
                for dirname, subdirs, files in os.walk(file_to_zip_name):
                    self.log.info("Writing '%s to zip file", str(dirname))
                    zip_file.write(dirname)
                    for filename in files:
                        file_name_to_write = os.path.join(dirname, filename)
                        self.log.info("Writing '%s to zip file", str(file_name_to_write))
                        zip_file.write(file_name_to_write)

            # todo: print out contents and results of zip file creation (compression ratio, size, etc)

            self.log.info("Closing Zip File Object")
            zip_file.close()

        self.log.info("Moving '%s' to '%s'", str(zip_file_name), str(self.path_to_save_zip))
        os.rename(zip_file_name, self.path_to_save_zip)

        self.log.info("Finished executing ZipOperator.execute(context)")


class UnzipOperator(BaseOperator):
    """
    An operator which takes in a path to a zip file and unzips the contents to a location you define.

    :param path_to_zip_file: Full path to the zip file you want to Unzip
    :type path_to_zip_file: string
    :param path_to_unzip_contents: Full path to where you want to save the contents of the Zip file you're Unzipping
    :type path_to_unzip_contents: string
    """

    template_fields = ("path_to_zip_file", "path_to_unzip_contents")
    template_ext = []
    ui_color = "#ffffff"  # UnzipOperator's Main Color: white  # TODO: find better color

    @apply_defaults
    def __init__(self, path_to_zip_file, path_to_unzip_contents, **kwargs):
        super().__init__(**kwargs)
        self.path_to_zip_file = path_to_zip_file
        self.path_to_unzip_contents = path_to_unzip_contents

    def execute(self, context):
        self.log.info("Executing UnzipOperator.execute(context)")

        self.log.info("path_to_zip_file: %s", str(self.path_to_zip_file))
        self.log.info("path_to_unzip_contents: %s", str(self.path_to_unzip_contents))

        # No check is done if the zip file is valid so that the operator fails when expected
        # so that airflow can properly mark the task as failed and schedule retries as needed
        with ZipFile(self.path_to_zip_file, "r") as zip_file:
            self.log.info("Created zip file '%s' from path '%s'", str(zip_file), str(self.path_to_zip_file))
            self.log.info("Extracting all the contents to '%s'", str(self.path_to_unzip_contents))
            zip_file.extractall(self.path_to_unzip_contents)
            self.log.info("Closing Zip File Object")
            zip_file.close()

        self.log.info("Finished executing UnzipOperator.execute(context)")
