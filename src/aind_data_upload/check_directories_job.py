"""
Module to handle checking for broken symlinks in upload jobs configs sources.
Uses Dask to parallelize checking of directories.
"""

import argparse
import json
import logging
import os
import sys
from copy import deepcopy
from glob import glob
from pathlib import Path
from time import time
from typing import List, Union

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.platforms import Platform
from aind_data_transfer_models.core import BasicUploadJobConfigs
from dask import bag as dask_bag
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

# Set log level from env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
logging.basicConfig(level=LOG_LEVEL)


class JobSettings(BaseSettings):
    """Job settings for CheckDirectoriesJob"""

    upload_configs: BasicUploadJobConfigs
    n_partitions: int = Field(default=20)
    num_of_smart_spim_levels: int = Field(default=3)

    @field_validator("upload_configs", mode="before")
    def parse_json_str(
        cls, upload_conf: Union[BasicUploadJobConfigs, dict]
    ) -> BasicUploadJobConfigs:
        """
        Method to ignore computed fields in serialized model, which might
        raise validation errors.
        Parameters
        ----------
        upload_conf : Union[BasicUploadJobConfigs, dict]

        Returns
        -------
        BasicUploadJobConfigs
        """
        # TODO: This should be moved to the BasicUploadJobConfigs class itself
        if isinstance(upload_conf, dict):
            json_obj = deepcopy(upload_conf)
            # Remove s3_prefix computed field
            if json_obj.get("s3_prefix") is not None:
                del json_obj["s3_prefix"]
            # Remove output_folder_name from modalities
            if json_obj.get("modalities") is not None:
                for modality in json_obj["modalities"]:
                    if "output_folder_name" in modality:
                        del modality["output_folder_name"]
            return BasicUploadJobConfigs.model_validate_json(
                json.dumps(json_obj)
            )
        else:
            return upload_conf


class CheckDirectoriesJob:
    """Job to scan basic upload job configs source directories for broken
    symlinks"""

    def __init__(self, job_settings: JobSettings):
        """
        Class constructor for CheckDirectoriesJob.
        Parameters
        ----------
        job_settings: JobSettings
        """
        self.job_settings = job_settings

    @staticmethod
    def _check_path(path: Union[Path, str]) -> None:
        """
        Checks if path is either a directory, file, or a valid symlink.
        Parameters
        ----------
        path : Union[Path, str]

        Returns
        -------
        None
          Raises an error if the file_path does not exist or the file_path is
          a broken symlink
        """
        if not (
            os.path.isdir(path)
            or os.path.isfile(path)
            or (os.path.islink(path) and os.path.exists(path))
        ):
            raise FileNotFoundError(
                f"{path} is either not a file or is a broken symlink"
            )

    def _get_list_of_directories_to_check(self) -> List[Union[Path, str]]:
        """
        Extracts a list of directories from self.job_settings.upload_configs
        to scan for broken symlinks. The list will be passed into dask to
        parallelize the scan. Will also scan files in top levels and raise an
        error if broken symlinks are found when compiling the list of dirs.
        Returns
        -------
        List[Union[Path, str]]

        """
        upload_configs = self.job_settings.upload_configs
        directories_to_check = []
        platform = upload_configs.platform
        # First, check all the json files in the metadata dir
        if upload_configs.metadata_dir is not None:
            metadata_dir_path = str(upload_configs.metadata_dir).rstrip("/")
            for json_file in glob(f"{metadata_dir_path}/*.json"):
                self._check_path(json_file)
        # Next add modality directories
        for modality_config in upload_configs.modalities:
            modality = modality_config.modality
            source_dir = modality_config.source
            # We'll handle SmartSPIM differently and partition 3 levels deep
            if modality == Modality.SPIM and platform == Platform.SMARTSPIM:
                # Check top level files
                base_path = str(source_dir).rstrip("/")
                for _ in range(0, self.job_settings.num_of_smart_spim_levels):
                    base_path = base_path + "/*"
                    for smart_spim_path in glob(base_path):
                        self._check_path(smart_spim_path)
                # Add directories to list to be partitioned.
                base_path = base_path + "/*"
                for smart_spim_path in glob(base_path):
                    if os.path.isdir(smart_spim_path):
                        directories_to_check.append(smart_spim_path)
                    else:
                        self._check_path(smart_spim_path)
            else:
                directories_to_check.append(source_dir)
        return directories_to_check

    def _dask_task_to_process_directory_list(
        self, directories: List[Union[Path, str]]
    ) -> None:
        """
        Scans each directory in list for broken sym links
        Parameters
        ----------
        directories : List[Union[Path, str]]

        Returns
        -------
        None
          Will raise an error if a broken symlink is encountered.

        """
        logging.debug(f"Scanning list: {directories}")
        total_to_scan = len(directories)
        dir_counter = 0
        for directory in directories:
            dir_counter += 1
            logging.debug(
                f"Checking {directory}. On {dir_counter} of {total_to_scan}"
            )
            for path, _, files in os.walk(directory):
                for name in files:
                    # Expecting posix paths
                    self._check_path(path=f"{path.rstrip('/')}/{name}")

    def _check_for_broken_sym_links(
        self, directories_to_check: List[Union[Path, str]]
    ) -> None:
        """
        Checks for broken symlinks. Will not follow symlinks. Uses Dask to
        parallelize the search across modalities.
        Returns
        -------
        None
          Will raise an error if a broken sym link is encountered.
        """
        # We'll use dask to check the directories in parallel
        directory_bag = dask_bag.from_sequence(
            directories_to_check, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_directory_list, directory_bag
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main job runner. Scans files and sources in upload job configs to
        create a list of directories to check. Then checks directories in dask
        partitions. Will raise an error if a source directory does not exist
        or if a broken symlink is encountered."""
        job_start_time = time()
        list_of_directories_to_check = self._get_list_of_directories_to_check()
        logging.debug(
            f"Total directories to scan: {len(list_of_directories_to_check)}"
        )
        self._check_for_broken_sym_links(
            directories_to_check=list_of_directories_to_check
        )
        job_end_time = time()
        execution_time = job_end_time - job_start_time
        logging.debug(f"Task took {execution_time} seconds")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    main_job = CheckDirectoriesJob(job_settings=main_job_settings)
    main_job.run_job()
