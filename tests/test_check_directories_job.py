"""Test module for classes and methods in check_directories_job"""

import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.platforms import Platform
from aind_data_transfer_models.core import (
    BasicUploadJobConfigs,
    ModalityConfigs,
)

from aind_data_upload.check_directories_job import (
    CheckDirectoriesJob,
    JobSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
SMART_SPIM_DIR = (
    RESOURCES_DIR
    / "example_smartspim_data_set"
    / "SmartSPIM_695464_2023-10-18_20-30-30"
)


class TestJobSettings(unittest.TestCase):
    """
    Tests for JobSettings class
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up class with example upload configs"""
        example_upload_configs = BasicUploadJobConfigs(
            project_name="SmartSPIM",
            platform=Platform.SMARTSPIM,
            modalities=[
                ModalityConfigs(
                    source=(
                        RESOURCES_DIR / "example_ephys_data_set"
                    ).as_posix(),
                    modality=Modality.ECEPHYS,
                ),
                ModalityConfigs(
                    source=(
                        RESOURCES_DIR
                        / "example_smartspim_data_set"
                        / "SmartSPIM_695464_2023-10-18_20-30-30"
                    ).as_posix(),
                    modality=Modality.SPIM,
                ),
            ],
            subject_id="12345",
            acq_datetime="2020-10-10T01:01:01",
            metadata_dir=(RESOURCES_DIR / "metadata_dir").as_posix(),
        )
        cls.example_upload_configs = example_upload_configs

    def test_class_constructor(self):
        """Tests that job settings can be constructed from serialized json."""
        upload_configs = self.example_upload_configs
        job_settings = JobSettings(upload_configs=upload_configs)
        deserialized_settings = job_settings.model_validate_json(
            job_settings.model_dump_json()
        )
        self.assertEqual(job_settings, deserialized_settings)


class TestCheckDirectoriesJob(unittest.TestCase):
    """Tests CheckDirectoriesJob class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Sets up class with example settings"""
        example_upload_configs = BasicUploadJobConfigs(
            project_name="SmartSPIM",
            platform=Platform.SMARTSPIM,
            modalities=[
                ModalityConfigs(
                    source=(
                        RESOURCES_DIR / "example_ephys_data_set"
                    ).as_posix(),
                    modality=Modality.ECEPHYS,
                ),
                ModalityConfigs(
                    source=(
                        RESOURCES_DIR
                        / "example_smartspim_data_set"
                        / "SmartSPIM_695464_2023-10-18_20-30-30"
                    ).as_posix(),
                    modality=Modality.SPIM,
                ),
            ],
            subject_id="12345",
            acq_datetime="2020-10-10T01:01:01",
            metadata_dir=(RESOURCES_DIR / "metadata_dir").as_posix(),
        )
        cls.example_job = CheckDirectoriesJob(
            job_settings=JobSettings(
                upload_configs=example_upload_configs,
                num_of_smart_spim_levels=2,
            )
        )
        cls.expected_list_of_directories_to_check = [
            (RESOURCES_DIR / "example_ephys_data_set").as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "471320"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "503720"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "536120"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "568520"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_561_Em_600" / "471320"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_561_Em_600" / "503720"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_561_Em_600" / "536120"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_561_Em_600" / "568520"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_639_Em_680" / "471320"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_639_Em_680" / "503720"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_639_Em_680" / "536120"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_639_Em_680" / "568520"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_488_Em_525_MIP" / "471320"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_488_Em_525_MIP" / "503720"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_488_Em_525_MIP" / "536120"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_488_Em_525_MIP" / "568520"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_561_Em_600_MIP" / "471320"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_561_Em_600_MIP" / "503720"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_561_Em_600_MIP" / "536120"
            ).as_posix(),
            (
                SMART_SPIM_DIR / "derivatives" / "Ex_561_Em_600_MIP" / "568520"
            ).as_posix(),
        ]

    @patch("os.path.isfile")
    def test_check_file_or_symlink(self, mock_is_file: MagicMock):
        """Tests _check_file_or_symlink when isfile is True"""
        mock_is_file.return_value = True
        self.assertIsNone(self.example_job._check_path("mocked_file"))

    @patch("os.path.isfile")
    @patch("os.path.islink")
    @patch("os.path.exists")
    def test_check_file_or_symlink_error(
        self,
        mock_exists: MagicMock,
        mock_is_link: MagicMock,
        mock_is_file: MagicMock,
    ):
        """Tests _check_file_or_symlink when isfile is False, islink is True,
        and exists is False."""
        mock_is_file.return_value = False
        mock_is_link.return_value = True
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError) as e:
            self.example_job._check_path("mocked_file")
        self.assertEqual(
            "mocked_file is either not a file or is a broken symlink",
            e.exception.args[0],
        )

    @patch(
        "aind_data_upload.check_directories_job.CheckDirectoriesJob."
        "_check_path"
    )
    def test_get_list_of_directories_to_check(
        self, mock_check_path: MagicMock
    ):
        """Tests _get_list_of_directories_to_check"""
        mock_check_path.return_value = True
        list_of_directories = (
            self.example_job._get_list_of_directories_to_check()
        )
        expected_calls = [
            call((RESOURCES_DIR / "metadata_dir" / "subject.json").as_posix()),
            call((SMART_SPIM_DIR / "SmartSPIM").as_posix()),
            call((SMART_SPIM_DIR / "derivatives").as_posix()),
            call((SMART_SPIM_DIR / "instrument.json").as_posix()),
            call((SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525").as_posix()),
            call((SMART_SPIM_DIR / "SmartSPIM" / "Ex_561_Em_600").as_posix()),
            call((SMART_SPIM_DIR / "SmartSPIM" / "Ex_639_Em_680").as_posix()),
            call((SMART_SPIM_DIR / "SmartSPIM" / "nohup.out").as_posix()),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Ex_488_Em_525_MIP"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Ex_561_Em_600_MIP"
                ).as_posix()
            ),
            call(
                (SMART_SPIM_DIR / "derivatives" / "ASI_logging.txt").as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "DarkMaster_cropped.tif"
                ).as_posix()
            ),
            call(
                (SMART_SPIM_DIR / "derivatives" / "FinalReport.txt").as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_488_Ch0_0.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_488_Ch0_1.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_561_Ch1_0.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_561_Ch1_1.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_639_Ch2_0.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "Flat_639_Ch2_1.tif"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "TileSettings.ini"
                ).as_posix()
            ),
            call(
                (SMART_SPIM_DIR / "derivatives" / "metadata.json").as_posix()
            ),
            call((SMART_SPIM_DIR / "derivatives" / "metadata.txt").as_posix()),
            call(
                (
                    SMART_SPIM_DIR / "derivatives" / "processing_manifest.json"
                ).as_posix()
            ),
            call(
                (
                    SMART_SPIM_DIR
                    / "SmartSPIM"
                    / "Ex_488_Em_525"
                    / "some_file_here.txt"
                ).as_posix()
            ),
        ]
        mock_check_path.assert_has_calls(expected_calls, any_order=True)
        self.assertCountEqual(
            self.expected_list_of_directories_to_check, list_of_directories
        )

    @patch(
        "aind_data_upload.check_directories_job.CheckDirectoriesJob."
        "_check_path"
    )
    @patch("logging.debug")
    def test_dask_task_to_process_directory_list(
        self, mock_log_debug: MagicMock, mock_check_path: MagicMock
    ):
        """Tests dask_task_to_process_directory_list"""

        list_of_directories = [
            (RESOURCES_DIR / "example_ephys_data_set").as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "471320"
            ).as_posix(),
        ]

        self.example_job._dask_task_to_process_directory_list(
            directories=list_of_directories
        )
        expected_calls = [
            call(
                path=(
                    RESOURCES_DIR
                    / "example_ephys_data_set"
                    / "hello_world.txt"
                ).as_posix()
            ),
            call(
                path=(
                    RESOURCES_DIR
                    / "example_ephys_data_set"
                    / "sub_dir"
                    / "hello_world.txt"
                ).as_posix()
            ),
            call(
                path=(
                    SMART_SPIM_DIR
                    / "SmartSPIM"
                    / "Ex_488_Em_525"
                    / "471320"
                    / "hello_world.txt"
                ).as_posix()
            ),
        ]
        mock_check_path.assert_has_calls(expected_calls, any_order=True)
        mock_log_debug.assert_called()

    @patch("dask.bag.map_partitions")
    def test_check_for_broken_sym_links(self, mock_map_partitions: MagicMock):
        """Tests _check_for_broken_sym_links"""
        list_of_directories = [
            (RESOURCES_DIR / "example_ephys_data_set").as_posix(),
            (
                SMART_SPIM_DIR / "SmartSPIM" / "Ex_488_Em_525" / "471320"
            ).as_posix(),
        ]
        self.example_job._check_for_broken_sym_links(
            directories_to_check=list_of_directories
        )
        mock_map_partitions.assert_called()

    @patch(
        "aind_data_upload.check_directories_job.CheckDirectoriesJob."
        "_check_for_broken_sym_links"
    )
    @patch("logging.debug")
    def test_run_job(
        self,
        mock_log_debug: MagicMock,
        mock_check_for_broken_symlinks: MagicMock,
    ):
        """Tests run_job method"""
        self.example_job.run_job()
        expected_list = self.expected_list_of_directories_to_check
        mock_call_list = mock_check_for_broken_symlinks.mock_calls[0].kwargs[
            "directories_to_check"
        ]
        self.assertCountEqual(expected_list, mock_call_list)
        mock_log_debug.assert_called()


if __name__ == "__main__":
    unittest.main()
