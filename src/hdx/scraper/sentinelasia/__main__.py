#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_state import HDXState
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import iso_string_from_datetime, now_utc, parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from src.hdx.scraper.sentinelasia.pipeline import Pipeline

from hdx.scraper.sentinelasia._version import __version__

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-sentinelasia"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Sentinel Asia"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        temp_dir = info["folder"]
        with HDXState(
            "pipeline-state-sentinelasia",
            temp_dir,
            parse_date,
            iso_string_from_datetime,
            configuration,
        ) as state:
            today = now_utc()
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_dir,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_dir,
                    save=save,
                    use_saved=use_saved,
                )
                pipeline = Pipeline(configuration, retriever, temp_dir, state.get())
                countries = pipeline.get_countries()
                for _, country in progress_storing_folder(info, countries, "iso3"):
                    countryiso = country["iso3"]
                    events = pipeline.get_events(countryiso)
                    datasets = pipeline.generate_datasets(countryiso, events)
                    for dataset in datasets:
                        dataset.update_from_yaml(
                            script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"), main
                            )
                        )
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )
                pipeline.output_ignored_types()
            state.set(today)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
