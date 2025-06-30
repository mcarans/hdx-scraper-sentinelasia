#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.sentinelasia._version import __version__
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from src.hdx.scraper.sentinelasia.sentinelasia import SentinelAsia

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-sentinelasia"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Sentinelasia"


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("hdx")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            sentinel_asia = SentinelAsia(configuration, retriever, temp_dir)
            countries = sentinel_asia.get_countries()
            #
            # Steps to generate dataset
            #
            # dataset.update_from_yaml(
            #     script_dir_plus_file(
            #         join("config", "hdx_dataset_static.yaml"), main
            #     )
            # )
            # dataset.create_in_hdx(
            #     remove_additional_resources=True,
            #     match_resource_order=False,
            #     hxl_update=False,
            #     updated_by_script=_UPDATED_BY_SCRIPT,
            #     batch=info["batch"],
            # )


if __name__ == "__main__":
    facade(
        main,
#        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
