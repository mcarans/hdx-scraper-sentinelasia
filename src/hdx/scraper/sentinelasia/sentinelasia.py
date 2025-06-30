#!/usr/bin/python
"""Sentinel Asia scraper"""

import logging
from typing import List, Optional


from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class SentinelAsia:

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._base_url = configuration["base_url"]

    def get_countries(self):
        countryisos = set()
        json = self._retriever.download_json(f"{self._base_url}/get_countries")
        for country in json:
            countryiso2 = analysis["country"]
            countryiso3 = Country.get_iso3_from_iso2(countryiso2)
            if countryiso3 is None:
                logger.error(
                    f"Could not find country ISO 3 code matching ISO 2 code {countryiso2}!"
                )
            else:
                countryisos.add(countryiso3)
        return [{"iso3": x} for x in sorted(countryisos)]

    def generate_dataset(self) -> Optional[Dataset]:

        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset
