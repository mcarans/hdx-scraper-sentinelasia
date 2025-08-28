#!/usr/bin/python
"""Sentinel Asia scraper"""

import logging
from datetime import datetime
from os.path import splitext
from typing import Dict, List
from zipfile import ZipFile

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import get_filename_from_url
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        startdate: datetime,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._startdate = startdate.strftime("%Y%m%d")
        self._base_url = configuration["base_url"]
        self._metadata = self.get_metadata()
        self._ignored_types = set()

    def get_metadata(self) -> Dict:
        return self._retriever.download_json(f"{self._base_url}/get_metadata")

    def get_countries(self) -> Dict:
        return self._retriever.download_json(f"{self._base_url}/get_countries")

    def get_events(self, iso3: str) -> List:
        return self._retriever.download_json(
            f"{self._base_url}/get_events?countryiso3s={iso3}&start_date={self._startdate}"
        )

    def get_base_dataset(self) -> Dataset:
        return Dataset(
            {
                "methodology_other": self._metadata["methodology"],
                "license_other": self._metadata["licence"],
                "caveats": self._metadata["caveats"],
            }
        )

    @staticmethod
    def get_extensions_from_zip(path: str) -> List[str]:
        extensions = []
        with ZipFile(path, "r") as zip_ref:
            file_list = zip_ref.namelist()
            for file in file_list:
                filename, extension = splitext(file)
                extensions.append(extension[1:].lower())
        return extensions

    def generate_datasets(self, iso3: str, events: List) -> List[Dataset]:
        datasets = []
        if not events:
            return datasets
        countryname = Country.get_country_name_from_iso3(iso3)
        for event in events:
            dataset = self.get_base_dataset()
            try:
                dataset.add_country_location(iso3)
            except HDXError:
                logger.error(f"Couldn't find country {iso3}, skipping")
                continue
            occurrence_date = event["occurrence_date"]
            disaster_type = event["disaster_type"]
            dataset["name"] = slugify(
                f"sentinelasia-{iso3}-{occurrence_date}-{disaster_type}"
            )
            orig_country = event["country"]
            description = event["description"].replace(orig_country, countryname)
            dataset["title"] = description.replace(f"{occurrence_date}: ", "")
            notes = f"{self._metadata['description']}  \n  \nDetails: {description}."
            glide_number = event["glide_number"]
            if glide_number:
                notes = f"{notes} Glide Number: {glide_number}."
            dataset["notes"] = notes
            tags = ["hxl", "geodata"]
            found_tag = False
            disaster_types = self._configuration["disaster_types"]
            disaster_type_lower = disaster_type.lower()
            description_lower = description.lower()
            for disaster in disaster_types:
                if disaster in disaster_type_lower or disaster in description_lower:
                    tags.append(disaster)
                    found_tag = True
            if not found_tag:
                logger.error(f"Unknown disaster type {disaster_type}!")
            dataset.add_tags(tags)
            dataset["dataset_source"] = event["requester"]
            dataset.set_subnational(False)
            dataset.set_time_period(parse_date(occurrence_date))
            resources = []
            for file in event["files"]:
                file_type = file["file_type"]
                if file_type not in ("zip", "shp", "geojson", "kmz", "tif", "tiff"):
                    self._ignored_types.add(file_type)
                    continue
                url = file["url"]
                filename = get_filename_from_url(url)
                path = self._retriever.download_file(url, filename)
                if file_type == "zip":
                    extensions = self.get_extensions_from_zip(path)
                    for extension in extensions:
                        if extension in ("shp", "geojson", "kmz", "tif", "tiff"):
                            file_type = extension
                            break
                        if extension == "zip":  # zip within a zip is assumed to be shp
                            file_type = "shp"
                            break
                    if file_type == "zip":
                        logger.warning(
                            f"Couldn't find expected formats in zip! Extensions found: {','.join(extensions)}"
                        )
                        continue
                resource = Resource(
                    {"name": filename, "description": file["description"]}
                )
                resource.set_format(file_type)
                path = self._retriever.download_file(url, filename)
                resource.set_file_to_upload(path)
                resources.append(resource)
            if len(resources) == 0:
                logger.info(f"No resources found for {description}")
                continue
            dataset.add_update_resources(resources)
            datasets.append(dataset)
        return datasets

    def output_ignored_types(self):
        logger.info(f"Ignored types: {', '.join(sorted(self._ignored_types))}")
