from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.sentinelasia.pipeline import Pipeline


class Testsentinelasia:
    def test_sentinelasia(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "Testsentinelasia",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                today = parse_date("2024-01-01")
                pipeline = Pipeline(configuration, retriever, tempdir, today)
                countries = pipeline.get_countries()
                assert len(countries) == 41
                events = pipeline.get_events("LAO")
                assert len(events) == 1
                datasets = pipeline.generate_datasets("LAO", events)
                assert len(datasets) == 1
                dataset = datasets[0]
                assert dataset == {
                    "caveats": "Some country names and disaster types may contain errors due to "
                    "automatic extraction.",
                    "dataset_date": "[2024-09-12T00:00:00 TO 2024-09-12T00:00:00]",
                    "dataset_source": "ASEAN Coordinating Centre for Humanitarian Assistance on "
                    "disaster management (AHA Centre)",
                    "groups": [{"name": "lao"}],
                    "license_other": "The obtained files cannot be modified. When publishing the "
                    "obtained files, please include credits as referenced in "
                    "each file. All products are provided by Sentinel Asia and "
                    "must be attributed accordingly.",
                    "methodology_other": "Automatically collected and parsed from the official "
                    "website.",
                    "name": "sentinelasia-lao-2024-09-12-flood",
                    "notes": "Sentinel Asia is an international cooperation project that utilizes "
                    "space technology to contribute to disaster management in the "
                    "Asia-Pacific region.  \n"
                    "  \n"
                    "Details: 2024-09-12: Flood in Lao People's Democratic Republic "
                    "(Laos) on 12 September, 2024. Glide Number: TC-2024-000161-LAO.",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Flood in Lao People's Democratic Republic (Laos) on 12 September, "
                    "2024",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "DETECTED FLOOD WATER IN NORTHERN PARTS OF LAO PDRAs Observed "
                        "by Sentinel-1 image on 13 September 2024",
                        "format": "shp",
                        "name": "MBRSC_LAOS_FLOOD-MAP-SHP.zip",
                    },
                    {
                        "description": "DETECTED FLOOD WATER IN VIENTIANE PREFECTURE AND BOLIKHAMXAI "
                        "PROVINCES,LAO PDRAs observed by ALOS-2 images on 18 "
                        "September 2024",
                        "format": "shp",
                        "name": "AIT-VAP001-LAOS.zip",
                    },
                ]
