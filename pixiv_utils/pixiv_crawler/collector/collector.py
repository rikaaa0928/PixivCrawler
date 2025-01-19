import concurrent.futures as futures
import functools
import json
import os
from typing import Dict, Iterable, List, Set, Callable

import tqdm

from pixiv_utils.pixiv_crawler.config import download_config, user_config
from pixiv_utils.pixiv_crawler.downloader import Downloader
from pixiv_utils.pixiv_crawler.utils import printInfo

from .collector_unit import collect
from .selectors import selectPage, selectTag, select_bookmark_data


class Collector:
    """
    Collect all image ids in each artwork, and send to downloader
    NOTE: An artwork may contain multiple images.
    """

    def __init__(self, downloader: Downloader):
        self.id_group: Set[str] = set()  # illust_id
        self.downloader = downloader

    def add(self, image_ids: Iterable[str]):
        for image_id in image_ids:
            self.id_group.add(image_id)

    def collect(self):
        """
        Collect all image ids in each artwork, and send to downloader
        NOTE: an artwork may contain multiple images
        """
        if download_config.with_tag:
            self.collectTags()
        if download_config.bookmark:
            self.collectBookmarkData()

        printInfo("===== Collector start =====")
        printInfo("NOTE: An artwork may contain multiple images.")

        with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            with tqdm.trange(len(self.id_group), desc="Collecting urls") as pbar:
                urls = [
                    f"https://www.pixiv.net/ajax/illust/{illust_id}/pages?lang=zh"
                    for illust_id in self.id_group
                ]
                additional_headers = [
                    {
                        "Referer": f"https://www.pixiv.net/artworks/{illust_id}",
                        "x-user-id": user_config.user_id,
                    }
                    for illust_id in self.id_group
                ]
                url_futures = [
                    executor.submit(collect, url, selectPage, headers)
                    for url, headers in zip(urls, additional_headers)
                ]
                for future in futures.as_completed(url_futures):
                    urls = future.result()
                    if urls is not None:
                        self.downloader.add(urls)
                    pbar.update()

        printInfo("===== Collector complete =====")
        printInfo(f"Number of images: {len(self.downloader.url_group)}")


    def _collect_and_save_data(
        self,
        selector: Callable,
        data_name: str,
        file_name: str,
    ):
        """
        Collect data using the given selector and save it to a file.

        Args:
            selector: A function that selects the desired data from the artwork page.
            data_name: The name of the data being collected (e.g., "tags", "bookmark_data").
            file_name: The name of the file to save the data to.
        """
        printInfo(f"===== {data_name.capitalize()} collector start =====")

        data: Dict[str, dict] = dict()
        additional_headers = {"Referer": "https://www.pixiv.net/bookmark.php?type=user"}
        collect_data_fn = functools.partial(
            collect, selector=selector, additional_headers=additional_headers
        )
        with futures.ThreadPoolExecutor(download_config.num_threads) as executor:
            # Filter out illust_ids for which the data file already exists
            filtered_id_group = []
            filtered_urls = []
            for illust_id in self.id_group:
                illust_dir = os.path.join(download_config.store_path, illust_id)
                file_path = os.path.join(illust_dir, file_name)
                if not os.path.exists(file_path):
                    filtered_id_group.append(illust_id)
                    filtered_urls.append(f"https://www.pixiv.net/artworks/{illust_id}")
                else:
                    printInfo(f"Data for illust_id {illust_id} already exists. Skipping.")

            with tqdm.trange(len(filtered_id_group), desc=f"Collecting {data_name}") as pbar:
                for illust_id, collected_data in zip(
                    filtered_id_group,
                    executor.map(
                        collect_data_fn,
                        filtered_urls,
                    ),
                ):
                    if collected_data is not None:
                        data[illust_id] = collected_data
                        # Create directory for each illust_id
                        illust_dir = os.path.join(download_config.store_path, illust_id)
                        os.makedirs(illust_dir, exist_ok=True)

                        # Save data to a file in the illust_id directory
                        file_path = os.path.join(illust_dir, file_name)
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(
                                json.dumps({illust_id: collected_data}, indent=4, ensure_ascii=False)
                            )
                    pbar.update()

        printInfo(f"===== {data_name.capitalize()} collector complete =====")
        return data

    def collectTags(self):
        """
        Collect artwork tags and save in tags.json
        """
        self.tags = self._collect_and_save_data(selectTag, "tags", "tags.json")

    def collectBookmarkData(self):
        """
        Collect artwork bookmark data and save in bookmark_data.json
        """
        self.bookmark_data = self._collect_and_save_data(
            select_bookmark_data, "bookmark_data", "bookmark_data.json"
        )
