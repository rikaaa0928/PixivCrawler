import os
import random
import shutil
import unittest

from pixiv_utils.pixiv_crawler import (
    BookmarkCrawler,
    checkDir,
    debug_config,
    download_config,
    network_config,
    user_config, PrivateBookmarkCrawler,
)


@unittest.skipIf(os.getenv("PIXIV_COOKIE") is None or os.getenv("PIXIV_UID") is None, "No cookie")
class TestBookmarkCrawler(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        download_config.store_path = "TEST_BOOKMARK_CRAWLER"
        assert not os.path.exists(download_config.store_path)

    # @classmethod
    # def tearDownClass(cls) -> None:
    #     shutil.rmtree(download_config.store_path)

    def test_run(self):
        debug_config.show_error = True
        proxy = os.getenv("https_proxy") or os.getenv("HTTPS_PROXY")
        if proxy is not None:
            network_config.proxy["https"] = proxy
            network_config.proxy["http"] = proxy
        cookie = os.getenv("PIXIV_COOKIE")
        uid = os.getenv("PIXIV_UID")
        assert cookie is not None and uid is not None
        user_config.cookie = cookie
        user_config.user_id = uid
        download_config.with_tag = True
        download_config.num_threads = 1
        download_config.bookmark = True
        download_config.timeout = 4
        download_config.url_only = False

        checkDir(download_config.store_path)
        app = PrivateBookmarkCrawler(n_images=48, capacity=1024)
        result = app.run()
        #
        # self.assertGreater(len(app.downloader.url_group), 20)
        # if download_config.url_only:
        #     self.assertEqual(result, app.downloader.url_group)
        #     self.assertEqual(len(os.listdir(download_config.store_path)), 0)
        # else:
        #     self.assertGreater(len(os.listdir(download_config.store_path)), 5)


if __name__ == "__main__":
    unittest.main()
