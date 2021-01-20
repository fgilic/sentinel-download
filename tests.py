import unittest

from utils import sort_entries_by_cloud_coverage


class MyTestCase(unittest.TestCase):
    def test_sorting_cloud(self):
        entries = [
            {'uuid': 11, 'cloudcoverpercentage': 50},
            {'uuid': 22, 'cloudcoverpercentage': 100},
            {'uuid': 33, 'cloudcoverpercentage': 27},
            {'uuid': 44, 'cloudcoverpercentage': 36}
        ]

        expected_result = [
            {'uuid': 44, 'cloudcoverpercentage': 27},
            {'uuid': 33, 'cloudcoverpercentage': 36},
            {'uuid': 22, 'cloudcoverpercentage': 50},
            {'uuid': 11, 'cloudcoverpercentage': 100}
        ]
        self.assertEqual(sort_entries_by_cloud_coverage(entries), expected_result)


if __name__ == '__main__':
    unittest.main()
