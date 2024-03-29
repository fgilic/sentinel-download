import csv
import sys

from mosaicking import create_rgb_composite, merge_rgb
from utils import build_search_params, get_response, get_xml_root, parse_search_results, get_bands


if __name__ == '__main__':
    rows = 10  # 10 is also default if not defined, max is 100
    start = 0  # 0 is also default if not defined

    root_search_uri = "https://scihub.copernicus.eu/dhus/search"
    producttype = "S2MSI2A"
    # beginposition = "[NOW-9MONTHS TO NOW]"
    beginposition = "[2021-08-01T00:00:00.000Z TO 2021-08-31T00:00:00.000Z]"
    # point: '(Lat, Long)'; polygon: 'POLYGON((Long1 Lat1, Long2 Lat2, ..., Long1 Lat1))'
    # Lat and Long in decimal degrees
    # http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
    # footprint = "POLYGON((16.1430 43.3531, 16.7802 43.3531, 16.7802 43.6489, 16.1430 43.6489, 16.1430 43.3531))"
    footprint = ["44.65, 15.60", "44.65, 16.93", "43.74, 15.60", "43.74, 16.93"]
    cloudcoverpercentage = "[0 TO 15]"
    bands_no = ["B02_10m", "B03_10m", "B04_10m"]
    root_download_folder = "D:\\2-S2-mosaick\\bands"

    for point in footprint:
        search_params = build_search_params(rows, start, producttype, beginposition, point, cloudcoverpercentage)

        search_response = get_response(root_search_uri, search_params)
        xml_root = get_xml_root(search_response)
        all_entries = []
        total_results = int(
            xml_root.find("{http://a9.com/-/spec/opensearch/1.1/}totalResults").text
        )

        if total_results == 0:
            print("No results with provided search parameters.")
            sys.exit()

        print(f"\nSearch URI: {search_response.url}")
        print(f"Total results: {total_results}")

        all_entries += parse_search_results(xml_root)

        if total_results > rows:
            start += 10
            while start < total_results:
                search_params = build_search_params(
                    rows, start, producttype, beginposition, footprint, cloudcoverpercentage
                )
                search_response = get_response(root_search_uri, search_params)
                xml_root = get_xml_root(search_response)
                all_entries += parse_search_results(xml_root)
                start += 10

        # sorting entries by cloudcoverpercentage, ascending
        all_entries.sort(key=lambda cover_percentage: cover_percentage["cloudcoverpercentage"])
        print ("Product (SAFE file) id: " + all_entries[0]["safe_file_id"])

        downloaded_files = get_bands(all_entries[0], bands_no, root_download_folder)

        create_rgb_composite(downloaded_files, root_download_folder)

    merge_rgb()