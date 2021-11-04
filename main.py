from utils import build_search_params, get_response, check_response_content, parse_search_results, get_tile


if __name__ == '__main__':
    rows = 10  # 10 is also default if not defined, max is 100
    start = 0  # 0 is also default if not defined

    root_search_uri = "https://scihub.copernicus.eu/dhus/search"
    producttype = "S2MSI2A"
    # beginposition = "[NOW-9MONTHS TO NOW]"
    beginposition = "[2020-08-01T00:00:00.000Z TO 2020-08-31T00:00:00.000Z]"
    # point: '(Lat, Long)'; polygon: 'POLYGON((Long1 Lat1, Long2 Lat2, ..., Long1 Lat1))'
    # Lat and Long in decimal degrees
    # http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
    # footprint = "POLYGON((16.1430 43.3531, 16.7802 43.3531, 16.7802 43.6489, 16.1430 43.6489, 16.1430 43.3531))"
    footprint = "42.807492, 18.264526"
    cloudcoverpercentage = "[0 TO 15]"

    search_params = build_search_params(rows, start, producttype, beginposition, footprint, cloudcoverpercentage)

    search_response = get_response(root_search_uri, search_params)
    xml_root = check_response_content(search_response)
    all_entries = []
    total_results = int(
        xml_root.find("{http://a9.com/-/spec/opensearch/1.1/}totalResults").text
    )
    print(f"URI: {search_response.url}\n")
    print(f"Total results: {total_results}")

    all_entries += parse_search_results(xml_root)

    if total_results > rows:
        start += 10
        while start < total_results:
            search_params = build_search_params(
                rows, start, producttype, beginposition, footprint, cloudcoverpercentage
            )
            search_response = get_response(root_search_uri, search_params)
            xml_root = check_response_content(search_response)
            all_entries += parse_search_results(xml_root)
            start += 10

    # sorting entries by cloudcoverpercentage, ascending
    all_entries.sort(key=lambda cover_percentage: cover_percentage["cloudcoverpercentage"])

    get_tile(all_entries[0])
