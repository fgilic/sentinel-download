import hashlib
import sys
import time

import xml.etree.ElementTree as ET
import rasterio
import pyproj
from shapely import wkt
from shapely.ops import transform
import requests

USERNAME = input("Username: ")
PASSWORD = input("Password: ")


def build_search_params(rows, start, producttype, beginposition, footprint, cloudcoverpercentage):
    # double quotes in footprint:"intersects()"
    query = (
        f"producttype:{producttype} AND beginposition:{beginposition} AND footprint:\"intersects({footprint})\" "
        f"AND cloudcoverpercentage:{cloudcoverpercentage}"
    )

    return {"q": query, "rows": rows, "start": start}


def get_response(root_url, params="", stream=False):
    connect_timeout = 3.1
    read_timeout = 60.1

    try:
        response = requests.get(
            root_url,
            params=params,
            auth=(USERNAME, PASSWORD),
            timeout=(connect_timeout, read_timeout),
            stream=stream,
        )

        response.raise_for_status()

        return response
    except requests.exceptions.Timeout:
        print(f"Request timed out. (connection timeout: {connect_timeout} s, read timeout: {read_timeout} s)")
        raise
    except requests.exceptions.ConnectionError:
        print("Connection error. Check internet connection and URL.")
        raise
    except requests.exceptions.HTTPError:
        print(f"HTTP error, status code: {response.status_code}")
        if response.status_code == 401 and response.reason == "Unauthorized":
            print("Check your username and password.")
        raise


def get_xml_root(response):
    root = ET.fromstring(response.content)

    if root[0].tag == "{http://www.w3.org/2005/Atom}error":
        print("Error while querying data.")
        print(f'Error code: {root[0].find("{http://www.w3.org/2005/Atom}code").text}.')
        print(
            f'Error message: {root[0].find("{http://www.w3.org/2005/Atom}message").text}.'
        )
        # https://stackoverflow.com/questions/19782075/how-to-stop-terminate-a-python-script-from-running/34029481
        sys.exit()
    return root


def parse_search_results(xml_root):
    entries = []
    for entry in xml_root.findall("{http://www.w3.org/2005/Atom}entry"):
        download_uri = entry.find("{http://www.w3.org/2005/Atom}link").attrib["href"]
        safe_file_title = entry.find("{http://www.w3.org/2005/Atom}title").text + ".SAFE"
        safe_file_id = entry.find("{http://www.w3.org/2005/Atom}id").text

        cloudcoverpercentage_ = None
        for item in entry.findall("{http://www.w3.org/2005/Atom}double"):
            if item.attrib["name"] == "cloudcoverpercentage":
                cloudcoverpercentage_ = item.text
                break

        relativeorbitnumber = None
        for item in entry.findall("{http://www.w3.org/2005/Atom}int"):
            if item.attrib["name"] == "relativeorbitnumber":
                relativeorbitnumber = item.text
                break

        beginposition = None
        for item in entry.findall("{http://www.w3.org/2005/Atom}date"):
            if item.attrib["name"] == "beginposition":
                beginposition = item.text
                break

        size = None
        footprint = None
        platform_id = None
        orbit_direction = None
        for item in entry.findall("{http://www.w3.org/2005/Atom}str"):
            if item.attrib["name"] == "size":
                size = item.text
                continue
            if item.attrib["name"] == "footprint":
                # TODO sometimes polygon, sometimes multipolygon type
                footprint = wkt.loads(item.text)
                continue
            if item.attrib["name"] == "platformidentifier":
                platform_id = item.text
                continue
            if item.attrib["name"] == "orbitdirection":
                orbit_direction = item.text

        entries.append(
            {
                "id": safe_file_id,
                "beginposition": beginposition,
                "cloudcoverpercentage": cloudcoverpercentage_,
                "relativeorbitnumber": relativeorbitnumber,
                "download_uri": download_uri,
                "size": size,
                "title": safe_file_title,
                "footprint": footprint,
                "platformidentifier": platform_id,
                "orbit_direction": orbit_direction,
            }
        )
    return entries


def build_rgb_composite():
    pass


def get_checksum(file_name, checksum_name):
    # https: // stackoverflow.com / questions / 16874598 / how - do - i - calculate - the - md5 - checksum - of - a - file - in -python
    # https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file/3431838#3431838
    if checksum_name == "MD5":
        hash_md5 = hashlib.md5()
        with open(file_name, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    elif checksum_name == "SHA3-256":
        hash_sha3_256 = hashlib.sha3_256()
        with open(file_name, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_sha3_256.update(chunk)
        return hash_sha3_256.hexdigest()
    else:
        # TODO
        print(f"Unexpected checksum_name (hash algorithm): {checksum_name}")
        sys.exit()


def get_bands_metadata(manifest_download_uri, bands_no):
    response = get_response(manifest_download_uri)
    root = get_xml_root(response)
    bands_metadata = []
    for data_object in root.iter('dataObject'):
        band_metadata = {}
        band_metadata["id"] = data_object.attrib["ID"]
        band_metadata["size"] = int(data_object.find("byteStream").get("size"))
        band_metadata["file_location"] = data_object.find("byteStream").find("fileLocation").get("href")
        band_metadata["checksum_name"] = data_object.find("byteStream").find("checksum").get("checksumName")
        band_metadata["checksum"] = data_object.find("byteStream").find("checksum").text.lower()

        band_metadata["band_no"] = None
        for band_no in bands_no:
            if f"Band_{band_no}" in band_metadata["id"]:
                band_metadata["band_no"] = band_no
                break

        bands_metadata.append(band_metadata)

    return bands_metadata


def download_band(download_uri, root_download_folder, band_file_name, size, checksum, checksum_name):
    # checks if file is already downloaded, and if it is and it has a valid MD5 checksum, than it doesn't download band
    try:
        open(f'{root_download_folder}\\{band_file_name}', "xb")
    except FileExistsError:
        if checksum == get_checksum(f'{root_download_folder}\\{band_file_name}', checksum_name):
            print(f' {band_file_name} already downloaded.')
            return

    size_downloaded = 0.0
    tick = 0
    response = get_response(download_uri, stream=True)
    with open(f'{root_download_folder}\\{band_file_name}', "wb") as fd:
        for chunk in response.iter_content(chunk_size=2048):
            fd.write(chunk)
            size_downloaded += 2048
            percentage = size_downloaded / size * 100
            if (percentage - tick) > 0:
                print("\r", "Downloading: ", f"{tick:2d} %", f' ({band_file_name})', end="")
                tick += 1

        print("\r", "Completed downloading", f' {band_file_name}')

    if get_checksum(f'{root_download_folder}\\{band_file_name}', checksum_name) != checksum:
        print("Download integrity problem (reported and calculated checksums are different).")
        y_n = input("Reattempt download [Y/n]? ")
        if y_n == "Y" or y_n == "y":
            download_band(download_uri, root_download_folder, band_file_name, size, checksum)


def download_bands(safe_file_download_uri, safe_file_title, bands_metadata, root_download_folder):

    downloaded_files = []
    for band_metadata in bands_metadata:
        if band_metadata["band_no"] is not None:
            tile_folder_name = band_metadata["file_location"].split("/")[2]
            band_file_name = band_metadata["file_location"].split("/")[-1]
            resolution_folder = "R" + band_metadata["band_no"].split("_")[1]
            # https://scihub.copernicus.eu/dhus/odata/v1/Products('aeff9a9c-5bf1-425c-8256-d26391156116')/Nodes('S2B_MSIL2A_20200822T094039_N0214_R036_T33TYH_20200822T115325.SAFE')/Nodes('GRANULE')/Nodes('L2A_T33TYH_A018080_20200822T094034')/Nodes('IMG_DATA')/Nodes('R10m')/Nodes('T33TYH_20200822T094039_B02_10m.jp2')/
            band_download_uri = safe_file_download_uri.replace("$value", f"Nodes('{safe_file_title}')/Nodes('GRANULE')/Nodes('{tile_folder_name}')/Nodes('IMG_DATA')/Nodes('{resolution_folder}')/Nodes('{band_file_name}')/$value")
            download_band(band_download_uri, root_download_folder, band_file_name, band_metadata["size"], band_metadata["checksum"], band_metadata["checksum_name"])

            downloaded_files.append(band_file_name)

    return downloaded_files


def get_bands(safe_file_data, bands_no, root_dowload_folder):
    safe_file_download_uri = safe_file_data["download_uri"]
    safe_file_title = safe_file_data["title"]
    manifest_download_uri = safe_file_download_uri.replace("$value", f"Nodes('{safe_file_title}')/Nodes('manifest.safe')/$value")

    response = get_response(safe_file_download_uri, stream=True)

    while response.status_code == 202:
        print(f"SAFE file {safe_file_title} is offline. Retrieval request has been successfully submitted.")
        print(f"Download reattempt in 10 minutes.", end="")

        for i in range(10):
            if i == 9:
                print("\r", "Download reattempt in 10 minutes (less than 1 minute left).", end="")
                time.sleep(60)
                print("\r", "Download reattempt in 10 minutes.")
            else:
                print("\r", f"Download reattempt in 10 minutes ({10-i} minutes left).", end="")
                time.sleep(60)

        response = get_response(safe_file_download_uri, stream=True)

    bands_metadata = get_bands_metadata(manifest_download_uri, bands_no)

    # try:
    #    check_response_content(response)
    # except ET.ParseError:
    #    pass

    downloaded_files = download_bands(safe_file_download_uri, safe_file_title, bands_metadata, root_dowload_folder)
    return downloaded_files

if __name__ == '__main__':
    pass
