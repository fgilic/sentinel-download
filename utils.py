import hashlib
import sys
import time

import xml.etree.ElementTree as ET
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
        print(f"Request timed out. ({connect_timeout})")
        raise
    except requests.exceptions.ConnectionError:
        print("Connection error. Check internet connection and URL.")
        raise
    except requests.exceptions.HTTPError:
        print(f"HTTP error, status code: {response.status_code}")
        if response.status_code == 401 and response.reason == "Unauthorized":
            print("Check you username and password.")
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
        title = entry.find("{http://www.w3.org/2005/Atom}title").text
        tile_id = entry.find("{http://www.w3.org/2005/Atom}id").text

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
        polygon = None
        platform_id = None
        orbit_direction = None
        for item in entry.findall("{http://www.w3.org/2005/Atom}str"):
            if item.attrib["name"] == "size":
                size = item.text
                continue
            if item.attrib["name"] == "footprint":
                # TODO sometimes polygon, sometimes multipolygon type
                polygon = wkt.loads(item.text)
                continue
            if item.attrib["name"] == "platformidentifier":
                platform_id = item.text
                continue
            if item.attrib["name"] == "orbitdirection":
                orbit_direction = item.text

        entries.append(
            {
                "id": tile_id,
                "beginposition": beginposition,
                "cloudcoverpercentage": cloudcoverpercentage_,
                "relativeorbitnumber": relativeorbitnumber,
                "download_uri": download_uri,
                "size": size,
                "title": title,
                "footprint": polygon,
                "platformidentifier": platform_id,
                "orbit_direction": orbit_direction,
            }
        )
    return entries


def get_md5_checksum(file_name):
    # https: // stackoverflow.com / questions / 16874598 / how - do - i - calculate - the - md5 - checksum - of - a - file - in -python
    # https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file/3431838#3431838
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def download_tile(response, tile_size, tile_title, tile_md5_checksum):
    # checks if file is already downloaded, and if it is and it has a valid MD5 checksum, than execution stops
    try:
        open(f'{tile_title}.zip', "xb")
    except FileExistsError:
        if tile_md5_checksum == get_md5_checksum(f'{tile_title}.zip'):
            print(f'{tile_title} already downloaded.')
            raise

    size_byte = 0.0
    tick = 0
    with open(f'{tile_title}.zip', "wb") as fd:
        for chunk in response.iter_content(chunk_size=2048):
            fd.write(chunk)
            size_byte += 2048
            percentage = ((size_byte / (1024 ** 3)) / tile_size * 100)
            if (percentage - tick) > 0:
                print("\r", "Downloading: ", f"{tick:3d} %", f' ({tile_title})', end="")
                tick += 1

        print("\r", "Completed downloading", f' {tile_title}', end="")

    if get_md5_checksum(f'{tile_title}.zip') != tile_md5_checksum:
        print("Download integrity problem (reported and calculated MD5 checksums are incompatible).")
        y_n = input("Reattempt download [Y/n]? ")
        if y_n == "Y" or "y":
            download_tile(response, tile_size, tile_title, tile_md5_checksum)
        sys.exit()


def get_tile(tile_data):
    download_uri = tile_data["download_uri"]
    tile_title = tile_data["title"]
    tile_size_unit = tile_data["size"].split(" ")[1]

    if tile_size_unit == "GB":
        tile_size = float(tile_data["size"].split(" ")[0])
    elif tile_size_unit == "MB":
        tile_size = float(tile_data["size"].split(" ")[0]) / 1000
    else:
        # TODO
        pass
    response = get_response(download_uri, stream=True)

    # try:
    #    check_response_content(response)
    # except ET.ParseError:
    #    pass
    while response.status_code == 202:
        print(f"Tile {tile_title} is offline. Retrieval request has been successfully submitted.")
        print(f"Download reattempt in 10 minutes.", end="")

        for i in range(10):
            if i == 9:
                print("\r", "Download reattempt in 10 minutes (less than 1 minute left).", end="")
                time.sleep(60)
                print("\r", "Download reattempt in 10 minutes.")
            else:
                print("\r", f"Download reattempt in 10 minutes ({10-i} minutes left).", end="")
                time.sleep(60)

        response = get_response(download_uri, stream=True)

    tile_md5_checksum = get_response(download_uri.replace("$value", "Checksum/Value/$value")).text
    download_tile(response, tile_size, tile_title, tile_md5_checksum)


if __name__ == '__main__':
    pass
