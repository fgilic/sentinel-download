import sys

import xml.etree.ElementTree as ET
import pyproj
from shapely import wkt
from shapely.ops import transform
import requests

USERNAME = "franegilic"
PASSWORD = "KhqSXUaPnpyQEMxZ"


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
        raise


def check_response_content(response):
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
        link = entry.find("{http://www.w3.org/2005/Atom}link").attrib["href"]
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
                polygon = wkt.loads(item.text)[0]
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
                "link": link,
                "size": size,
                "title": title,
                "footprint": polygon,
                "platformidentifier": platform_id,
                "orbit_direction": orbit_direction,
            }
        )
    return entries


def get_tile(tile_data):
    tile_url = tile_data['link']
    tile_title = tile_data["title"]
    tile_size = float(tile_data["size"].split(" ")[0])
    response = get_response(tile_url, stream=True)

    # try:
    #    check_response_content(response)
    # except ET.ParseError:
    #    pass

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

        print("\r", "Copleted downloading", f' {tile_title}', end="")


if __name__ == '__main__':
    pass
