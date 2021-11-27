import json
import hashlib
import sys
import time

import xml.etree.ElementTree as ET
from shapely.geometry import shape
import rasterio
import pyproj
from shapely import wkt
from shapely.ops import transform
import requests

import local_settings

# TODO
# USERNAME = input("Username: ")
# PASSWORD = input("Password: ")

USERNAME = local_settings.USERNAME
PASSWORD = local_settings.PASSWORD


def get_intersecting_mgrs_tiles(footprint_polygon_file_path):
    with open(footprint_polygon_file_path, "r") as footprint_polygon_file:
        footprint_geometry = shape(json.loads(footprint_polygon_file.read())["features"][0]["geometry"])
        with open("MGRS_ll.geojson", "r") as mgrs_tiles:
            mgrs_tiles = json.loads(mgrs_tiles.read())
            intersecting_tiles = []
            tiles_utm = []
            for tile in mgrs_tiles["features"]:
                tile_geometry = shape(tile["geometry"])
                if footprint_geometry.intersects(tile_geometry):
                    centroid = tile["properties"]["centroid_ll"].split(",")
                    # switch lat and long
                    # TODO there are some tiles partly on east, partly on west hemisphere
                    if float(centroid[1]) < 40.0 or float(centroid[1]) > 52.0 or float(centroid[0]) < 12.0 or float(centroid[0]) >20.0:
                        continue
                    centroid = f"{centroid[1]},{centroid[0]}"
                    tile_utm_geometry = wkt.loads(tile["properties"]["UTM_WKT"])
                    intersecting_tiles.append({"centroid": centroid, "geometry": tile_utm_geometry, "EPSG_code": tile["properties"]["UTM_EPSG"], "MGRS_ID": tile["properties"]["MGRS_ID"]})

    return intersecting_tiles


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
        safe_file_id = entry.find("{http://www.w3.org/2005/Atom}id").text
        download_uri = entry.find("{http://www.w3.org/2005/Atom}link").attrib["href"]
        safe_file_name = entry.find("{http://www.w3.org/2005/Atom}str[@name='filename']").text
        cloudcoverpercentage_ = entry.find("{http://www.w3.org/2005/Atom}double[@name='cloudcoverpercentage']").text
        orbitnumber = entry.find("{http://www.w3.org/2005/Atom}int[@name='orbitnumber']").text
        relativeorbitnumber = entry.find("{http://www.w3.org/2005/Atom}int[@name='relativeorbitnumber']").text
        beginposition = entry.find("{http://www.w3.org/2005/Atom}date[@name='beginposition']").text
        safe_file_size = entry.find("{http://www.w3.org/2005/Atom}str[@name='size']").text
        # footprint = wkt.loads("{http://www.w3.org/2005/Atom}str[@name='footprint']".text) # sometimes polygon, sometimes multipolygon type
        platform_id = entry.find("{http://www.w3.org/2005/Atom}str[@name='platformidentifier']").text
        orbit_direction = entry.find("{http://www.w3.org/2005/Atom}str[@name='orbitdirection']").text

        entries.append(
            {
                "safe_file_id": safe_file_id,
                "download_uri": download_uri,
                "safe_file_name": safe_file_name,
                "cloudcoverpercentage": cloudcoverpercentage_,
                "orbitnumber": orbitnumber,
                "relativeorbitnumber": relativeorbitnumber,
                "beginposition": beginposition,
                "safe_file_size": safe_file_size,
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


def get_bands_metadata(safe_file_data, bands_no):
    safe_file_name = safe_file_data["safe_file_name"]
    safe_file_download_uri = safe_file_data["download_uri"]
    manifest_download_uri = safe_file_download_uri.replace("$value",f"Nodes('{safe_file_name}')/Nodes('manifest.safe')/$value")

    response = get_response(manifest_download_uri)
    root = get_xml_root(response)
    bands_metadata = []
    for data_object in root.iter('dataObject'):
        for band_no in bands_no:
            if f"Band_{band_no}" in data_object.attrib["ID"]:
                band_metadata = safe_file_data
                band_metadata["band_no"] = band_no
                band_metadata["id"] = data_object.attrib["ID"]
                band_metadata["band_file_size"] = int(data_object.find("byteStream").get("size"))
                band_metadata["file_location"] = data_object.find("byteStream").find("fileLocation").get("href")
                band_metadata["checksum_name"] = data_object.find("byteStream").find("checksum").get("checksumName")
                band_metadata["checksum"] = data_object.find("byteStream").find("checksum").text.lower()
                # https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
                bands_metadata.append(band_metadata.copy())
                break

    tile_folder_name = bands_metadata[0]["file_location"].split("/")[2]
    bands_metadata_download_uri = safe_file_download_uri.replace("$value", f"Nodes('{safe_file_name}')/Nodes('GRANULE')/Nodes('{tile_folder_name}')/Nodes('MTD_TL.xml')/$value")

    response = get_response(bands_metadata_download_uri)
    root = get_xml_root(response)
    geometric_info = root.find("{https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-2A_Tile_Metadata.xsd}Geometric_Info")
    for band_metadata in bands_metadata:
        band_metadata["horiz_crs_name"] = geometric_info.find("./Tile_Geocoding/HORIZONTAL_CS_NAME").text
        band_metadata["horiz_crs_code"] = geometric_info.find("./Tile_Geocoding/HORIZONTAL_CS_CODE").text
        if "10m" in band_metadata["band_no"]:
            resolution = 10
        elif "20m" in band_metadata["band_no"]:
            resolution = 20
        elif "60m" in band_metadata["band_no"]:
            resolution = 60
        rows = geometric_info.find(f"./Tile_Geocoding/Size[@resolution='{resolution}']/NROWS").text
        columns = geometric_info.find(f"./Tile_Geocoding/Size[@resolution='{resolution}']/NCOLS").text
        ulx = geometric_info.find(f"./Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULX").text
        uly = geometric_info.find(f"./Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULY").text
        xdim = geometric_info.find(f"./Tile_Geocoding/Geoposition[@resolution='{resolution}']/XDIM").text
        ydim = geometric_info.find(f"./Tile_Geocoding/Geoposition[@resolution='{resolution}']/YDIM").text

        band_metadata["rows"] = rows
        band_metadata["columns"] = columns
        band_metadata["ulx"] = ulx
        band_metadata["uly"] = uly
        band_metadata["xdid"] = xdim
        band_metadata["ydim"] = ydim
    return bands_metadata


def download_band(download_uri, root_download_folder, band_file_name, band_file_size, checksum, checksum_name):
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
            percentage = size_downloaded / band_file_size * 100
            if (percentage - tick) > 0:
                print("\r", "Downloading: ", f"{tick:2d} %", f' ({band_file_name})', end="")
                tick += 1

        print("\r", "Completed downloading", f' {band_file_name}')

    if get_checksum(f'{root_download_folder}\\{band_file_name}', checksum_name) != checksum:
        print("Download integrity problem (reported and calculated checksums are different).")
        y_n = input("Reattempt download [Y/n]? ")
        if y_n == "Y" or y_n == "y":
            download_band(download_uri, root_download_folder, band_file_name, band_file_size, checksum, checksum_name)


def download_bands(safe_file_download_uri, safe_file_name, bands_metadata, root_download_folder):

    downloaded_files = []
    for band_metadata in bands_metadata:
        tile_folder_name = band_metadata["file_location"].split("/")[2]
        band_file_name = band_metadata["file_location"].split("/")[-1]
        resolution_folder = "R" + band_metadata["band_no"].split("_")[1]
        # https://scihub.copernicus.eu/dhus/odata/v1/Products('aeff9a9c-5bf1-425c-8256-d26391156116')/Nodes('S2B_MSIL2A_20200822T094039_N0214_R036_T33TYH_20200822T115325.SAFE')/Nodes('GRANULE')/Nodes('L2A_T33TYH_A018080_20200822T094034')/Nodes('IMG_DATA')/Nodes('R10m')/Nodes('T33TYH_20200822T094039_B02_10m.jp2')/
        band_download_uri = safe_file_download_uri.replace("$value", f"Nodes('{safe_file_name}')/Nodes('GRANULE')/Nodes('{tile_folder_name}')/Nodes('IMG_DATA')/Nodes('{resolution_folder}')/Nodes('{band_file_name}')/$value")
        download_band(band_download_uri, root_download_folder, band_file_name, band_metadata["band_file_size"], band_metadata["checksum"], band_metadata["checksum_name"])

        downloaded_files.append(band_file_name)

        try:
            json_file = open(f"{root_download_folder}\\METADATA.json", "x")
            json_file.write(json.dumps([band_metadata]))
            json_file.close()
        except FileExistsError:
            json_file = open(f"{root_download_folder}\\METADATA.json", "r+")
            json_file_content = json.loads(json_file.read())
            json_file.seek(0)
            json_file_content.append(band_metadata)
            json_file.write(json.dumps(json_file_content))
            json_file.close()

    return downloaded_files


def get_bands(safe_file_data, bands_no, root_dowload_folder):
    safe_file_download_uri = safe_file_data["download_uri"]
    safe_file_name = safe_file_data["safe_file_name"]

    response = get_response(safe_file_download_uri, stream=True)

    while response.status_code == 202:
        print(f"SAFE file {safe_file_name} is offline. Retrieval request has been successfully submitted.")
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

    bands_metadata = get_bands_metadata(safe_file_data, bands_no)

    # try:
    #    check_response_content(response)
    # except ET.ParseError:
    #    pass

    downloaded_files = download_bands(safe_file_download_uri, safe_file_name, bands_metadata, root_dowload_folder)
    return downloaded_files

if __name__ == '__main__':
    pass
