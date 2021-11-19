import rasterio

def create_rgb_composite(downloaded_files, root_download_folder):

    for downloaded_file in downloaded_files:
        if "B04_10m" in downloaded_file:
            red = rasterio.open(f"{root_download_folder}\\{downloaded_file}")
            composite_path_file_name = f"{root_download_folder}\\" + downloaded_file.replace("B04_10m", "RGB").replace("jp2","tif")
        elif "B03_10m" in downloaded_file:
            green = rasterio.open(f"{root_download_folder}\\{downloaded_file}")
        elif "B02_10m" in downloaded_file:
            blue = rasterio.open(f"{root_download_folder}\\{downloaded_file}")

    kwds = red.profile
    kwds['driver'] = 'GTiff'
    kwds['tiled'] = True
    kwds['blockxsize'] = 256
    kwds['blockysize'] = 256
    # kwds['photometric'] = 'RGB'
    # kwds['compress'] = 'lzw'
    kwds['predictor'] = 2
    kwds['count'] = 3

    # https://gis.stackexchange.com/questions/341809/merging-sentinel-2-rgb-bands-with-rasterio
    with rasterio.open(composite_path_file_name, "w", **kwds) as composite:
        composite.write(red.read(1), 1)
        composite.write(green.read(1), 2)
        composite.write(blue.read(1), 3)