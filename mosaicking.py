import rasterio.merge

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
        print(f" RGB composite {composite_path_file_name} created.")


def merge_rgb():
    rst4 = rasterio.open("D:\\2-S2-mosaick\\bands\\T33TXJ_20210815T095031_RGB.tif")
    rst1 = rasterio.open("D:\\2-S2-mosaick\\bands\\T33TWK_20210815T095031_RGB.tif")
    rst3 = rasterio.open("D:\\2-S2-mosaick\\bands\\T33TWJ_20210815T095031_RGB.tif")
    rst2 = rasterio.open("D:\\2-S2-mosaick\\bands\\T33TXK_20210815T095031_RGB.tif")
    datasets = [rst1, rst2, rst3, rst4]
    output = rasterio.merge.merge(datasets)
    output_tif = rasterio.open("D:\\2-S2-mosaick\\bands\\mosaick.tif", "w", driver="GTiff", transform=output[1], crs=rst4.crs, width=20982, height=20982, count=3, dtype="uint16")
    output_tif.write(output[0][2], 3)
    output_tif.write(output[0][0], 1)
    output_tif.write(output[0][1], 2)
    output_tif.close()

if __name__ == '__main__':
    merge_rgb()
