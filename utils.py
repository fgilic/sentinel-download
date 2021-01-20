import sys
import xml.etree.ElementTree as ET
import requests

username = 'franegilic'
password = 'KhqSXUaPnpyQEMxZ'


def get_results(rows_, start_):
    producttype = 'S2MSI2A'
    beginposition = '[NOW-2MONTHS TO NOW]'
    # point: '(Lat, Long)'; polygon: 'POLYGON((Lat1 Long1, Lat2 Long2, ..., Lat1 Long2))'; Lat and Long id decimal degrees
    footprint = 'POLYGON((43.3531 16.1430, 43.3531 16.7802, 43.6489 16.7802, 43.6489 16.1430, 43.3531 16.1430))'
    cloudcoverpercentage = '[0 TO 15]'

    query = f'producttype:{producttype} AND beginposition:{beginposition} AND footprint:"intersects({footprint})" ' \
            f'AND cloudcoverpercentage:{cloudcoverpercentage}'

    payload = {'q': query, 'rows': rows_, 'start': start_}
    return requests.get(
        f'https://scihub.copernicus.eu/dhus/search', params=payload, auth=(username, password), timeout=(3.1, 60.1))


def check_response(response):
    if response.status_code == requests.codes.ok:
        pass
    else:
        print(f'HTTP status code: {response.status_code}')
        response.raise_for_status()


def check_results(response):
    check_response(response)
    root = ET.fromstring(response.content)

    if root[0].tag == '{http://www.w3.org/2005/Atom}error':
        print(f'Error while querying data.')
        print(f'Error code: {root[0].find("{http://www.w3.org/2005/Atom}code").text}.')
        print(f'Error message: {root[0].find("{http://www.w3.org/2005/Atom}message").text}.')
        # https://stackoverflow.com/questions/19782075/how-to-stop-terminate-a-python-script-from-running/34029481
        sys.exit()
    return root


def parse_results(xml_root):
    entries = []
    for entry in xml_root.findall('{http://www.w3.org/2005/Atom}entry'):
        link = entry.find('{http://www.w3.org/2005/Atom}link').attrib['href']
        title = entry.find('{http://www.w3.org/2005/Atom}title').text
        id = entry.find('{http://www.w3.org/2005/Atom}id').text

        cloudcoverpercentage_ = None
        for item in entry.findall('{http://www.w3.org/2005/Atom}double'):
            if item.attrib['name'] == 'cloudcoverpercentage':
                cloudcoverpercentage_ = item.text
                break

        relativeorbitnumber = None
        for item in entry.findall('{http://www.w3.org/2005/Atom}int'):
            if item.attrib['name'] == 'relativeorbitnumber':
                relativeorbitnumber = item.text
                break

        beginposition = None
        for item in entry.findall('{http://www.w3.org/2005/Atom}date'):
            if item.attrib['name'] == 'beginposition':
                beginposition = item.text
                break

        size = None
        for item in entry.findall('{http://www.w3.org/2005/Atom}str'):
            if item.attrib['name'] == 'size':
                size = item.text
                break

        entries.append(
            {'id': id, 'beginposition': beginposition, 'cloudcoverpercentage': cloudcoverpercentage_,
             'relativeorbitnumber': relativeorbitnumber, 'link': link, 'size': size, 'title': title})
    return entries


def get_data(entries):
    r = requests.get(entries[0]['link'], auth=(username, password), stream=True)
    check_response(r)

    size_byte = 0.0
    tick = 0
    with open(f'{entries[0]["title"]}.zip', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=2048):
            fd.write(chunk)
            size_byte += 2048
            percentage = (size_byte / (1024 ** 3)) / float(entries[0]['size'].split(' ')[0]) * 100
            if (percentage - tick) > 0:
                print('\r', 'Downloading: ', f'{tick} %', f' ({entries[0]["title"]})', end="")
                tick += 1

        print('\r', 'Copleted downloading', f' {entries[0]["title"]}', end="")


rows = 10
start = 0
response = get_results(rows, start)
xml_root = check_results(response)
all_entries = []
total_results = int(xml_root.find("{http://a9.com/-/spec/opensearch/1.1/}totalResults").text)
print(f'URI: {response.url}\n')
print(f'Total results: {total_results}')

all_entries.append(parse_results(xml_root))

if total_results > rows:
    start = 10
    while start < total_results:
        response = get_results(rows, start)
        xml_root = check_results(response)
        all_entries.append(parse_results(xml_root))
        start += 10

# sorting entries by cloudcoverpercentage, ascending
all_entries.sort(key=lambda cover_percentage: cover_percentage['cloudcoverpercentage'])

print(all_entries)

get_data(all_entries)