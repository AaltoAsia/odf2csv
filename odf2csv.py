#!python3
import glob, csv, sys, os, argparse, gzip
from datetime import datetime, timedelta, timezone
from queue import PriorityQueue, Empty

def eprint(*args, **kwargs): print(*args, file=sys.stderr, **kwargs)
def noprint(*args, **kwargs): pass

try:
    from lxml import etree
except ImportError:
    eprint("ERROR: missing lxml library; Try `pip3 install lxml` and then run again.")
    sys.exit(2)


DEFAULTS_MAX_HEADER_PATH_LENGTH=15  # in path segments



parser = argparse.ArgumentParser(description=
'''Parses O-DF xml to csv format. It is essential that the xmlns is correct and
the input O-DF data is sorted ascending in time (within an InfoItem, unless --sort flag is given).''')

parser.add_argument('files', metavar='o-df-file', type=open, nargs='+',
                                        help='O-DF or O-MI file (in xml format). Multiple files can be given.')
parser.add_argument('--output', '-o', dest='output',# type=argparse.FileType('w'),
                    help='Output csv file')

parser.add_argument('--overwrite', '-f', '--force', dest='overwrite', default=False, action='store_true',
                    help='Overwrite the output csv file if it already exists.')

parser.add_argument('--xmlns', '--ns', '--odf-xmlns', dest='odfVersion', default="http://www.opengroup.org/xsd/odf/1.0/",
                    help='Xml namespace for the O-DF; This must match the uri in the input files, including the version number. Otherwise, xpath will not work. Will be assigned to "odf:" and "d" prefixes in XPath')

parser.add_argument('--omi-xmlns', dest='omiVersion', default="http://www.opengroup.org/xsd/omi/1.0/",
                    help='Xml namespace for the O-MI if needed in XPath queries. Will be assigned to "omi:" and "m" prefixes in XPath')

parser.add_argument('--select-xpath', dest='select',
                        default="//odf:InfoItem", #type=etree.XPath,
                    help='XPath to select certain InfoItems from the O-DF (use "odf:" or "d:" as namespace for elements).')
                    #help='XPath to select parts of O-DF (use "odf:" or "d:" as namespace for elements) before InfoItems of the resulting structure are iterated.')

pathSelectorStr = '|'.join(map(lambda x: '/'.join(x*['..']) + '/odf:id/text()', range(DEFAULTS_MAX_HEADER_PATH_LENGTH,0,-1))) + "|./@name"

parser.add_argument('--header-xpath', dest='headerSelect',
                    default=pathSelectorStr, #type=etree.XPath,
                    help=('XPath to select header title for each InfoItem. The '
                    'XPath is run from each InfoItem (tip: recall the parent '
                    'selector "../"). The result is further handled by '
                    '"./odf:id/text()|@name" and results joined with '
                    "'/'-character. The default is the O-DF path to the "
                    'item without the root /Objects/. Use "odf:" or "d:" as the namespace for elements.'))

parser.add_argument('--merge-columns', dest='mergeColumns',
                    default=False, action='store_true',
                    help='Merge columns with the same header name. By default, only combines columns having the same O-DF path and puts them in the order found.'
                    )

parser.add_argument('--sort', dest='sort',
                    default=False, action='store_true',
                    help='Sorts the values in ascending order. This is needed, if they are not sorted in the input O-DF. Note: Loads all data to memory; not recommended for large files atm.'
                    )

#parser.add_argument('--read-window', dest='readwindow',
#                    type=int, default=720)

parser.add_argument('--verbose', '-v', dest='debug',
                    action='store_const',const=eprint, default=noprint, 
                    help="Print extra information during processing")
parser.add_argument('--version', action='version', version="odf2csv v0.1")

args = parser.parse_args()
debug = args.debug

odfVersion = args.odfVersion
ns = {"odf" : odfVersion, "d": odfVersion, "omi": args.omiVersion, "m": args.omiVersion}

if 'output' not in args:
    filename = os.path.splitext(args.files[0].name)[0] + ".csv"
    debug("No output file name given, using:", filename)
    args.output = filename

if not args.overwrite and os.path.exists(args.output):
    eprint("ERROR: output file", args.output, " exists!")
    sys.exit(3)

pathSelectorXpath = etree.XPath(pathSelectorStr, namespaces=ns)
args.select = etree.XPath(args.select, namespaces=ns)
args.headerSelect = etree.XPath(args.headerSelect, namespaces=ns)
args.output = open(args.output, 'w')


debug("Note: Works correctly with O-DF hierarchy depth up to", DEFAULTS_MAX_HEADER_PATH_LENGTH)

# The Plan:
# core: --select-xpath='' --headers-xpath='' --xmlns='' --output=file <file> [files..]
# usage: odf2csv.py --ignore-infoitems='item1,item2' --select-infoitems='item1,item2' --select-xpath='' --headers=xpath <file> [files...]

# NOTES
# * many files will behandled as combined O-DF structure
# * xpath parent "/.." works even after attribute selection (//odf:id[text()="Settings"]/..)

#import pdb; pdb.set_trace()




def xpath(root, query): return root.xpath(query, namespaces=ns)
def find(root, query): return root.iterfind(query, namespaces=ns)
#def show(e): eprint(etree.tostring(e).decode("utf8"))



def processInputFile(file):
    name = file.name
    if name.endswith(".gz"):
        file.close()
        return gzip.open(file.name, mode="r")
    else: return file

debug("Parsing xml files:", args.files)
dataRoots = [etree.parse(processInputFile(xmlFile)) for xmlFile in args.files]

infoItemSelector = ''#'//odf:InfoItem' # TODO: do not select MetaData InfoItems
debug("Filtering data with xpaths:", args.select, ',', infoItemSelector)
dataFiltered = (customResults #item
                for data in dataRoots
                for customResults in args.select(data) 
                #for item in xpath(customResults, infoItemSelector)
                )

debug("Matching same O-DF paths")
def slashEscape(string): return string.replace('/', '\\/')
def createOdfPath(item, selector=pathSelectorXpath):
    return '/'.join(map(slashEscape, selector(item)))

class Item:
    def __init__(s, path, itemElem):
        s.elem = itemElem
        #s.path = path  # not used
        s.header = createOdfPath(s.elem, args.headerSelect)
        s.headerPos = None

class ValueParser:
    def __init__(s, item):
        s.iter = find(item, 'odf:value') #valueIter
        s.advance()
    def time(s):
        return int(s.valueElem.get('unixTime'))
    def value(s):
        return s.valueElem.text
    def advance(s):
        s.valueElem = next(s.iter, None)
        #show(s.valueElem)
        #return s.valueElem

from dataclasses import dataclass, field
from typing import Any

@dataclass(order=True)
class PQValue:
    value: ValueParser= field(compare=False)
    item: Item        = field(compare=False)
    priority: int     = field(init=False)
    def __post_init__(s):
        s.priority = s.value.time()

@dataclass(order=True)
class HeaderPos:
    headerPos: int

# 0. create writer and create headers
# 1. add next value of each item into priority queue sorted by value timestamp
# 2. start a row
# 3. take values to row until timestamp changes, then go to step 2


# Match InfoItems which have same path but keep their value iterators separated because time can flow differently 
counter = 0
valueQueue = PriorityQueue()
headers = ['UTC-Time']
items = {}
for item in dataFiltered:
    counter += 1
    path = createOdfPath(item, args.headerSelect) if args.mergeColumns else createOdfPath(item)
    #debug(path)
    if path not in items:
        tmp = Item(path, item)
        tmp.headerPos = len(headers)
        headers.append(tmp.header)
        items[path] = tmp
    #else:
        #items[path].addSource(item)
    v = ValueParser(item)
    if args.sort:
        while v.valueElem != None:
            valueQueue.put((v.time(), v.value(), HeaderPos(items[path].headerPos)))
            v.advance()
    else:
        valueQueue.put(PQValue(v, items[path]))



debug("Total InfoItems:", counter, ", after merging:", len(items))

if len(items) == 0:
    eprint("ERROR: No InfoItems found")
    sys.exit(4)
if valueQueue.empty():
    eprint("ERROR: No values found!")
    sys.exit(5)

# 0. headers
writer = csv.writer(args.output)
#headers = ['UTC-Time'] + list(map(lambda x: x.header, items.values()))
writer.writerow(headers)

# 1. priority queue
# valueQueue

def getValue():
    val = valueQueue.get_nowait()
    value = val.value.value() # parse value
    val.value.advance()
    if val.value.valueElem != None:
        valueQueue.put(PQValue(val.value, val.item))
    return (val.priority, value, val.item) # unixtime, value

def timeformat(unixtime):
    time = datetime.fromtimestamp(unixtime, timezone.utc)
    return time.strftime('%Y-%m-%d %H:%M:%S')


noneRow = [None] * len(headers)
timestamp = nextValue = item = None
lastTime = 0
row = []
rowCounter = 0
valueCounter = 0
rowValueCounter = 0

def status(): debug("Processed", rowCounter, "rows and", valueCounter, "values.")#, "values, values/full_row =", rowValueCounter/len(items))

while not valueQueue.empty():
    timestamp, nextValue, item = getValue() if not args.sort else valueQueue.get_nowait()

    if timestamp > lastTime:
        if len(row) > 0:
            writer.writerow(row)
            rowCounter += 1
        # 2. start a row
        rowValueCounter = 0
        row = [*noneRow] # copy Nones
        row[0] = timeformat(timestamp)

    # 3. collect values
    row[item.headerPos] = nextValue

    valueCounter += 1
    rowValueCounter += 1
    if (valueCounter & 0b1111111111111) == 0b1000000000000: status()

    lastTime = timestamp

status()
debug("Row filling: average values/full_row =", round(valueCounter/(len(items)*rowCounter), 2))

args.output.close()

debug("Done.")

#tables = ['Temperature', 'Set-Point', 'Valve-Position']
#valueIters = {}  # {device : { sensor : iter}}
#for device in find(fourdegObject, 'odf:Object'):
#  odfId = next(find(device, 'odf:id')).text
#
#  sensorIters = {}
#  for sensor in tables:
#    item = xpath(device, './odf:InfoItem[@name="{}"]'.format(sensor))
#    if len(item) == 0:
#      sensorIters[sensor] = iter([])
#    else:
#      sensorIters[sensor] = find(item[0], 'odf:value')
#
#  valueIters[odfId] = sensorIters
#
#
#def deviceTemplate(device): return {device : {}}
#readWindow = {} # {time : {device : {sensor : value}}}
#hour = timedelta(hours=1)
#
#files = {sensor : open(xml_files.replace('.xml','-') + sensor + '.csv', 'w', encoding='utf-8') for sensor in tables}
#writers = {sensor : csv.writer(files[sensor]) for sensor in tables}
#deviceNames = list(valueIters.keys())
#headers = ['UTC-Time'] + deviceNames
#
#for writer in writers.values(): writer.writerow(headers)
#
#class Empty:
#  def get(a, b): return b
#
#def writeRow(time):
#  devices = readWindow[time]
#  for sensor in tables:
#    #for device, sensorData in devices:
#    row = [time.strftime('%Y-%m-%d %H:%M:%S')]
#    row += [devices.get(device, Empty).get(sensor, None) for device in deviceNames]
#    writers[sensor].writerow(row)
#    
#def handleWriteRow():
#  minTime = min(readWindow.keys())
#  writeRow(minTime)
#  del readWindow[minTime]
#  
#sensorsLeft = sum([len(x) for x in valueIters.values()])
#counter = 0
#while len(readWindow) > 0 or counter == 0:
#  counter += 1
#  for device, sensorIters in valueIters.items():
#
#    for sensor, valueIter in sensorIters.items():
#
#      value = next(valueIter, None)
#      if value is None:
#        continue
#
#      unixtime = int(value.get('unixTime'))
#      time = datetime.fromtimestamp(unixtime, timezone.utc)
#      timeFloor = time.replace(minute=0, second=0)
#
#      if not timeFloor in readWindow:
#        readWindow[timeFloor] = {}
#      if not device in readWindow[timeFloor]: # clean code?
#        readWindow[timeFloor][device] = {}
#
#      readWindow[timeFloor][device][sensor] = value.text
#
#  if counter >= READWINDOW:
#    handleWriteRow()
#
#
#
#for f in files.values(): f.close() 

