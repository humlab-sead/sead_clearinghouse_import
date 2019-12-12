import io
import base64
import logging
from xml.dom import minidom
import time
import zlib

def setup_logger(logger, filename=None, level=logging.DEBUG):
    '''
    Setup logging of import messages to both file and console
    '''

    filename = filename or './explode_{}.log'.format(time.strftime("%Y%m%d-%H%M%S"))
    logger.handlers = []

    logger.setLevel(level)
    formatter = logging.Formatter('%(message)s')

    fh = logging.FileHandler(filename)
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def flatten(l):
    '''
    Flattens a list of lists
    '''
    return [item for sublist in l for item in sublist]

def flatten_sets(x, y):
    '''
    Flattens a set of sets
    '''
    return set(list(x) + list(y))

def tidy_xml(path, suffix='_tidy'):
    try:
        doc = minidom.parse(path)
        tidy_doc = doc.toprettyxml(encoding='UTF-8')
        tidy_path = path[:-4] + '{}.xml'.format(suffix)
        with io.open(tidy_path, 'wb') as outstream:
            outstream.write(tidy_doc)
    except OSError as _:
        print("fatal: Tidy XML failed. Is tidy installed? (sudo apt-get install tidy)")
        return path

    return tidy_path

# def tidy_xml2(path, suffix='_tidy'):
#     try:
#         tidy_path = path[:-4] + '{}.xml'.format(suffix)
#         with io.open(path, 'r', encoding='utf8') as instream:
#             xml_document = instream.read()
#         tidy_xml_document = tidylib.tidy_document(xml_document, {"input_xml": True})[0]
#         with io.open(tidy_path, 'w', encoding='utf8') as outstream:
#             outstream.write(tidy_xml_document)
#     except OSError as ex:
#         print("fatal: Tidy XML failed. Is tidy installed? (sudo apt-get install tidy)")
#         return path

#     return tidy_path

def compress_and_encode(path):

    compressed_data = zlib.compress(path.encode('utf8'))
    encoded = base64.b64encode(compressed_data)
    uue_filename = path + '.gz.uue'
    with io.open(uue_filename, 'wb') as outstream:
        outstream.write(encoded)

    gz_filename = path + '.gz'
    with io.open(gz_filename, 'wb') as outstream:
        outstream.write(compressed_data)