from hadoop.fs.hadoopfs import Hdfs
import datetime
from subprocess import *
import datetime
from django.template.defaultfilters import stringformat, filesizeformat
from filebrowser.lib.rwx import filetype, rwx
from hadoop.fs.hadoopfs import Hdfs

def fprint(txt):
    """ Print some text in a debug file """
    f = open('/tmp/cgs_debug.txt', 'a')
    f.write(str(txt)+"\n")
    f.close()
    return True

def directory_current_user(request):
    """ Return the current user directory """
    path = request.user.get_home_directory()
    try:
        if not request.fs.isdir(path):
            path = '/'
    except Exception:
        pass

    return path

def list_directory_content(request, first_path, extension, save_stats=False):
    """ Load the content of a directory and its subdirectories, according to the given extension. Find only files. """

    # Recursive functions are the root of all evil.
    paths = []
    paths.append(first_path)
    files = []
    while len(paths) > 0:
        current_path = paths.pop()
        stats = request.fs.listdir_stats(current_path)
        data = [_massage_stats(request, stat) for stat in stats]
        for f in data:
            if f['name'].endswith(extension) and f['type'] == 'file':
                destination_file = f['path'].replace(first_path+"/","",1)
                if save_stats == True:
                    files.append(f)
                else:
                    files.append(destination_file)
            elif f['type'] == 'dir' and f['name'] != '.Trash' and f['name'] != '.' and f['name'] != '..':
                paths.append(f['path'])

    return files

def _massage_stats(request, stats):
    """
    Massage a stats record as returned by the filesystem implementation
    into the format that the views would like it in.
    """
    path = stats['path']
    normalized = Hdfs.normpath(path)
    return {
    'path': normalized,
    'name': stats['name'],
    'stats': stats.to_json_dict(),
    'mtime': datetime.datetime.fromtimestamp(stats['mtime']).strftime('%B %d, %Y %I:%M %p'),
    'humansize': filesizeformat(stats['size']),
    'type': filetype(stats['mode']),
    'rwx': rwx(stats['mode'], stats['aclBit']),
    'mode': stringformat(stats['mode'], "o")
    #'url': make_absolute(request, "view", dict(path=urlquote(normalized))),
    #'is_sentry_managed': request.fs.is_sentry_managed(path)
    }


def sample_insert_vcfinfo(request, filename, total_length):
    """ Return the different samples found in the given vcf file """

    offset = 0
    length = min(1024*1024*5,total_length)
    path = directory_current_user(request)+"/"+filename

    # We read the text and analyze it
    while offset < total_length:
        text = request.fs.read(path, offset, length)
        lines = text.split("\n")
        samples = []
        for line in lines:
            info = line.split("\t")
            if info[0] == '#CHROM':

                # We add the samples information
                for i in xrange(9, len(info)):
                    if info[i]:
                        samples.append(info[i].strip())

                # We can stop it here
                break

        if len(samples) > 0:
            break
        else:
            offset = offset+length

    # We return the different samples in the file
    return samples

def sample_insert_questions(request):
    """ Return the questions asked to insert the data """
    questions = {
        "sample_registration":{
            "main_title": "Sample",
            "original_sample_id": {"question": "Original sample id", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "patient_id": {"question": "Patient id", "field": "text", "regex": "a-zA-Z0-9_-", "mandatory": True},
            "biobank_id": {"question": "Biobank id", "field": "text", "regex": "a-zA-Z0-9_-"},
            "prenatal_id": {"question": "Prenatal id", "field": "text", "regex": "a-zA-Z0-9_-"},
            "sample_collection_date": {"question": "Date of collection", "field": "date", "regex": "date"},
            "collection_status": {"question": "Collection status", "field": "select", "fields":("collected","not collected")},
            "sample_type": {"question": "Sample type", "field": "select", "fields":("serum","something else")},
            "biological_contamination": {"question": "Biological contamination", "field": "select", "fields":("no","yes")},
            "sample_storage_condition": {"question": "Storage condition", "field": "select", "fields":("0C","1C","2C","3C","4C")},
        },
    }

    # A dict in python is not ordered so we need a list
    q = ("main_title", "original_sample_id", "patient_id", "biobank_id", "prenatal_id", "sample_collection_date", "collection_status", "sample_type",
        "biological_contamination", "sample_storage_condition")

    # We also load the files
    files = list_directory_content(request, directory_current_user(request), ".vcf", False)

    return questions, q, files