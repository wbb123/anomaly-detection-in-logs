'''
Some examples for LSH
'''
import re
from hashlib import sha1
import numpy as np
from collections import defaultdict
from datasketch.minhash import MinHash
from datasketch.weighted_minhash import WeightedMinHashGenerator
from datasketch.lsh import MinHashLSH

# set1 = set(['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
#             'estimating', 'the', 'similarity', 'between', 'datasets'])
# set2 = set(['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
#             'estimating', 'the', 'similarity', 'between', 'documents'])
# set3 = set(['minhash', 'is', 'probability', 'data', 'structure', 'for',
#             'estimating', 'the', 'similarity', 'between', 'documents'])

# v1 = np.random.uniform(1, 10, 10)
# v2 = np.random.uniform(1, 10, 10)
templates = [
'.*Adding an already existing block.*',
'.*Verification succeeded for.*',
'.*Served block.*to.*',
'.*Got exception while serving.*to.*',
'.*Receiving block.*src:.*dest:.*',
'.*Received block.*src:.*dest:.*of size.*',
'.*writeBlock.*received exception.*',
'.*PacketResponder.*for block.*Interrupted.*',
'.*Received block.*of size.*from.*',
'.*PacketResponder.*Exception.*',
'.*PacketResponder.*for block.*terminating.*',
'.*:Exception writing block.*to mirror.*',
'.*Receiving empty packet for block.*',
'.*Exception in receiveBlock for block.*',
'.*Changing block file offset of block.*from.*to.*meta file offset to.*',
'.*:Transmitted block.*to.*',
'.*:Failed to transfer.*to.*got.*',
'.*Starting thread to transfer block.*to.*',
'.*Reopen Block.*',
'.*Unexpected error trying to delete block.*BlockInfo not found in volumeMap.*',
'.*Deleting block.*file.*',
'.*BLOCK\* NameSystem.*allocateBlock:.*',
'.*BLOCK\* NameSystem.*delete:.*is added to invalidSet of.*',
'.*BLOCK\* Removing block.*from neededReplications as it does not belong to any file.*',
'.*BLOCK\* ask.*to replicate.*to.*',
'.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*',
'.*BLOCK\* NameSystem.*addStoredBlock: Redundant addStoredBlock request received for.*on.*size.*',
'.*BLOCK\* NameSystem.*addStoredBlock: addStoredBlock request received for.*on.*size.*But it does not belong to any file.*',
'.*PendingReplicationMonitor timed out block.*',
'.*BLOCK\* ask.*to delete.*'
]

class Templatelog:
    """ log message template and variable """
    def __init__(self, logline, templates):
        templateMatched = False
        for template in templates:
            patternT = re.compile(template)
            if patternT.match(logline):
                self.template = template
                templateMatched = True
        if not templateMatched:
            self.template = None
        timestamp_re = '0811[0-9][0-9]\s[0-9]+'
        pattern_timestamp = re.compile(timestamp_re)
        m = pattern_timestamp.search(logline)
        if m:
            timestamp = m.group()
            self.time = str(int(timestamp[4:6])*24*60*60 + int(timestamp[7:9])*60*60 + int(timestamp[9:11])*60 + int(timestamp[11:13]))
        else:
            self.time = None

    def getTemplate(self):
        return self.template

    def getTimestamp(self):
        return self.time

def getTemplateTimeseries(inputFile, templates):
    """store each templates' timeseries as a list """
    template_timeseries = list()
    for eachTemplate in templates:
        eachTemplate_time = list()
        with open(inputFile) as lines:
            for line in lines:
                #print(line)
                logTemplate = Templatelog(line, templates)
                if eachTemplate == logTemplate.getTemplate():
                    time = logTemplate.getTimestamp()
                    eachTemplate_time.append(time)
        template_timeseries.append(eachTemplate_time)
    return template_timeseries

def checkTimeSimilarity(templates, template_timeseries, similarity_threshold):
    """ store each substructure as a defaultdict, adjust similarity_threshold
        value to obtain different substructure"""
    m = list()
    substructure  =  defaultdict(list)
    # Create LSH index
    lsh = MinHashLSH(similarity_threshold, num_perm = 128)
    for index, each in enumerate(template_timeseries):
        each_m = MinHash(num_perm= 128)
        for d in each:
            each_m.update(d.encode('utf8'))
        m.append(each_m)
        lsh.insert(templates[index], each_m)
    for index, each_m in enumerate(m):
        result = lsh.query(each_m)
        #remove self-similarity
        result.remove(templates[index])
        substructure[templates[index]].extend(result)
    return substructure



if __name__ == "__lsh__":
        inputFile = 'data/test_singleBlock.txt'
        template_timeseries = getTemplateTimeseries(inputFile, templates)
        index_list = [index for index, item in enumerate(template_timeseries) if item == []]
        print(index_list)
        print(checkTimeSimilarity(templates,template_timeseries, 1))
