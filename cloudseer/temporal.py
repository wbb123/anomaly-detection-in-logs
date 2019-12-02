import os
import re
import copy
import glob
from collections import defaultdict
from itertools import combinations
from itertools import product
from itertools import permutations

templates={
'.*Verification succeeded for.*',
'.*Served block.*to.*',
'.*Got exception while serving.*to.*',
'.*Receiving block.*src:.*dest:.*',
'.*Received block.*src:.*dest:.*of size.*',
'.*writeBlock.*received exception.*',
'.*Received block.*of size.*from.*',
'.*PacketResponder.*for block.*terminating.*',
'.*Receiving empty packet for block.*',
'.*Exception in receiveBlock for block.*',
'.*Changing block file offset of block.*from.*to.*meta file offset to.*',
'.*:Transmitted block.*to.*',
'.*Starting thread to transfer block.*to.*',
'.*Unexpected error trying to delete block.*BlockInfo not found in volumeMap.*',
'.*Deleting block.*file.*',
'.*BLOCK\* NameSystem.*allocateBlock:.*',
'.*BLOCK\* NameSystem.*delete:.*is added to invalidSet of.*',
'.*BLOCK\* ask.*to replicate.*to.*',
'.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*',
'.*BLOCK\* NameSystem.*addStoredBlock: Redundant addStoredBlock request received for.*on.*size.*',
'.*BLOCK\* NameSystem.*addStoredBlock: addStoredBlock request received for.*on.*size.*But it does not belong to any file.*',
}

class Para:
    def __init__(self, template_variable_re , gapthreshold_weak):
        self.template_variable_re = template_variable_re
        self.gapthreshold_weak = gapthreshold_weak

def check_strongDependency(inputFile, pair):
    """check strong dependency of template pair(a, b), b next to a"""
    firstMatched = False
    strongMatched = False
    pattern0 = re.compile(pair[0])
    pattern1 = re.compile(pair[1])
    with open(inputFile) as lines:
        for line in lines:
            #print(line)
            if firstMatched:
                if pattern0.match(line):
                    firstMatched = True
                    continue

                elif not pattern1.match(line):
                    strongMatched = False
                    break
                else:
                    strongMatched = True
                    firstMatched = False
            else:
                if pattern0.match(line):
                    firstMatched = True
        #print(next(inputSequence))
    return strongMatched

def check_weakDependency(inputFile, pair, para):
    """" check weak dependency of template pair(a, b), a always occurs before b """
    weakMatched = False
    firstMatched = False
    gapthreshold = para.gapthreshold_weak
    gapsize = 0
    pattern0 = re.compile(pair[0])
    pattern1 = re.compile(pair[1])
    with open(inputFile) as lines:
        for line in lines:
            if firstMatched:
                if pattern1.match(line):
                    if gapsize <= gapthreshold:
                        weakMatched = True
                        firstMatched = False
                    else:
                        break
                elif pattern0.match(line):
                    gapsize = 0

                else:
                    gapsize = gapsize + 1
            else:
                if pattern1.match(line):
                    break;
                elif pattern0.match(line):
                    gapsize = 0
                    firstMatched = True
    return weakMatched

def getDependencyPairs(inputFile, key_templates, para):
    """ get all pairs and their dependency"""
    pairs = list(permutations(key_templates,2))
    # print(len(pairs))
    temporal_pairs = []
    strong_temporal_pairs = []
    weak_temporal_pairs = []
    for pair in pairs:
        if check_strongDependency(inputFile, pair):
            strong_temporal_pairs.append(pair)
        else:
            if check_weakDependency(inputFile, pair, para):
                weak_temporal_pairs.append(pair)
    # print(strong_temporal_pairs)
    # print(len(strong_temporal_pairs))
    # print(weak_temporal_pairs)
    # print(len(weak_temporal_pairs))
    strong_temporal_pairs.extend(weak_temporal_pairs)
    return strong_temporal_pairs


def getAutomatonGraph(inputFile, key_templates, para):
    temporal_pairs = getDependencyPairs(inputFile, key_templates, para)
    tmp_pair = copy.copy(temporal_pairs)
    # redundant transition removal
    for pair1 in temporal_pairs:
        for pair2 in temporal_pairs:
            if pair1[1] == pair2[0]:
                if (pair1[0],pair2[1]) in temporal_pairs:
                    temporal_pairs.remove((pair1[0], pair2[1]))
    automaton_graph = defaultdict(list)
    for pair in temporal_pairs:
        automaton_graph[pair[0]].append(pair[1])
    return automaton_graph


if __name__ == "__temporal__":
    #inputFile = 'data/test_temporal.txt'
    para = Para(['blk_-?[0-9]+', 'from\s/\S+', 'blockMap\supdated:\s\S+', 'src:\s/\S+', 'dest:\s/\S+'], 40)
    path = 'data/auto/'
    for filename in glob.glob(os.path.join(path, '*.txt')):
        automaton_graph = getAutomatonGraph(filename, templates)
