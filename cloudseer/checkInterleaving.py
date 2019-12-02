import os
import re
import random
import glob
import copy
import glob
import temporal
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
    def __init__(self, template_variable_re, gapthreshold_weak):
        self.template_variable_re = template_variable_re
        self.gapthreshold_weak = gapthreshold_weak


class Sequence:
    """ represent each task workflow sequence"""
    def __init__(self,logline, templates):
        templatelog = Templatelog(logline, templates, para)
        self.state = templatelog.getTemplate()
        self.identifierSet = set()
        self.logSequence = list()
        self.identifierSet =(self.identifierSet)|(templatelog.getVariable())
        self.logSequence.append(logline)

    def getState(self):
        return self.state

    def getIdentifierSet(self):
        return self.identifierSet

    def getlogSequence(self):
        return self.logSequence

    def updateSequence(self, logline, templates):
        templatelog = Templatelog(logline, templates, para)
        self.identifierSet =(self.identifierSet)|(templatelog.getVariable())
        self.state = templatelog.getTemplate()
        self.logSequence.append(logline)

    # continue to implement
    def checkEndSequence(self):
        if self.state == endTemplate:
            return True
        else:
            return False

class Templatelog:
    """ log message template and variable, templates is a list of template,
        variable_re is a list of Regular expression of variable """
    def __init__(self, logline, templates, para):
        templateMatched = False
        variableMatched = False
        for template in templates:
            patternT = re.compile(template)
            if patternT.match(logline):
                self.template = template
            else:
                templateMatched = True

        if not templateMatched:
            self.template = None

        self.variable = set()
        variable_re = para.template_variable_re
        for each_variable in variable_re:
            patternI = re.compile(each_variable)
            m = patternI.search(logline)
            if m :
                self.variable.add(m.group())

    def getTemplate(self):
        return self.template

    def getVariable(self):
        return self.variable

#
# def dataFromFile(fname):
#     """Function which reads from the file and yields a generator"""
#     file_iter = open(fname, 'r')
#     for line in file_iter:
#         yield line

def chooseAutomaton(templatelog, automatonGroup, sequenceList):
    """ Firstly, choose automatons have max common identifierSet with least difference;
        Secondly, check if these automaton can accept the log message being checked
    """
    #store all growing sequences that make transition happen and have most common identifiers
    seqTakenGroup = list()

    maxNumCommIdList = list()
    leastNumDiffeIdList = list()
     # find the sequences have most common identifiers
    for seq in sequenceList:
        interSet = seq.getIdentifierSet() & templatelog.getVariable()
        maxNumCommIdList.append(len(interSet))

    maxNum = max(maxNumCommIdList)
    maxNumIndexList = [index for index, elem in enumerate(maxNumCommIdList) if elem == maxNum]
    maxCommSeq = [sequenceList[index] for index in maxNumIndexList]

    if len(maxCommSeq) < 1:
        leastDiffSeq = maxCommSeq
    # multiple sequences have most common identifiers then choose which has least difference
    else:
        for seq in maxCommSeq:
            differenceSet = seq.getIdentifierSet() - templatelog.getVariable()
            leastNumDiffeIdList.append(len(differenceSet))

        leastNum = min(leastNumDiffeIdList)
        leastNumIndexList = [index for index, elem in enumerate(leastNumDiffeIdList) if elem == leastNum]
        leastDiffSeq = [maxCommSeq[index] for index in leastNumIndexList]
    # check if the sequence can accept log message
    for seq in leastDiffSeq:
        for automaton in automatonGroup:
            if templatelog.getTemplate() in automaton[seq.getState()]:
                seqTakenGroup.append(seq)

    return seqTakenGroup

def seperateInterleaving(inputFile, automatonGroups):
    """base on automaton to separate interleaving logs"""
    #store all growing sequences
    sequenceList = list()
    with open(inputFile) as lines:
        for logLine in lines:
            logTemplate = Templatelog(logLine, templates, para)
            if not logTemplate.getTemplate():
                continue
            if not sequenceList:
                newLogSeq = Sequence(logLine, templates)
                sequenceList.append(newLogSeq)
            else:
                automatonChoosen = chooseAutomaton(logTemplate, automatonGroups, sequenceList)
                # there are only one sequence
                if len(automatonChoosen) == 1 :
                    automatonChoosen[0].updateSequence(logLine, templates)
                # there are several sequence  random choose one
                elif len(automatonChoosen) > 1 :
                    sys_random = random.SystemRandom()
                    oneauto = sys_random.choice(automatonChoosen)
                    oneauto.updateSequence(logLine, templates)
                # if no sequence accept log message, still have two ways to implement
                else:
                    newLogSeq = Sequence(logLine,templates)
                    sequenceList.append(newLogSeq)

    return sequenceList


if __name__ == "__main__":

    # initialize all parameter
    para = Para(template_variable_re = ['from\s/\S+', 'blockMap\supdated:\s\S+', 'src:\s/\S+', 'dest:\s/\S+'], gapthreshold_weak = 40)
    # input logSequence
    inputFile = 'data/testkeylog.txt'
    # inputSequence = dataFromFile(inputFile)

    #to test automation construction
    path_auto = 'data/auto/'
    #store all task automatons
    automatonGroups = list()
    # get all task automatons
    for filename in glob.glob(os.path.join(path_auto, '*.txt')):
        automaton_graph = temporal.getAutomatonGraph(filename, templates, para)
        automatonGroups.append(automaton_graph)
    print(automatonGroups)
    # check logSequence
    sequenceList = seperateInterleaving(inputFile, automatonGroups)
    for seq in sequenceList:
        print(seq.getlogSequence())
