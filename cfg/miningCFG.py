import os
import re
import copy
import lsh
import util
from collections import deque
from collections import defaultdict
from itertools import combinations
from itertools import product
from itertools import permutations

#templates
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

class Para:
    def __init__(self, template_timestamp, non_independent_probability, lagthreshold, gapthreshold):
        self.template_timestamp = template_timestamp
        self.non_independent_probability = non_independent_probability
        self.lagthreshold = lagthreshold
        self.gapthreshold = gapthreshold

class Templatelog:
    """ log message template and variable """
    def __init__(self, logline, templates, para):
        templateMatched = False
        for template in templates:
            patternT = re.compile(template)
            if patternT.match(logline):
                self.template = template
                templateMatched = True
        if not templateMatched:
            self.template = None

        timestamp_re = para.template_timestamp
        pattern_timestamp = re.compile(timestamp_re)
        m = pattern_timestamp.search(logline)
        if m:
            timestamp = m.group()
            self.time = int(timestamp[4:6])*24*60*60 + int(timestamp[7:9])*60*60 + int(timestamp[9:11])*60 + int(timestamp[11:13])
        else:
            self.time = 0

    def getTemplate(self):
        return self.template

    def getTimestamp(self):
        return self.time



def getNNSgroup(inputFile, templates, para):
    """ get each templates's NNS group (store in a defaultdict)
        Firstly, look ahead several templates(set as gapthreshold) based on
        reference templates and get basic NNS group
        Secondly, ONLY keep neighbours templates that meet the requirement of
        three structure: linear, fork, merge
     """
    #total number of loglines
    total_count = 0
    #store each templates' NNS group
    NNSgroup = defaultdict(list)

    template_count = list()
    #store numbers of each templates occurances
    oneTemplate_count = list()
    #store conditional probability of pair(A->B)
    template_prob_count = list()
    #window size
    gapthreshold = para.gapthreshold

    for index1, eachTemplate in enumerate(templates):
        #check the first time that reference template occur
        firstMatched = False
        #the gap between neighbor template and reference template
        gapsize = 0
        #store number of neighbor templates' occurances  after each reference templates occur
        eachTemplate_count = [0] * len(templates)
        with open(inputFile) as lines:
            for line in lines:
                logTemplate = Templatelog(line, templates, para)
                if not logTemplate.getTemplate() == None:
                    if firstMatched:
                        for index2, otherTemplate in enumerate(templates):
                            if eachTemplate == logTemplate.getTemplate():
                                eachTemplate_count[index1] = eachTemplate_count[index1] + 1
                                gapsize = 0
                                break;
                            else:
                                if otherTemplate == logTemplate.getTemplate():
                                    gapsize = gapsize + 1
                                    if gapsize <= gapthreshold:
                                        eachTemplate_count[index2] = eachTemplate_count[index2] + 1
                                    break
                    else:
                        if eachTemplate == logTemplate.getTemplate():
                            eachTemplate_count[index1] = eachTemplate_count[index1] + 1
                            gapsize = 0
                            firstMatched = True
        template_count.append(eachTemplate_count)
        total_count = total_count + eachTemplate_count[index1]
        oneTemplate_count.append(eachTemplate_count[index1])
        eachTemplate_prob = [item/(eachTemplate_count[index1]+1) for item in eachTemplate_count]
        template_prob_count.append(eachTemplate_prob)

    # LSH check timeseries similarity
    template_timeseries = lsh.getTemplateTimeseries(inputFile, templates)
    linear = lsh.checkTimeSimilarity(templates,template_timeseries, 1)            # set threshold to 1 => check linear relation
    fork = lsh.checkTimeSimilarity(templates,template_timeseries, 0.5)            # set threshold to 0.5 => check fork relation
    merge = lsh.checkTimeSimilarity(templates,template_timeseries, 0.1)            # set threshold to 0.1 => check merge relation
    for index1, eachTemplate_prob in enumerate(template_prob_count):
        for index2, otherTemplate_prob in enumerate(eachTemplate_prob):
            if not index1 == index2:
                # linear and fork can be determined by lsh, otherTemplate_prob > 0 can be used to avoid both occurances are 0
                if templates[index2] in fork[templates[index1]] and otherTemplate_prob > 0:
                    NNSgroup[templates[index1]].append(templates[index2])
                else:
                    # check merge structure which needs to combine bayesian condition probability and timeseries similarity
                    if otherTemplate_prob/(oneTemplate_count[index1]/total_count + 1) > para.non_independent_probability \
                    and (templates[index2] in merge[templates[index1]]):
                        # print(templates[index1])
                        # print(templates[index2])
                        NNSgroup[templates[index1]].append(templates[index2])

    return NNSgroup, linear


def constructCFG(inputFile, NNSgroup, templates, linear, para):
    """ """
    CFG = defaultdict(list)
    #store every processed logline
    lastTemplateSeenStack = util.Stack()
    #store every referenceTemplate
    referenceTqueue = util.Queue()
    referenceT = None
    with open(inputFile) as lines:
        for line in lines:
            logTemplate = Templatelog(line, templates, para)
            if not logTemplate.getTemplate() == None:
                lastTemplateSeenStack.push(logTemplate)
                if referenceT:
                    tmplastTemplateSeenStack = copy.deepcopy(lastTemplateSeenStack)
                    lastTemplateSeen = tmplastTemplateSeenStack.pop()
                    #
                    while(int(logTemplate.getTimestamp() - int(lastTemplateSeen.getTimestamp())) <= para.lagthreshold):
                        if lastTemplateSeen.getTemplate() in NNSgroup[logTemplate.getTemplate()]:
                            if lastTemplateSeen.getTemplate() == referenceT.getTemplate():
                                if logTemplate.getTemplate() not in CFG[referenceT.getTemplate()]:
                                    CFG[referenceT.getTemplate()].append(logTemplate.getTemplate())
                                    # print(logTemplate.getTemplate())
                                referenceTqueue.push(logTemplate)
                                if logTemplate.getTemplate() in linear[referenceT.getTemplate()]:
                                    #print( referenceT.getTemplate() + logTemplate.getTemplate() + "$$$$$$$$$$$")
                                    referenceT = referenceTqueue.pop()
                                    # print(referenceT.getTemplate()+"##############################################")

                        if not tmplastTemplateSeenStack.isEmpty():
                            lastTemplateSeen = tmplastTemplateSeenStack.pop()
                        else:
                            break
                else:
                    #first logline as fisrt referenceT
                    referenceTqueue.push(logTemplate)
                    referenceT = referenceTqueue.pop()
                    lastTemplateSeenStack.push(logTemplate)
                    # print(referenceT.getTemplate()+"##############################################")
                #push processed logline into stack

                # if processing logline is impossible to be referenceT's child because of timeout then change referenceT
                if (int(logTemplate.getTimestamp()) - int(referenceT.getTimestamp())) > para.lagthreshold:
                    referenceT = referenceTqueue.pop()
                    # print(referenceT.getTemplate()+"##############################################")

    return CFG


if __name__ == "__main__":
    inputFile = 'data/testkeylog_cfg.txt'
    para = Para(template_timestamp = '0811[0-9][0-9]/s[0-9]+', non_independent_probability = 1.5, \
    lagthreshold = 3, gapthreshold = 10)
    # inputtemplateFile = 'data/templates.txt'
    # with open(inputtemplateFile) as lines:
    #     templates = list()
    #     for line in lines:
    #         templates.append(line)
    NNSgroup, linear = getNNSgroup(inputFile, templates, para)
    print(NNSgroup)
    CFG = constructCFG(inputFile, NNSgroup, templates, linear, para)
    print(CFG)
    #print(CFG)
