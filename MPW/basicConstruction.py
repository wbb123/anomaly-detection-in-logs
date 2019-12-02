import os
import re
import random
import glob
import copy
import glob
import util
import dependency
import graph
from collections import defaultdict
from itertools import combinations
from itertools import product
from itertools import permutations

templates={
'.*Received block.*src:.*dest:.*of size.*',
'.*Received block.*of size.*from.*',
'.*PacketResponder.*for block.*terminating.*',
'.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*',
}

eventTraces = [['.*Received block.*src:.*dest:.*of size.*',
                '.*PacketResponder.*for block.*terminating.*',
                '.*Received block.*src:.*dest:.*of size.*',
                '.*Received block.*of size.*from.*',
                '.*PacketResponder.*for block.*terminating.*',
                '.*PacketResponder.*for block.*terminating.*',
                '.*Received block.*of size.*from.*',
                '.*Received block.*of size.*from.*',
                '.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*',
                '.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*'],
                ['.*Received block.*src:.*dest:.*of size.*',
                 '.*PacketResponder.*for block.*terminating.*',
                 '.*Received block.*src:.*dest:.*of size.*',
                 '.*PacketResponder.*for block.*terminating.*',
                 '.*Received block.*of size.*from.*',
                 '.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*',
                 '.*Received block.*of size.*from.*',
                 '.*PacketResponder.*for block.*terminating.*',
                 '.*Received block.*of size.*from.*',
                 '.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*']]

# class Para:
#     def __init__(self, template_variable_re , gapthreshold_weak):
#         self.template_variable_re = template_variable_re
#         self.gapthreshold_weak = gapthreshold_weak

class BasicWorkflow:
    """   """
    def __init__(self, eventTraces, dependencies, templates):
        self.dependencies = dependencies
        self.templates = templates
        # self.para = para
        self.basic_workFlow = defaultdict(list)
        # self.workFlow = Graph()
        self.wfsuccessor = defaultdict(list)
        self.wfpredecessor = defaultdict(list)


    def findRoot(self, key_templates):
        """ find template that has no predecessor """
        root = list()
        for eachTemplate in key_templates:
            if not self.dependencies.predecessor[eachTemplate]:
                root.append(eachTemplate)
        return root

    # def addSimpleStateType(self, eventTraces):
    #     """ default state is switch, split/merge
    #         based on occurances of event type  """
    #     root = self.findRoot(tmp_templates)
    #     index = 0
    #     base = 0
    #     tmp_templates = key_templates
    #     self.workFlow.add_vertex(s[base])
    #     while(tmp_workFlow):
    #         for eachTemplate in tmp_workFlow:
    #             if not self.wfpredecessor[eachTemplate] and self.wfsuccessor[eachTemplate]:
    #                 self.workFlow.add_vertex(s[index + 1])
    #                 self.workFlow.add_edge(s[base], s[index + 1], eachTemplate)
    #                 index = index + 1
    #             for v in tmp
    #             if eachTemplate in self.wfsuccessor[v.get_transition(w)]
    #
    #         root = self.findRoot(tmp_templates)
    #
    #         for eachRoot in root:
    #
    #             index = index + 1
    #         for
    #     workFlow[s[0]].append(root)
    #     for eachEventType in root:
    #         workFlow[eachEventType].append(s[index])
    #         if len(basic_workFlow[eachEventType]) == 1 and self.getOccurance[eachEventType] == self.getOccurance[basic_workFlow[eachEventType]]:
    #             index = index + 1
    #             workFlow[eachEventType].append(s[index])
    #             workFlow[s[index]].append(basic_workFlow[eachEventType])
    #             stateType[s[index]] = 'switch'
    #         if len(basic_workFlow[eachEventType]) > 1:
    #             for successorEventType in basic_workFlow[eachEventType]:
    #                 count_successor = list()
    #                 count_successor.append(self.getOccurance[successorEventType])
    #             if len(set(count_successor)) == 1:
    #                 index = index + 1
    #                 workFlow[eachEventType].append(s[index])
    #                 stateType[s[index]] = 'split'
    #                 for successorEventType in basic_workFlow[eachEventType]:
    #                     workFlow[s[index]].append(successorEventType)
    #
    #     for eachEventType in tmp_workFlow:
    #         #if eventType only has one successor
    #         if len(basic_workFlow[eachEventType]) == 1:
    #
    #         elif len(basic_workFlow[eachEventType]) > 1:
    #
    # def getOccurance(self, eventTraces):
    #     """calculate eventType's occurance in eachTrace """


    def wfConstruction(self, eventTraces, dependencies, key_templates):
        """ basicWorkflow: only one transition between two templates, like a->b->c->d"""
        Q = util.Queue()
        tmp_templates = key_templates
        tmp_workFlow = list()
        wfsuccessor = self.dependencies.successor
        wfpredecessor = self.dependencies.predecessor
        workFlow = defaultdict(list)
        index = 0

        while(tmp_templates):
            root = self.findRoot(tmp_templates)
            tmp_workFlow.extend(root)
            for eachR in root:
                Q.push(eachR)
            #remove rebundant dependency
            while not Q.isEmpty():
                checkT = Q.pop()
                if checkT not in tmp_templates:
                    continue
                for eachValue in wfsuccessor[checkT]:
                    if set(wfpredecessor[eachValue]) & set(tmp_workFlow):
                        flag = False
                        for eachPredecessor in wfpredecessor[eachValue]:
                            # if eachSuccessor and checkT has no dependency
                            if checkT not in wfpredecessor[eachPredecessor] and eachPredecessor not in wfpredecessor[checkT]:
                                #wfsuccessor[checkT].append(eachValue)
                                pass
                            elif checkT in wfsuccessor[eachPredecessor]:
                                if eachValue in wfsuccessor[eachPredecessor]:
                                    wfsuccessor[eachPredecessor].remove(eachValue)
                                    wfpredecessor[eachValue].remove(eachPredecessor)
                                #wfsuccessor[checkT].append(eachValue)
                            else:
                                flag = True
                        if(flag):
                            pass
                            #if eachValue in wfsuccessor[checkT]:
                            #    wfsuccessor[checkT].remove(eachValue)
                    else:
                        pass
                        #wfsuccessor[checkT].append(eachValue)
                    if eachValue in tmp_templates:
                        Q.push(eachValue)
                tmp_templates.remove(checkT)
        # transform successor to predecessor
        # for eachTemplate in tmp_workFlow:
        #     if wfsuccessor[eachTemplate]
        #     for eachValue in wfsuccessor[eachTemplate]:
        #         wfpredeccessor[eachValue].append[eachTemplate]
        # self.basic_workFlow = wfsuccessor
        self.wfsuccessor = wfsuccessor
        self.wfpredecessor = wfpredecessor
        return wfsuccessor

if __name__ == "__main__":
    #inputFile = 'data/test_temporal.txt'
    # para = Para(['blk_-?[0-9]+', 'from\s/\S+', 'blockMap\supdated:\s\S+', 'src:\s/\S+', 'dest:\s/\S+'], 40)
    # path = 'data/auto/'
    # for filename in glob.glob(os.path.join(path, '*.txt')):
    #     automaton_graph = getAutomatonGraph(filename, templates)
    dependency_example = dependency.Dependency(templates, eventTraces)
    dependency_example.getDependencyPairs()
    basicWorkflow = BasicWorkflow(eventTraces, dependency_example, templates)
    print(basicWorkflow.wfConstruction(eventTraces, dependency_example, templates))
