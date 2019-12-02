import os
import re
import copy
import glob
from collections import defaultdict
from itertools import combinations
from itertools import product
from itertools import permutations

templates={
'.*Received block.*src:.*dest:.*of size.*',
'.*Received block.*of size.*from.*',
'.*PacketResponder.*for block.*terminating.*',
'.*BLOCK\* NameSystem.*addStoredBlock: blockMap updated:.*is added to.*size.*'
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
#     def __init__(self, templates):
#         self.templates = templates


class Dependency:
    """ """
    def __init__(self, templates, eventTraces):
        print("##########")
        self.eventTraces = eventTraces
        self.templates = templates
        self.predecessor = defaultdict(list)
        self.successor = defaultdict(list)

    def checkSF(self, pair):
        """ SF(a -> b) means every a must causes b's appearance.
             Only if the confidence of SF(a -> b) is 1,
             SF(a -> b) is special forward relation.
          """

        a, b = pair
        pattern_a = re.compile(a)
        pattern_b = re.compile(b)
        count_alla = 0
        count_sfa = 0
        #check every trace in the trace set
        for eachTrace in self.eventTraces:
            firstMatched = False
            secondMatched = False

            for eachlog in eachTrace:
                #count the total number of a
                if pattern_a.match(eachlog):
                    firstMatched = True
                    secondMatched = False
                    count_alla = count_alla + 1
                #count number of a that meet the requirement of SF(a -> b)
                elif pattern_b.match(eachlog) and secondMatched == False and firstMatched == True:
                    count_sfa = count_sfa + 1
                    secondMatched = True

        if count_alla == 0:
            return False
        else:
            if count_sfa/count_alla == 1:
                return True
            else:
                return False



    def checkSB(self, pair):
        """" SB(a -> b) means before every b there must exist a.
             Only if the confidence of SB(a -> b) is 1,
             SB(a -> b) is special backward relation.
          """

        a, b = pair
        pattern_a = re.compile(a)
        pattern_b = re.compile(b)
        count_allb = 0
        count_sbb = 0
        for eachTrace in self.eventTraces:
            firstMatched = False
            secondMatched = False
            # in the reverse sequence and the logic is the same as the
            for eachlog in reversed(eachTrace):
                #count all b
                if pattern_b.match(eachlog):
                    count_allb = count_allb + 1
                    firstMatched = True
                    secondMatched = False

                #count number of b that meet the requirement of SB(a -> b)
                elif pattern_a.match(eachlog) and secondMatched == False and firstMatched == True:
                    count_sbb = count_sbb + 1
                    firstMatched = True
                    secondMatched = True
        if count_allb == 0:
            return False
        else:
            if count_sbb/count_allb == 1:
                return True
            else:
                return False


    def checkF(self, pair):
        """ F(a -> b) means one or more occurance of a will cause the appearance of b.
            Only if the confidence of F(a -> b) is 1, F(a -> b) is special backward relation.
        """

        a, b = pair
        pattern_a = re.compile(a)
        pattern_b = re.compile(b)
        #count number of traces that have a
        count_alla = 0
        #count number of traces that after last a that contain b
        count_fa = 0

        for eachTrace in self.eventTraces:
            firstMatched = False
            secondMatched = False
            for eachlog in reversed(eachTrace):
                if pattern_a.match(eachlog):
                    count_alla = count_alla + 1
                    if secondMatched == True:
                        count_fa = count_fa + 1
                    break
                elif pattern_b.match(eachlog):
                    secondMatched =  True

        if count_alla == 0:
            return False
        else:
            if count_fa/count_alla == 1:
                return True
            else:
                return False


    def checkB(self, pair):
        """ B(a -> b) means before b there exists a.
            Only if the confidence of B(a -> b) is 1, B(a -> b) is backward relation.
        """
        a, b = pair
        pattern_a = re.compile(a)
        pattern_b = re.compile(b)
        #count number of traces that have a
        count_allb = 0
        #count number of traces that after last a that contain b
        count_bb = 0
        for eachTrace in self.eventTraces:
            firstMatched = False
            secondMatched = False
            for eachlog in eachTrace:
                if pattern_b.match(eachlog):
                    count_allb = count_allb + 1
                    if firstMatched == True:
                        count_bb = count_bb + 1
                    break
                elif pattern_a.match(eachlog):
                    firstMatched = True

        if count_allb == 0:
            return False
        else:
            if count_bb/count_allb == 1:
                return True
            else:
                return False

    def getDependencyPairs(self):
        """ get all pairs and their dependency
            F(a -> b), B(a -> b) a is predecessor of b and b is successor of a
            avoid bidirectional dependencies """

        pairs = list(permutations(self.templates, 2))
        SF = list()
        forward = list()
        # print(len(pairs))
        for pair in pairs:
            a, b= pair
            # print(pair)
            # print("111111111111111111111")
            if self.checkSF(pair):
                # print(pair)
                # print("checkSF***********************")
                SF.append(pair)
                forward.append(pair)
                if a not in self.predecessor[b]:
                    self.predecessor[b].append(a)
                    self.successor[a].append(b)
            elif self.checkF(pair):
                # print(pair)
                # print("checkF***********************")
                forward.append(pair)
                if a not in self.predecessor[b]:
                    self.predecessor[b].append(a)
                    self.successor[a].append(b)
            if self.checkSB(pair):
                # print(pair)
                # print("checkSB***********************")
                if a not in self.predecessor[b]:
                    self.predecessor[b].append(a)
                    self.successor[a].append(b)
            elif self.checkB(pair):
                # print(pair)
                # print("checkB***********************")
                if a not in self.predecessor[b]:
                    self.predecessor[b].append(a)
                    self.successor[a].append(b)
        # for bidirectional pairs (a -> b)(b -> a), transform b into b_ in forward relation F(a -> b)
        # in case of endless loop in constructing basicworkflow
        for pair in pairs:
            a, b = pair
            if b in self.successor[a] and a in self.successor[b]:
                if pair in forward:
                    self.successor[a].remove(b)
                    b_ = b + "_"
                    self.successor[a].append(b_)
                else:
                    self.successor[b].remove(a)
                    a_ = a + "_"
                    self.successor[a].append(a_)

        return self.successor


if __name__ == "__dependency__":
    #inputFile = 'data/test_temporal.txt'
    # para = Para(['blk_-?[0-9]+', 'from\s/\S+', 'blockMap\supdated:\s\S+', 'src:\s/\S+', 'dest:\s/\S+'], 40)
    # path = 'data/auto/'
    # for filename in glob.glob(os.path.join(path, '*.txt')):
    #     automaton_graph = getAutomatonGraph(filename, templates)
    dependency_example = Dependency(templates, eventTraces)
    print(dependency_example.getDependencyPairs())
    print(SF)
    print(forward)
