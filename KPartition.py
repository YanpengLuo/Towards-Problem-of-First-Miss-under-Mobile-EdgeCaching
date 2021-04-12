# -*- coding: UTF-8 -*-
# _author_ = "luoyanpeng"
# 此类的输入是各个字典 contentServer : requests

import pymetis
import numpy as np


def computeSimilarity( requests1t, requests2t):  # 计算两个集合的相似度
    same = 0
    diff = 0
    requests1 = requests1t.copy()
    requests2 = requests2t.copy()
    for oneContent in requests1:
        if oneContent in requests2:
            same += 1
            requests2.remove(oneContent)
        else:
            diff += 1
    return same / (same + len(requests2) + diff)

class KPartition:
    def __init__(self, contentServerReq):
        self.contentServerReq = contentServerReq
        self.partitionContentServer = {}  # partitionNo : [content server ips]
        self.serverSimilarity = np.zeros([len(self.contentServerReq.keys()), len(self.contentServerReq.keys())])
        self.contentServers = list(self.contentServerReq.keys())
        self.contentServerNum = len(self.contentServerReq.keys())

    def computeSimilarity(self, requests1t, requests2t):  # 计算两个集合的相似度
        same = 0
        diff = 0
        requests1 = requests1t.copy()
        requests2 = requests2t.copy()
        for oneContent in requests1:
            if oneContent in requests2:
                same += 1
                requests2.remove(oneContent)
            else:
                diff += 1
        return same / (same + len(requests2) + diff)

    def constructGraph(self):  # 计算所有集合两两之间的相似度
        n = len(self.contentServers)
        for i in range(n):
            for j in range(i, n):
                if (i == j):
                    self.serverSimilarity[i][i] = 1
                else:
                    similarity = self.computeSimilarity(self.contentServerReq[self.contentServers[i]], self.contentServerReq[self.contentServers[j]])
                    self.serverSimilarity[i][j] = similarity
                    self.serverSimilarity[j][i] = similarity

    def constructAdjncy(self):
        adj = []
        for oneNode in range(self.contentServerNum):
            for oneAdj in range(self.contentServerNum):
                if oneAdj != oneNode:
                    adj.append(oneAdj)
        return adj

    def constrctXadj(self):
        xadj = []
        edges = self.contentServerNum * (self.contentServerNum - 1)
        edges += 1
        for oneEnd in range(0, edges, self.contentServerNum - 1):
            xadj.append(oneEnd)
        return xadj

    def constrctEweights(self):
        w = []
        self.constructGraph()
        for i in range(self.contentServerNum):
            for j in range(self.contentServerNum):
                if i != j:
                    w.append(int(self.serverSimilarity[i][j]*100))
        return w

    def getKPartitions(self, k):
        adj = self.constructAdjncy()

        xadj = self.constrctXadj()

        w = self.constrctEweights()
        (edgecuts, parts) = pymetis.part_graph(nparts=k, adjncy=adj, xadj=xadj, eweights=w)
        return parts


# def stoerWagner(self, graph: list):  # 将一个图分为两个部分
#     partition1 = []
#     partition2 = []
#     return partition1, partition2
#
# def getKPartitions(self, k):
#     self.constructGraph()

#
# kpa = KPartition({0: [1, 2, 3, 4, 5], 1: [2, 3, 4, 5, 3], 2: [1, 2, 3, 4, 6], 3: [5, 2, 13, 5]})
# # adj = kpa.constructAdjncy()
# # xadj = kpa.constrctXadj()
# # w = kpa.constrctEweights()
# parts = kpa.getKPartitions(2)
# print(parts)
# # print(adj)
# # print(xadj)
