# -*- coding: UTF-8 -*-
# _author_ = "luoyanpeng"

import numpy as np
import math


def getRandomContentServer(contentServerNumDic, cacheNum):
    contentServers = list(contentServerNumDic.keys())
    if len(contentServers) == 0:
        return np.random.randint(0, cacheNum)
    else:
        contentServer = contentServers[np.random.randint(0, len(contentServers))]
        contentServerNumDic[contentServer] -= 1
        if contentServerNumDic[contentServer] == 0:
            del contentServerNumDic[contentServer]
        return contentServer


class SNMMapToServerYoutube():
    def __init__(self, requestsSeq: list, contentServerNum):
        self.k = 0  # 内容数量
        self.requestSeq = requestsSeq
        self.contentServerNum = contentServerNum
        self.requestNum = len(requestsSeq)
        self.xm = np.random.rand(self.k)
        self.yl = np.random.rand(self.contentServerNum)
        self.contentServerContentNum = {}  # contentServer:{content:num}
        self.contentContentServerNum = {}  # content:{contentServer:num}
        self.contentNum = {}



    def computeContentNum(self):
        for oneReq in self.requestSeq:
            content = oneReq.content
            if content not in self.contentNum.keys():
                self.contentNum[content] = 1
            else:
                self.contentNum[content] += 1
        self.k = len(self.contentNum.keys())
        self.xm = np.random.rand(self.k)
        self.yl = np.random.rand(self.contentServerNum)

    def kernerFunction(self, x, y):
        x = abs(x - y)
        return 5 * ((1 - 2 * x) ** 4)

    def getUml(self, m, l):
        x = self.xm[m]
        y = self.yl[l]
        k = self.kernerFunction(x, y)
        sum = 0
        for oney in self.yl:
            sum += self.kernerFunction(x, oney)
        if sum != 0:
            return k / sum
        else:
            return 0

    def getContentServerContentNum(self):
        for oneContent in range(self.k):
            self.contentContentServerNum[oneContent] = {}  # 每个内容在各个cache中请求的数量
        for oneContent in range(self.k):
            contentDic = self.contentContentServerNum[oneContent]
            for oneContentServer in range(self.contentServerNum):
                uml = self.getUml(oneContent, oneContentServer)

                contentNum = int(round(self.contentNum[oneContent] * uml))
                if contentNum == 0:
                    continue
                contentDic[oneContentServer] = contentNum
            self.contentContentServerNum[oneContent] = contentDic

    def computeRequestsReq(self, cacheNum):
        self.computeContentNum()
        self.getContentServerContentNum()
        for oneReq in self.requestSeq:
            content = int(oneReq.content)
            contentServerNumDic = self.contentContentServerNum[content]
            oneReq.contentServer = getRandomContentServer(contentServerNumDic, cacheNum)

