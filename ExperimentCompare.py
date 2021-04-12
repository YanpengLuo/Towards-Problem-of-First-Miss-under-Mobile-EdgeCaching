# -*- coding: UTF-8 -*-
# _author_ = "luoyanpeng"


import FIFOCacher
import LfuCacher
import LruCacher
import KPartition
from matplotlib import pyplot as plt
import numpy as np
import datetime
import SNMGenerator
import GetYoutubeReq

print("Exp7 begin")
timebeginG = datetime.datetime.now()

cacheNum = 100

# getYoutubeReq = GetYoutubeReq.GetYoutubeReq()

# requests = getYoutubeReq.getFormatRequest(cacheNum)  # youtube seq data
# requests = SNMGenerator.getRequest(cacheNum)  # youtube based
requests = SNMGenerator.getSNMRequest(cacheNum)  # pre SNM

contentServerCaches = []
for i in range(cacheNum):
    contentServerCaches.append(LruCacher.LRUCache())


cacheSize = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000]
# cacheSize = [10,20,30,40,50,60,70,80,90,100]

"""计算单播下的命中率"""

print("in unicast")
# unicastHits = []
# for oneCacheSize in cacheSize:
#     for i in range(cacheNum):
#         contentServerCaches[i].setsize(oneCacheSize)
#     unicastHit = 0
#     for oneRequest in requests:
#         content = oneRequest.content
#         contentServer = oneRequest.contentServer
#
#         if contentServerCaches[contentServer].get(content):
#             unicastHit += 1
#         else:
#             contentServerCaches[contentServer].set(content, 1)
#     unicastHits.append(unicastHit / len(requests))
"""计算单播下的命中率end"""


"""计算广播下的命中率"""
print("in broadcast")
broadcastHits = []
for oneCacheSize in cacheSize:
    print(oneCacheSize)
    for i in range(cacheNum):
        contentServerCaches[i].setsize(oneCacheSize)

    broadcastHit = 0

    for oneRequest in requests:
        content = oneRequest.content
        contentServerIp = oneRequest.contentServer

        if contentServerCaches[contentServerIp].get(content):
            broadcastHit += 1
        else:

            for i in range(cacheNum):
                randNum = np.random.rand()
                if randNum<22/24:
                    contentServerCaches[i].set(content, 1)
    broadcastHits.append(broadcastHit / len(requests))
    for i in range(cacheNum):
        contentServerCaches[i].clear()
"""计算广播下的命中率end"""
print(broadcastHits)
print(str(datetime.datetime.now()))
timebegin = datetime.datetime.now()

requestThreshold = 10000  # 设置触发cluster所需要收到的请求数量
print("in cluster")
"""计算cluster下的命中率"""
partitionK = 4
clusterHits = []
contentServerRequests = {}
for i in range(cacheNum):
    contentServerRequests[i] = [0]
for oneCacheSize in cacheSize:
    parts = []
    cluster = []
    clusterHit = 0
    clusterFlag = 0  # 判断当前是否已经进行过cluster的划分
    requestsNum = 0  # 计数当前收到的请求数量

    for i in range(cacheNum):
        contentServerCaches[i].setsize(oneCacheSize)
    for oneRequest in requests:
        requestsNum += 1
        content = oneRequest.content
        contentServer = oneRequest.contentServer

        line = contentServerRequests[contentServer]
        line.append(content)
        contentServerRequests[contentServer] = line

        if requestsNum > requestThreshold:  # 请求数量达到阈值,触发cluster
            requestsNum = 0
            clusterFlag = 1
            kpartitioner = KPartition.KPartition(contentServerRequests)
            parts = kpartitioner.getKPartitions(partitionK)
            cluster = {}  # culsterNo:[contentServer1,contentServer2]
            cacheNo = 0
            for oneVal in parts:
                if oneVal in cluster.keys():
                    line = cluster[oneVal]
                    line.append(cacheNo)
                    cluster[oneVal] = line
                else:
                    line = []
                    line.append(cacheNo)
                    cluster[oneVal] = line
                cacheNo += 1

            for i in range(cacheNum):
                contentServerRequests[i] = [0]
        if contentServerCaches[contentServer].get(content):
            clusterHit += 1
            continue
        if clusterFlag == 0:
            contentServerCaches[contentServer].set(content, 1)
        else:
            clusterNo = parts[contentServer]
            cachesToPush = cluster[clusterNo]
            for oneCache in cachesToPush:
                randNum = np.random.rand()
                if randNum<22/24:
                    contentServerCaches[oneCache].set(content, 1)
    clusterHits.append(clusterHit / len(requests))
endTime = datetime.datetime.now()
print("cluster time cost: " + str(endTime - timebegin))
print(clusterHits)
print(str(datetime.datetime.now()))
"""计算cluster下的命中率end"""
print("in probablility ")
timebegin = datetime.datetime.now()
probabilityHits = []
contentServerRequests = {}
for i in range(cacheNum):
    contentServerRequests[i] = [0]
for oneCacheSize in cacheSize:
    parts = []
    cluster = []
    probabilityHit = 0
    clusterFlag = 0  # 判断当前是否已经进行过cluster的划分
    requestsNum = 0  # 计数当前收到的请求数量
    probabilityScope = 100  # 用于计算probability 的范围
    for i in range(cacheNum):
        contentServerCaches[i].setsize(oneCacheSize)
    for oneRequest in requests:
        requestsNum += 1
        content = oneRequest.content
        contentServer = oneRequest.contentServer

        line = contentServerRequests[contentServer]
        line.append(content)
        contentServerRequests[contentServer] = line

        if requestsNum > requestThreshold:  # 请求数量达到阈值,触发cluster
            requestsNum = 0
            clusterFlag = 1
            kpartitioner = KPartition.KPartition(contentServerRequests)
            parts = kpartitioner.getKPartitions(partitionK)
            cluster = {}  # culsterNo:[contentServer1,contentServer2]
            cacheNo = 0
            for oneVal in parts:
                if oneVal in cluster.keys():
                    line = cluster[oneVal]
                    line.append(cacheNo)
                    cluster[oneVal] = line
                else:
                    line = []
                    line.append(cacheNo)
                    cluster[oneVal] = line
                cacheNo += 1

            for i in range(cacheNum):
                contentServerRequests[i] = [0]
        if contentServerCaches[contentServer].get(content):
            probabilityHit += 1
            continue
        if clusterFlag == 0:
            contentServerCaches[contentServer].set(content, 1)
        else:
            clusterNo = parts[contentServer]
            cachesToPush = cluster[clusterNo]
            for oneCache in cachesToPush:
                if oneCache in contentServerRequests.keys():
                    simility = kpartitioner.computeSimilarity(contentServerRequests[contentServer][-100:],
                                                              contentServerRequests[oneCache][-100:])
                else:  # 如果该cache目前没有收到请求
                    simility = 0
                randNum = np.random.rand()
                if randNum < simility and randNum < 22/24:
                    contentServerCaches[oneCache].set(content, 1)
    probabilityHits.append(probabilityHit / len(requests))
endTime = datetime.datetime.now()
print("probability time cost: " + str(endTime - timebegin))
print(probabilityHits)
print(str(datetime.datetime.now()))

def whetherCacheThisContent(totalReuqestsNum, feature, r_c, contentRequestCount: dict, featureContent: dict):
    contentPopularities = {}
    for oneContent in contentRequestCount.keys():
        contentPopularities[oneContent] = contentRequestCount[oneContent] / totalReuqestsNum
    contentPopularities = dict(sorted(contentPopularities.items(), key=lambda item: item[1], reverse=True))
    keyList = list(contentPopularities.keys())
    if r_c > 1:
        r_c = 1
    threshold = contentPopularities[keyList[int(r_c * len(keyList) - 1)]]
    contentNum = 0
    totalPop = 0
    for oneContent in featureContent[feature]:
        contentNum += 1
        totalPop += contentPopularities[oneContent]
    avgPop = totalPop / contentNum

    if avgPop >= threshold:
        return True

    return False
#
contentCount = set()
for oneRequest in requests:
    content = oneRequest.content
    contentCount.add(content)
contentNum = len(contentCount)
#
# ageBasedHits = []
# contentRequestCount = {}
# contentFirstArriveTime = {}
# featureContent = {}
#
# for oneCacheSize in cacheSize:
#     print(oneCacheSize)
#     print(str(datetime.datetime.now()))
#     r_c = oneCacheSize / contentNum
#     for i in range(cacheNum):
#         contentServerCaches[i].setsize(oneCacheSize)
#     ageBasedHit = 0
#     totalRequestsNum = 0
#     ##debug 变量
#     cachedNum = 0
#     notCachedNum = 0
#     ##debug 变量
#
#     for oneRequest in requests:
#         totalRequestsNum += 1
#         content = oneRequest.content
#         contentServer = oneRequest.contentServer
#         sampleTime = oneRequest.requestTime
#         # 记录每个内容全局被访问的总数,内容全局第一次到达的时间，该特征值对应的内容
#         if content in contentRequestCount:
#             contentRequestCount[content] += 1
#         else:
#             contentFirstArriveTime[content] = sampleTime
#             contentRequestCount[content] = 1
#
#         N_m = contentRequestCount[content]  # 内容对应的请求数量
#         T_m = sampleTime - contentFirstArriveTime[content]  # 内容年龄
#         feature = (N_m, T_m)
#         if feature not in featureContent.keys():
#             contents = []
#             contents.append(content)
#             featureContent[feature] = contents
#         else:
#             featureContent[feature].append(content)
#
#         if contentServerCaches[contentServer].get(content):
#             ageBasedHit += 1
#
#         elif whetherCacheThisContent(totalRequestsNum, feature, max(0.5, r_c), contentRequestCount, featureContent):
#             cachedNum += 1
#             contentServerCaches[contentServer].set(content, 1)
#         else:
#             notCachedNum += 1
#     print("cached " + str(cachedNum) + " notCachedNum  " + str(notCachedNum))
#     ageBasedHits.append(ageBasedHit / len(requests))
# print(ageBasedHits)
preFetchHits = []
contentRequestCount = {}
contentFirstArriveTime = {}
featureContent = {}
for oneCacheSize in cacheSize:
    print(oneCacheSize)
    print(str(datetime.datetime.now()))
    r_c = oneCacheSize / contentNum
    for i in range(cacheNum):
        contentServerCaches[i].setsize(oneCacheSize)
    preFetchHit = 0
    totalRequestsNum = 0
    ##debug 变量
    cachedNum = 0
    notCachedNum = 0
    ##debug 变量

    for oneRequest in requests:
        totalRequestsNum += 1
        content = oneRequest.content
        contentServer = oneRequest.contentServer
        sampleTime = oneRequest.requestTime
        # 记录每个内容全局被访问的总数,内容全局第一次到达的时间，该特征值对应的内容
        if content in contentRequestCount:
            contentRequestCount[content] += 1
        else:
            contentFirstArriveTime[content] = sampleTime
            contentRequestCount[content] = 1

        N_m = contentRequestCount[content]  # 内容对应的请求数量
        T_m = sampleTime - contentFirstArriveTime[content]  # 内容年龄
        feature = (N_m, T_m)
        if feature not in featureContent.keys():
            contents = []
            contents.append(content)
            featureContent[feature] = contents
        else:
            featureContent[feature].append(content)

        if contentServerCaches[contentServer].get(content):
            preFetchHit += 1

        # 若该内容热度非常高，为二分之一r_c上侧分位数
        elif whetherCacheThisContent(totalRequestsNum, feature, min(0.05, r_c / 2), contentRequestCount,
                                     featureContent):
            cachedNum += 1
            for i in range(cacheNum):
                contentServerCaches[i].set(content, 1)
        elif whetherCacheThisContent(totalRequestsNum, feature, max(0.5, r_c), contentRequestCount, featureContent):
            cachedNum += 1
            contentServerCaches[contentServer].set(content, 1)

        else:
            notCachedNum += 1
    print("cached " + str(cachedNum) + " notCachedNum  " + str(notCachedNum))
    preFetchHits.append(preFetchHit / len(requests))

# print(unicastHits)
print(broadcastHits)
print(clusterHits)
print(probabilityHits)
# print(ageBasedHits)
print(preFetchHits)
endTime = datetime.datetime.now()
print("time cost: " + str(endTime - timebeginG))