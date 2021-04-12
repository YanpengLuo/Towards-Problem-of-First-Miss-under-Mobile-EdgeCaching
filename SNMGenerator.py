# -*- coding: UTF-8 -*-
# _author_ = "luoyanpeng"


import numpy as np
import copy
import Request
import SNMMapToServer
import pymysql
import SNMMapToServer_Youtube

def getRequest(contentServerNum):
    db = pymysql.connect('localhost', 'root', 'love1001', 'SDN')
    cursor = db.cursor()
    sqlLine = "SELECT * FROM SDN.YoutubeBasedSNM  ORDER BY time ASC limit 50000 "
    try:
        cursor.execute(sqlLine)
        requests = cursor.fetchall()
    except:
        db.rollback()
    result = []
    reqId = 0
    contentMapNo = {}
    contentNo = 0
    for oneReq in requests:
        oneRequest = Request.Request()
        oneRequest.id = reqId
        reqId += 1
        oneReq = list(oneReq)
        if oneReq[1] not in contentMapNo.keys():
            contentMapNo[oneReq[1]] = contentNo
            oneContentNo = contentNo
            contentNo += 1
        else:
            oneContentNo = contentMapNo[oneReq[1]]
        oneRequest.content = oneContentNo
        oneRequest.time = oneRequest.requestTime = oneReq[2]
        result.append(oneRequest)
    if contentServerNum == 0 :
        return result
    snmMapToServerYoutube = SNMMapToServer_Youtube.SNMMapToServerYoutube(requestsSeq=result,
                                                                         contentServerNum=contentServerNum)
    snmMapToServerYoutube.computeRequestsReq(cacheNum=contentServerNum)

    return result

def getSNMRequest(contentServerNum):#纯SNM
    db = pymysql.connect('localhost', 'root', 'love1001', 'SDN')
    cursor = db.cursor()
    sqlLine = "SELECT * FROM SDN.SNM  ORDER BY time ASC limit 50000 "
    try:
        cursor.execute(sqlLine)
        requests = cursor.fetchall()
    except:
        db.rollback()
    result = []
    reqId = 0
    contentMapNo = {}
    contentNo = 0
    for oneReq in requests:
        oneRequest = Request.Request()
        oneRequest.id = reqId
        reqId += 1
        oneReq = list(oneReq)
        if oneReq[1] not in contentMapNo.keys():
            contentMapNo[oneReq[1]] = contentNo
            oneContentNo = contentNo
            contentNo += 1
        else:
            oneContentNo = contentMapNo[oneReq[1]]
        oneRequest.content = oneContentNo
        oneRequest.time = oneRequest.requestTime = oneReq[2]
        result.append(oneRequest)

    if contentServerNum == 0:
        return result
    snmMapToServerYoutube = SNMMapToServer_Youtube.SNMMapToServerYoutube(requestsSeq=result,
                                                                         contentServerNum=contentServerNum)
    snmMapToServerYoutube.computeRequestsReq(cacheNum=contentServerNum)

    return result



def getRequestsSeq(cacheNum):
    T = 100  # shot duration
    M = 100  # 内容种类数量
    alpha = 0.8  # 幂律参数
    u_mean = 2  # 平均热度

    zm = np.random.rand(M)  # zm用来计算um，数组长度为内容种类数量，元素从[0,1]中取值
    um = zm ** (-alpha) * u_mean * (1 - alpha)  # 内容m的平均热度


    requestnum = um * T  # 内容m总的请求数量
    wn = []  # 单个内容请求到达的时间序列
    m_wn = {}  # 每个内容的请求到达的时间序列
    wn.append(0)
    T_interva = []
    for onem in range(M):
        T_interva = np.random.exponential(1 / um[onem], int(requestnum[onem]))
        for onetime in range(1, len(T_interva) + 1):
            wn.append(onem * 1 + wn[onetime - 1] + T_interva[onetime - 1])
        m_wn[onem] = copy.copy(wn)
        wn = []
        wn.append(0)

    totallist = []  # 将所有内容的请求时间序列放在一起
    for onekey in m_wn.keys():
        totallist += m_wn[onekey][1:]
    totallist.sort()  # 将其按从小到达排序

    num = 0

    requestseq = [-1] * len(totallist)  # 最后生成的请求序列
    for onem in range(M):  # 对每个内容做遍历，依据其请求到达的时间节点决定请求序列中的位置
        num += 1
        wn = m_wn[onem]
        for onet in wn[1:]:
            requestseq[totallist.index(onet)] = onem


    requestSeq = []
    for i in range(len(totallist)):
        oneRequest = Request.Request()
        oneRequest.id = i
        oneRequest.content = str(requestseq[i])
        oneRequest.requestTime = float(totallist[i])
        requestSeq.append(oneRequest)


    snmMapToServer = SNMMapToServer.SNMMapToServer(requestSeq,um,cacheNum,T)
    yl = snmMapToServer.computeRequestsReq(cacheNum)



    return requestSeq,yl



# getRequestsSeq()