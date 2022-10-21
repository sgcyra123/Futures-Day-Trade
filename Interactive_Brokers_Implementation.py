# -*- coding: utf-8 -*-
"""
Created on Thu Oct 20 18:01:10 2022

@author: scyra
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
from ibapi.order import Order
import numpy as np
from datetime import date
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.data = {}
        self.acct = pd.DataFrame(columns=["AccountSummary. ReqId:","Account:",
                   "Tag: ", "Value:" ,"Currency:"])
        self.pos_df = pd.DataFrame(columns=["Symbol", 
                   "Position","SecType", "Avg cost"])
        self.order_df = pd.DataFrame(columns=["PermId:", "ClientId:", " OrderId:", 
                   "Account:", "Symbol:", "SecType:",
                   "Exchange:", "Action:", "OrderType:",
                   "TotalQty:", "CashQty:", 
                   "LmtPrice:", "AuxPrice:", "Status:"])
        self.realpnl = pd.DataFrame(columns=["Date", "RealizedPnL"])
        self.nqorderid=0
        self.ymorderid=0
        self.firstfive=0
        self.balance=0
        self.ymmultiplier=0
   
        
    def contractDetails(self, reqId, contractDetails):
        print("reqID: {}, contract:{}".format(reqId, contractDetails))
        
    def pnl(self,reqId, dailyPnL, unrealizedPnL, realizedPnL):

        dictionary=pd.DataFrame({"Date":date.today(),"RealizedPnL":realizedPnL}, index=[0])
        self.realpnl = pd.concat([self.realpnl,dictionary])
        print(realizedPnL)
     
    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId]=[{"Date": bar.date, "Open":bar.open, "High":bar.high, "Low":bar.low, "Close":bar.close, "Volume":bar.volume}]
        if reqId in self.data:
            self.data[reqId].append({"Date": bar.date, "Open":bar.open, "High":bar.high, "Low":bar.low, "Close":bar.close, "Volume":bar.volume})
            
        
        
    def position(self, account, contract, position,avgCost):
        super().position(account, contract, position, avgCost)
        dictionary= pd.DataFrame({"Symbol": contract.symbol, 
                   "Position":position,"SecType": contract.secType, "Avg cost": avgCost},index=[0])
        self.pos_df=pd.concat([self.pos_df, dictionary], ignore_index = True)
    def positionEnd(self):
        print("latest position data extracted")
        
        
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)  
        
    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        print("AccountSummary. ReqId:", reqId, "Account:", account,
                   "Tag: ", tag, "Value:", value, "Currency:", currency)

        dictionary = {"AccountSummary. ReqId:": reqId, "Account:": account,
                    "Tag: ": tag, "Value:": value, "Currency:": currency}
        self.acct=self.acct.append(dictionary, ignore_index = True)
        
    def openOrder(self, orderId, contract, order, orderState):
             super().openOrder(orderId, contract, order, orderState)
             dictionary = pd.DataFrame({"PermId:": order.permId, "ClientId:": order.clientId, " OrderId:": orderId, 
                   "Account:": order.account, "Symbol:": contract.symbol, "SecType:": contract.secType,
                   "Exchange:": contract.exchange, "Action:": order.action, "OrderType:": order.orderType,
                   "TotalQty:": order.totalQuantity, "CashQty:": order.cashQty, 
                   "LmtPrice:": order.lmtPrice, "AuxPrice:":order.auxPrice, "Status:": orderState.status},index=[0])
             self.order_df=pd.concat([self.order_df,dictionary], ignore_index = True)
def websocket_con():
    app.run()

        
app=TradingApp()
app.connect("127.0.0.1", 7497, clientId=0)

con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)


def Future(symbol,yearmonth, sec_type="FUT", currency="USD"):
    contract = Contract()
    contract.symbol = symbol #"YM"
    contract.secType = sec_type #"FUT"
    contract.currency = currency #"USD"
    if contract.symbol =="YM" or contract.symbol=="MYM":
        contract.exchange = "ECBOT"
    elif contract.symbol =="NQ":
        contract.exchange = "GLOBEX"
    
    contract.lastTradeDateOrContractMonth = yearmonth #"202209"
    contract.includeExpired= True
    return contract


def histData(req_num, contract,enddatetime,duration, candle_size):
    app.reqHistoricalData(reqId=req_num, contract=contract, endDateTime=enddatetime, durationStr=duration, barSizeSetting=candle_size, whatToShow='BID', useRTH=0, formatDate=1, keepUpToDate=0, chartOptions=[])


def limitOrder(direction,quantity,lmt_price):
    order = Order()
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = lmt_price
    
    return order
def marketOrder(direction,quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    return order

def start(action,nqlast,ymlast,balance,ymmultiplier):
    nq=Future("NQ","202212")
    nqcontracts=round((balance*10)/(nqlast*20))
    if action=="BUY":
        nqprice=nqlast-1
    else:
        nqprice=nqlast+1               
    nqorder=limitOrder(action,nqcontracts, nqprice)
    nqorder.transmit = False
    ratio=((nqlast*20*ymmultiplier)/(ymlast*5))
    if action=="BUY":
        if (ratio*nqcontracts)-int(ratio*nqcontracts)>.7:
            ymcontracts=round((ratio*nqcontracts))
        else:
            ymcontracts=int(ratio*nqcontracts)
            
    else:
        if (ratio*nqcontracts)-int(ratio*nqcontracts)<=.3:
            ymcontracts=round(nqcontracts*ratio)
        else:
            ymcontracts=int((ratio*nqcontracts)+1)
        
 
    ratio3=(ymcontracts/nqcontracts)+.01
    ratio2=int(ratio*nqcontracts)/nqcontracts
    
    app.reqIds(-1)
    time.sleep(1)
    nqorderid = app.nextValidOrderId
    app.nqorderid=nqorderid
    #ymorderid=app.nextValidId
    app.placeOrder(nqorderid, nq, nqorder)
    time.sleep(1)
    ym=Future("YM","202212")
    ymorder=marketOrder("SELL" if action=="BUY" else "BUY" , 0)
    ymorder.parentId = nqorderid
    ymorder.hedgeType = "P"
    ymorder.hedgeParam = str(ratio3)#".4"
    ymorder.transmit=True
   
    app.placeOrder(nqorderid+1,ym, ymorder)
    
    print("ratio="+str(ratio3))
    return nqorderid

def stop(action,nqlast,nqcontracts,ratio3):
    nq=Future("NQ","202212")
    if action=="BUY":
        nqprice=nqlast-2
    else:
        nqprice=nqlast+2 
    nqorder=limitOrder(action,nqcontracts, nqprice)
    nqorder.transmit = False
    app.reqIds(-1)
    time.sleep(1)
    nqorderid = app.nextValidOrderId
    app.nqorderid=nqorderid
    #ymorderid=app.nextValidId
    app.placeOrder(nqorderid, nq, nqorder)
    time.sleep(1)
    ym=Future("YM","202212")
    ymorder=marketOrder("SELL" if action=="BUY" else "BUY" , 0)
    ymorder.parentId = nqorderid
    ymorder.hedgeType = "P"
    ymorder.hedgeParam = str(ratio3+.01)
    ymorder.transmit=True
    app.placeOrder(nqorderid+1,ym, ymorder)
    print("Close order submitted")
    return nqorderid
    




data_dict = app.data

def dataDataframe(app,futs):
    df_data={}
    
    for fut in futs:
        if len(app.data)!=2:
            continue
        df_data[fut]=pd.DataFrame(app.data[futs.index(fut)])
        df_data[fut].set_index("Date", inplace = True)
    return df_data
futs = ["YM", "NQ"]


for fut in futs:
     histData(futs.index(fut), Future(fut, "202212"), "","7 D","5 mins")
time.sleep(3.5)

if len(app.data)==0:
    print("Did not download initial data")
else:    
    ohlcv = dataDataframe(app,futs)
    ym=ohlcv["YM"]
    nq=ohlcv["NQ"]
    ym.index = pd.to_datetime(ym.index)
    nq.index = pd.to_datetime(nq.index)
    ym["yperiodreturn"]=np.log(ym["Close"]/ym["Close"].shift(198))
    nq["yperiodreturn"]=np.log(nq["Close"]/nq["Close"].shift(198))
   
    ym["ymvol"]=ym["yperiodreturn"].rolling(1300).std()
    nq["nqvol"]=nq["yperiodreturn"].rolling(1300).std()
    nq["hour"]=nq.index.hour
    
    app.balance=69517
    ymmultiplier = nq["nqvol"][-1]/ym["ymvol"][-1]
    app.ymmultiplier = ymmultiplier  
    print("Initial Data download successful")
    print("BALANCE:"+str(app.balance))
    app.data = {}   
def main(a="YM", b="NQ"):
    
    
    app.reqOpenOrders()
    
    order_df=app.order_df
    order_df.drop_duplicates(inplace=True,ignore_index=True)
    for fut in futs:
        histData(futs.index(fut), Future(fut, "202212"), "","1 D","5 mins")
    time.sleep(.7)
    if len(app.data)!=2:
        print("Did not download data in main function")
    else:    
        ohlcv = dataDataframe(app,futs)
        ym=ohlcv["YM"]
        nq=ohlcv["NQ"]
        ym.index = pd.to_datetime(ym.index)
        nq.index = pd.to_datetime(nq.index)
        ym["xperiodreturn"]=np.log(ym["Close"]/ym["Close"].shift(180))
        nq["xperiodreturn"]=np.log(nq["Close"]/nq["Close"].shift(180))
        ym["yperiodreturn"]=np.log(ym["Close"]/ym["Close"].shift(198))
        nq["yperiodreturn"]=np.log(nq["Close"]/nq["Close"].shift(198))
        ym["ymret"]=np.log(ym["Close"]/ym["Close"].shift(1))
        nq["nqret"]=np.log(nq["Close"]/nq["Close"].shift(1))
        ym["ymvol"]=ym["yperiodreturn"].rolling(1300).std()
        nq["nqvol"]=nq["yperiodreturn"].rolling(1300).std()
        nq["hour"]=nq.index.hour
        
        balance=app.balance
        
        ymtot=0
        nqtot=0
        print("ymmultiplier:"+str(app.ymmultiplier))
        print("NQ"+str(nq.iloc[-1,[0,3]]))
        print("YM"+str(ym.iloc[-1,[0,3]]))
        if "NQ" in order_df["Symbol:"].to_list():# or "MYM" in order_df["Symbol:"].to_list():
                app.cancelOrder(app.nqorderid)
                #app.cancelOrder(app.nqorderid+1)
                app.cancelOrder(app.ymorderid)
                time.sleep(.5)
        app.pos_df=app.pos_df.iloc[0:0]
        app.reqPositions()
        time.sleep(1)
        pos_df=app.pos_df
        pos_df.drop_duplicates(inplace=True,ignore_index=True)
        pos_df=pos_df[pos_df.Position!=0]        
        ymseries=pos_df.loc[pos_df['Symbol'] == "YM","Position"]
        nqseries=pos_df.loc[pos_df['Symbol'] == "NQ","Position"]
        if not ymseries.empty: 
           print("YM Pos:"+str(ymseries.iloc[-1]))
        if not nqseries.empty:
           print("NQ Pos:"+str(nqseries.iloc[-1]))
        if ymseries.empty and nqseries.empty:
                
            if nq["hour"][-1]==8 and nq["hour"][-7]==8 and nq["hour"][-8]!=8:
                if nq["nqret"][-1]-(ym["ymret"][-1]*ymmultiplier)>0:
                    app.firstfive=1
                    print("nq up firstfive")
                    print(nq["xperiodreturn"][-1]-(ym["xperiodreturn"][-1]*ymmultiplier))
                    
                elif (ym["ymret"][-1]*ymmultiplier)-nq["nqret"][-1]>0:
                    app.firstfive=2
                    print("ym up firstfive")
                    print((ym["xperiodreturn"][-1]*ymmultiplier)-nq["xperiodreturn"][-1])
                else:
                    app.firstfive=0
        
        
            if nq["hour"][-1]==8 and nq["hour"][-7]==8:
                if nq["xperiodreturn"][-1]-(ym["xperiodreturn"][-1]*ymmultiplier)>.002:
                    print(nq["xperiodreturn"][-1]-(ym["xperiodreturn"][-1]*ymmultiplier))
                    if app.firstfive==1:
                        start("BUY",nq["Close"][-1],ym["Close"][-1],balance,ymmultiplier)
                        print("BUY nq triggered")
                    
                    
                elif (ym["xperiodreturn"][-1]*ymmultiplier)-nq["xperiodreturn"][-1]>.002:
                    print((ym["xperiodreturn"][-1]*ymmultiplier)-nq["xperiodreturn"][-1])
                    if  app.firstfive==2:
                        start("SELL",nq["Close"][-1],ym["Close"][-1],balance,ymmultiplier)
                        print("Sell nq triggered")
                
                    
        elif not ymseries.empty and not nqseries.empty:
            
            if nq["hour"][-1]==10 and nq["hour"][-4]==10:
               
                if ymseries.iloc[-1]>0 and nqseries.iloc[-1]<0:
                    print((ym["yperiodreturn"][-1]*ymmultiplier)-nq["yperiodreturn"][-1])
                    if (ym["yperiodreturn"][-1]*ymmultiplier)-nq["yperiodreturn"][-1]<0:
                        stop("BUY",nq["Close"][-1],abs(nqseries.iloc[-1]),abs(ymseries.iloc[-1]/nqseries.iloc[-1]))
                elif nqseries.iloc[-1]>0 and ymseries.iloc[-1]<0:
                    print(nq["yperiodreturn"][-1]-(ym["yperiodreturn"][-1]*ymmultiplier))
                    if nq["yperiodreturn"][-1]-(ym["yperiodreturn"][-1]*ymmultiplier)<0:
                        stop("SELL",nq["Close"][-1],abs(nqseries.iloc[-1]),abs(ymseries.iloc[-1]/nqseries.iloc[-1]))
            
            #elif (nq["hour"][-1]==14 and nq["hour"][-11]==14) or nq["hour"][-1]==15:         
            elif nq["hour"][-1]==8 and nq["hour"][-3]==8 and nq["hour"][-7]!=8:   
                    if nqseries.iloc[-1]<0 and ymseries.iloc[-1]>0:
                        stop("BUY",nq["Close"][-1],abs(nqseries.iloc[-1]),abs(ymseries.iloc[-1]/nqseries.iloc[-1]))
                    else:
                        stop("SELL",nq["Close"][-1],abs(nqseries.iloc[-1]),abs(ymseries.iloc[-1]/nqseries.iloc[-1]))
        elif not ymseries.empty and nqseries.empty:
                    app.reqIds(-1)
                    time.sleep(2)
                    jgh = app.nextValidOrderId
                    app.ymorderid=jgh
                    if ymseries.iloc[-1]<0:
                        app.placeOrder(jgh, Future("YM","202212"),marketOrder("BUY",abs(ymseries.iloc[-1]) ))
                    elif ymseries.iloc[-1]>0:
                        app.placeOrder(jgh, Future("YM","202212"),marketOrder("SELL",abs(ymseries.iloc[-1]) ))
        elif ymseries.empty and not nqseries.empty: 
                    app.reqIds(-1)
                    time.sleep(2)
                    tgh = app.nextValidOrderId
                    app.ymorderid=tgh
                    if nqseries.iloc[-1]<0:
                        app.placeOrder(tgh, Future("NQ","202212"),marketOrder("BUY",abs(nqseries.iloc[-1]) ))
                    elif nqseries.iloc[-1]>0:
                        app.placeOrder(tgh, Future("NQ","202212"),marketOrder("SELL",abs(nqseries.iloc[-1]) ))
       
    
        app.data = {}       
                
            
    
starttime = time.time()
timeout = starttime +(10800)
while time.time() <=timeout:
    
    main()
    time.sleep(300- ((time.time()-starttime)%300))