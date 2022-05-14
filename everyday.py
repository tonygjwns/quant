

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import Analyzer
import datetime
from datetime import timedelta
import matplotlib.dates as mdates
import DualMomentum
import requests
import DBUpdater

#볼린저 밴드 신호: 주가가 상단 밴드에 접근하며, MFI가 양이어서 추세가 상승일때
#볼린저 반전 신호: 주가가 하단 밴드부분인데 II가 양이어서 강세일때
#삼중창: 주간은 상승인데 일간이 하락일 때 매수, 반대는 매도
#듀얼 모멘텀: 6개월간 상승 추세

db=DBUpdater.DBUpdater()
db.execute_daily()



mk = Analyzer.MarketDB()
df = pd.DataFrame()
d=datetime.date.today()
stockcode=[]
stockcode=mk.codes.keys()

signals=pd.DataFrame(columns=['code','bollingerbandsignal','bollingerinversesignal','threesscreen','dualmomentum'])

dm=DualMomentum.DualMomentum()
rm=dm.get_rltv_momentum((d-timedelta(days=180)).strftime("%Y-%m-%d"),d,300)
tryed=[]
failedlist=[]

portfoliolist=['미원에스씨','우리금융지주','농심','삼성전자','동부건설','삼성전기','한국전력공사','삼성에스디에스','삼보판지','디씨엠','코웰패션','케이티앤지','NAVER','카카오','JYP Ent.','정상제이엘에스','고려신용정보','위세아이텍','LG전자','GST','일진파워','ISC','SK이노베이션','디와이피엔에프','크리스에프앤씨','미원화학','에이리츠','하나머티리얼즈','메가스터디교육','타이거일렉','지니언스']
order=[]
checked=[]

for s in stockcode:
    company_name=s
    df = mk.get_daily_price(company_name, (d-timedelta(days=90)).strftime("%Y-%m-%d"),d.strftime("%Y-%m-%d"))
      
    df['MA20'] = df['CLOSE'].rolling(window=20).mean() 
    df['stddev'] = df['CLOSE'].rolling(window=20).std() 
    df['upper'] = df['MA20'] + (df['stddev'] * 2)
    df['lower'] = df['MA20'] - (df['stddev'] * 2)
    df['PB'] = (df['CLOSE'] - df['lower']) / (df['upper'] - df['lower'])
    df['TP'] = (df['HIGH'] + df['LOW'] + df['CLOSE']) / 3
    df['II'] = (2*df['CLOSE']-df['HIGH']-df['LOW'])/(df['HIGH']-df['LOW'])*df['VOLUME']  # ①
    df['IIP21'] = df['II'].rolling(window=21).sum()/df['VOLUME'].rolling(window=21).sum()*100  # ②
    df['PMF'] = 0
    df['NMF'] = 0
    for i in range(len(df.CLOSE)-1):
        if df.TP.values[i] < df.TP.values[i+1]:
            df.PMF.values[i+1] = df.TP.values[i+1] * df.VOLUME.values[i+1]
            df.NMF.values[i+1] = 0
        else:
            df.NMF.values[i+1] = df.TP.values[i+1] * df.VOLUME.values[i+1]
            df.PMF.values[i+1] = 0
    df['MFR'] = (df.PMF.rolling(window=10).sum() /
        df.NMF.rolling(window=10).sum())
    df['MFI10'] = 100 - 100 / (1 + df['MFR'])
    df = df[19:]
    
    bollingerchecker=0
    bollingerinversechecker=0
    

    for i in range(len(df.CLOSE)):
        if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:       # ①
            bollingerchecker=1
        elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:     # ③
            bollingerchecker=-1 # ④
            
    for i in range(0, len(df.CLOSE)):
        if df.PB.values[i] < 0.05 and df.IIP21.values[i] > 0:       # ①
            bollingerinversechecker=1
        elif df.PB.values[i] > 0.95 and df.IIP21.values[i] < 0:     # ③
            bollingerinversechecker=-1
    
    

    ddf = mk.get_daily_price(s, (d-timedelta(days=252)).strftime("%Y-%m-%d"))
    
    ema60 = ddf.CLOSE.ewm(span=60).mean()
    ema130 = ddf.CLOSE.ewm(span=130).mean()
    macd = ema60 - ema130
    signal = macd.ewm(span=45).mean()
    macdhist = macd - signal
    ddf = ddf.assign(ema130=ema130, ema60=ema60, macd=macd, signal=signal, macdhist=macdhist).dropna()
    
    ddf['number'] = ddf.DATE.map(mdates.date2num)
    ohlc = ddf[['number','OPEN','HIGH','LOW','CLOSE']]
    
    ndays_high = ddf.HIGH.rolling(window=14, min_periods=1).max()
    ndays_low = ddf.LOW.rolling(window=14, min_periods=1).min()
    
    fast_k = (ddf.CLOSE - ndays_low) / (ndays_high - ndays_low) * 100
    slow_d = fast_k.rolling(window=3).mean()
    ddf = ddf.assign(fast_k=fast_k, slow_d=slow_d).dropna()
    screenchecker=0
    for i in range(1, len(ddf.CLOSE)):
        if ddf.ema130.values[i-1] < ddf.ema130.values[i] and \
            ddf.slow_d.values[i-1] >= 20 and ddf.slow_d.values[i] < 20:
                screenchecker=1
        elif ddf.ema130.values[i-1] > ddf.ema130.values[i] and \
            ddf.slow_d.values[i-1] <= 80 and ddf.slow_d.values[i] > 80:
                screenchecker=-1
    if(s in rm.code.values):
        momentumchecker=1
    else:
        momentumchecker=0
    
    temp_df=pd.DataFrame({'code':[mk.codes[s]],'bollingerbandsignal':[bollingerchecker],'bollingerinversesignal':[bollingerinversechecker],'threesscreen':[screenchecker],'dualmomentum':[momentumchecker]})
    signals=pd.concat([signals,temp_df])
    print(s+" signal processed")

signals.to_csv("D:\\금융\\signalsforall\\"+datetime.date.today().strftime("%Y%m%d")+".csv", encoding="cp949",index=False)
ys=pd.read_csv("D:\\금융\\signalsforall\\"+(datetime.date.today()-timedelta(days=1)).strftime("%Y%m%d")+".csv", encoding='cp949',index_col=False)
ts=pd.read_csv("D:\\금융\\signalsforall\\"+datetime.date.today().strftime("%Y%m%d")+".csv", encoding="cp949",index_col=False)

uplist=[]
downlist=[]
for s in stockcode:
    addyes=ys[ys.code==mk.codes[s]].bollingerbandsignal+ys[ys.code==mk.codes[s]].bollingerinversesignal+ys[ys.code==mk.codes[s]].threesscreen+ys[ys.code==mk.codes[s]].dualmomentum
    addtoday=ts[ts.code==mk.codes[s]].bollingerbandsignal+ts[ts.code==mk.codes[s]].bollingerinversesignal+ts[ts.code==mk.codes[s]].threesscreen+ts[ts.code==mk.codes[s]].dualmomentum
    try:
        addyes=int(addyes)
        addtoday=int(addtoday)
    except:
        if(len(addyes)==0 or len(addtoday)==0):
            print(mk.codes[s]+" error. Not in yes or today")
            continue
        addyes=addyes.values[0]
        addtoday=addtoday.values[0]
    if (addtoday>addyes):
        uplist.append(mk.codes[s])
        if (mk.codes[s] in portfoliolist):
            order.append(mk.codes[s])
            checked.append("got better")
        if(addtoday>=2):
            print(mk.codes[s]+" addtoday is bigger than 2")
            try:
                URL = "https://finance.naver.com/item/main.nhn?code="+s
                comp_temp = requests.get(URL)
                html = comp_temp.text
                financial_stmt = pd.read_html(comp_temp.text)[3]
                financial_stmt.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
                financial_stmt.index.rename('주요재무정보', inplace=True)
                financial_stmt.columns = financial_stmt.columns.droplevel(2)
                financial_stmt.to_csv("D:\\금융\\checklist\\"+mk.codes[s]+".csv", encoding="cp949")
            except Exception as e:
                print(e)
                continue
    elif (addtoday<addyes):
        if (mk.codes[s] in portfoliolist):
            order.append(mk.codes[s])
            checked.append("worsen")
        downlist.append(mk.codes[s])
    else:
        if(mk.codes[s] in portfoliolist):
            order.append(mk.codes[s])
            checked.append("same")
        continue

print(uplist)
print(downlist)
result=dict(zip(order,checked))
print(result)


