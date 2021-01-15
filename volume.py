import ccxt
import re
import pandas as pd
import time
start_time = time.time()

exchange = ccxt.binance()
exchange.load_markets()


x = pd.DataFrame()
counter = 0
keys_list=[]
for key in exchange.symbols:


    if key.endswith('USDT' ) | key.endswith('BTC' ):
        keys_list.append(key)
        print(f'downloading data for {key}')
        data = exchange.fetch_ohlcv(key,'1h')
        df = pd.DataFrame(data)
        if not df.empty:
            df[0] =  pd.to_datetime(df[0],unit='ms')
            df.columns = ['date','open','high','low','close','volume']
            



            df["open"] = pd.to_numeric(df["open"])
            df["open"] = pd.to_numeric(df["open"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])
            df["close"] = pd.to_numeric(df["close"])
            df["volume"] = round(pd.to_numeric(df["volume"]))

            df['pchange1h'] = df.close.diff(1).fillna(0) # diff can has if for different timeperiods 
            df['pchange1hpct'] = round((df['pchange1h']/df["close"])*100,2)

            df['pchange24h'] = df.close.diff(23).fillna(0) # diff can has if for different timeperiods 
            df['pchange24hpct'] = round((df['pchange24h']/df["close"])*100,2)

            df['v1h'] = df.volume.rolling(window = 1).sum().fillna(0)#.shift()

            df['vchange1h'] = df.v1h.diff(1).fillna(0) # diff can has if for different timeperiods 
            df['vchange1hpct'] = round((df['vchange1h']/df["volume"])*100,2)

            df['v4h'] = df.volume.rolling(window = 4).sum().fillna(0)#.shift()
            df['vchange4h'] = df.v4h.diff(4).fillna(0) # diff can has if for different timeperiods 
            df['vchange4hpct'] = round((df['vchange4h']/df["volume"])*100,2)

            df['v24'] = df.volume.rolling(window = 23).sum().fillna(0)#.shift()
            df['vchange24h'] = df.v24.diff(23).fillna(0) # diff can has if for different timeperiods 
            df['vchange24hpct'] = round((df['vchange24h']/df["volume"])*100,2)

            df['pricegain'] = (df.open.pct_change()*100).fillna(0)
    


            lastprice = (list(df.close.tail(1)))[0] #lastprice
            pchange1hpct = (list(df.pchange1hpct.tail(1)))[0] #Pchange1H_pct
            pchange24hpct = (list(df.pchange24hpct.tail(1)))[0] #Pchange24H_pct
            vchange1h = (list(df.vchange1h.tail(1)))[0] #Vchange1H
            vchange1hpct = (list(df.vchange1hpct.tail(1)))[0] #vchange1hpct
            vchange4h = (list(df.vchange4h.tail(1)))[0] #Vchange4H
            vchange4hpct = (list(df.vchange4hpct.tail(1)))[0] #Vchange4H
            vchange24h = (list(df.vchange24h.tail(1)))[0] #vchange24h
            vchange24hpct = (list(df.vchange24hpct.tail(1)))[0] #Vchange24H_pct
            volume24h = (list(df.v24.tail(1)))[0] #vchange24h
            interval = '1h'

            
            x = x.append(pd.DataFrame({'symbol':key,
                'interval':interval,
                'lastPrice':lastprice,
                'volume_24h':volume24h,
                'priceChangePercent_1h':pchange1hpct,
                'priceChangePercent_24h':pchange24hpct,
                'volumeChange_1h':vchange1h,
                'volumeChangePercent_1h':vchange1hpct,
                'volumeChange_4h':vchange4h,
                'volumeChangePercent_4h':vchange4hpct,
                'volumeChange_24h':vchange24h,
                'volumeChangePercent_24h':vchange24hpct},index = [0]))
            print("appened")
            counter += 1
x.to_csv('VolumeGainers.csv',index= False, mode = 'a',header =True)

keys_df = pd.DataFrame(keys_list)


print(x)
print(counter)

print("--- %s seconds ---" % (time.time() - start_time))

