import numpy as np
import pandas as pd

train=pd.read_csv('NIFTY_BBC20_14_s_o3m_y_2019.csv')

train1=[]
train1=train['high']
train1=list(train1)
a=[]

for i in range(0,20):
    a.append(train1[i])    

mea=np.mean(a);

