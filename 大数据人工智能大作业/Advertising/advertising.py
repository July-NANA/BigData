import csv
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression


# 读取数据
path = 'advertising.csv'
# pandas读入
data = pd.read_csv(path)    # TV、Radio、Newspaper、Sales
x = data[['TV', 'Radio', 'Newspaper']]
y = data['Sales']


# 构建线性回归模型
x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=1)
linreg = LinearRegression()
model = linreg.fit(x_train, y_train)
print(model)
print(linreg.coef_)
print(linreg.intercept_)

# print(x_test)
test=np.array(x_test)
# type(test)
test_pd=pd.DataFrame(test)
# print(test_pd)
test_pd.columns=['TV','Radio','Newspaper']
# test_pd


# 预测
y_hat = linreg.predict(np.array(x_test))
# type(y_hat)
y_pd=pd.DataFrame(y_hat)
# type(y_pd)
y_pd.columns=['Sales']
# y_pd



# 输出结果
result=pd.concat([test_pd,y_pd],axis=1)
result.to_csv('./result.csv',index=0)