import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math

from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import *


def centroid(data):
    return np.mean(data, axis = 0)


def anomaly_score(data, c, noise = 0.01):
    dist = {i: euclidean(data[i], c) for i in range(len(data))}

    minv = min(dist.values())
    maxv = max(dist.values())

    dist = {i: (dist[i] - minv + noise) / (maxv - minv + noise) for i in dist.keys()}

    return dist


def forest(D):
    isolation_forest = IsolationForest(n_estimators=100)
    isolation_forest.fit(D['Temperature'].values.reshape(-1, 1))

    x = np.linspace(D['Temperature'].min(), D['Temperature'].max(), len(D)).reshape(-1, 1)
    print(x)

    anomaly_score = isolation_forest.decision_function(x)
    print (anomaly_score)

    outlier = isolation_forest.predict(x)
    print(len(outlier))
    return anomaly_score


def forest2(D):
    isolation_forest = IsolationForest(n_estimators=100)
    D = np.array(D)
    print(np.shape(D))

    return isolation_forest.fit_predict(D)


def pca(Data):

    pca = PCA(n_components=2)
    Data = StandardScaler().fit_transform(Data)
    pca.fit(Data)
    Data = pca.transform(Data)
    return Data


D = pd.read_excel('/home/virtualmachine1/PycharmProjects/mqtt_anomalyDetection/AnomalyData.xlsx')

Data = []
for i in range(len(D)):
    point = list(D.loc[i, ['Temperature']]) + list(D.loc[i, ['Barometric']]) + list(D.loc[i, ['Humidity']]) + list(D.loc[i, ['Solar Radiance']]) + list(D.loc[i, ['Soil Temp']])
    print (point)
    Data.append(point)

Data = pca(Data)

# Isolation forest approach
#score = forest2(Data)

# Centroid approach
c = centroid(Data)
score = anomaly_score(Data, c, noise = 0.01)
print (score)

Dict = {}
for i in range(len(D)):

    # For centroid-based approach
    Dict[i] = list(D.loc[i]) + [score[i]]

    # For isolation forest-based approach (-1 for outliers and 1 for inliers)
    Dict[i] = list(D.loc[i]) + [score[i]]

print (Dict)

Dict2 = {key:Dict[key][-1] for key in Dict.keys()}
sortedList=sorted(Dict2.items(), key=lambda x: x[1], reverse = True)
sortedList=[val[0] for val in sortedList][:5] # k is the bound


dictionary_report={}
for j in sortedList:

    timestamp=Dict[j][0]
    dictionary_report[timestamp] = Dict[j][-1]
    #timestamp.append((Dict[j][0],Dict[j][-1]))
    #timestamp.append(Dict[j][-1])
print("Top 5 most anomalous points", sortedList)
print("Anomalous Data Points timestamp", dictionary_report)
