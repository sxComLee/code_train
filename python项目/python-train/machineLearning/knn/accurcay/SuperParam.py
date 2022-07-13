from sklearn.neighbors import KNeighborsClassifier
from sklearn import datasets
from sklearn.model_selection import train_test_split

iris = datasets.load_iris()
x = iris.data
y = iris.target
X_train, X_test, y_train, y_test  = train_test_split(x, y, test_size=0.2, random_state=666)
best_score = 0.0
best_k = -1
for k in range(1,11): # 暂且设定到1～11的范围内
    knn_clf = KNeighborsClassifier(n_neighbors=k)
    knn_clf.fit(X_train,y_train)
    score = knn_clf.score(X_train,y_train)
    if score > best_score:
        best_k = k
        best_score = score
    print('best_k = ',best_k)
    print('best_score = ',best_score)

best_method = ""
best_score = 0.0
best_k = -1
for method in ["uniform","distance"]:
    for k in range(1,11):
        knn_clf = KNeighborsClassifier(n_neighbors=k,weights=method,p=2)
        knn_clf.fit(X_train,y_train)
        score = knn_clf.score(X_test, y_test)
        if score > best_score:
            best_k = k
            best_score = score
            best_method = method

print("best_method = ", method)
print("best_k = ", best_k)
print("best_score = ", best_score)

params_search = [{'weights':['uniform'] , 'n_neighbors':[i for i in range(1,11)]}
                 ,{'weights':['distance'] , 'n_neighbors':[i for i in range(1,11)]
                 ,'p':[i for i in range(1,6)]}]
knn_clf = KNeighborsClassifier
# 调用网格搜索方法
from sklearn.model_selection import GridSearchCV
# 定义网格搜索的对象grid_search，其构造函数的第一个参数表示对哪一个分类器进行算法搜索，第二个参数表示网格搜索相应的参数
grid_search = GridSearchCV(knn_clf,params_search)
grid_search.fit(X_train, y_train)
