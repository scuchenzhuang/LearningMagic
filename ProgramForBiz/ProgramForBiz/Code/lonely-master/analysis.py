import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score,accuracy_score,confusion_matrix, precision_recall_fscore_support, f1_score, roc_curve, precision_score, recall_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import roc_auc_score,accuracy_score,confusion_matrix, precision_recall_fscore_support, f1_score, roc_curve, precision_score, recall_score
from sklearn.model_selection import GridSearchCV

def data_process1(file):

    my_data = pd.read_excel(file)
    my_data.head(5)
    # 对分类变量进行编码
    mapping_dict = {
        'ethnicity': {
            "Asian": 0,
            "Black": 1,
            "Middle Eastern": 2,
            "South Asian": 3,
            "White-European": 4,
            "Others": 5
        },
        'jundice': {
            "no": 0,
            "yes": 1
        },
        'austim': {
            'no': 0,
            'yes': 1
        },
        'contry_of_res': {
            "Africa": 0,
            "Asia": 1,
            "Europea": 2,
            "North America": 3,
            "Oceania": 4,
            "South America": 5
        },
        'gender': {
            'm': 0,
            'f': 1
        },
        "used_app_before": {
            "no": 0,
            "yes": 1
        },
        "age": {
            "<27": 0,
            ">27": 1
        },
        "relation": {
            "Self": 0,
            "Other": 1
        },
        "Class.ASD": {
            "NO": 0,
            "YES": 1
        }
    }
    my_data = my_data.replace(mapping_dict)  # 变量映射
    # 绘制相关系数矩阵
    plt.subplots(figsize=(40, 40))
    sns.heatmap(my_data.corr(), annot=True, vmax=1, square=True)
    plt.savefig("./static/png/相关系数.png")
    plt.show()
    # 查看因变量的分布
    x = my_data.drop('Class.ASD', axis=1, inplace=False)
    y = my_data.loc[:, 'Class.ASD']
    my_data['Class.ASD'].value_counts()
    # 数据标准化处理
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    sc_X = sc_X.fit_transform(x)
    X = pd.DataFrame(data=sc_X,
                     columns=['A1_Score', 'A2_Score', 'A3_Score', 'A4_Score', 'A5_Score', 'A6_Score', 'A7_Score',
                              'A8_Score',
                              'A9_Score', 'A10_Score', 'age', 'gender', 'ethnicity', 'jundice', 'austim',
                              'contry_of_res', 'used_app_before', 'result', 'relation'])
    print(X.head())
    return X,y,my_data


'''数据预处理，返回X和y的数据作为之后的训练集'''
def data_process(file='./static/dataset/Aurism_Processing1.csv'):
    my_data = pd.read_csv(file)
    my_data.head(5)
    # 对分类变量进行编码
    mapping_dict = {
        'ethnicity': {
            "Asian": 0,
            "Black": 1,
            "Middle Eastern": 2,
            "South Asian": 3,
            "White-European": 4,
            "Others": 5
        },
        'jundice': {
            "no": 0,
            "yes": 1
        },
        'austim': {
            'no': 0,
            'yes': 1
        },
        'contry_of_res': {
            "Africa": 0,
            "Asia": 1,
            "Europea": 2,
            "North America": 3,
            "Oceania": 4,
            "South America": 5
        },
        'gender': {
            'm': 0,
            'f': 1
        },
        "used_app_before": {
            "no": 0,
            "yes": 1
        },
        "age": {
            "<27": 0,
            ">27": 1
        },
        "relation": {
            "Self": 0,
            "Other": 1
        },
        "Class.ASD": {
            "NO": 0,
            "YES": 1
        }
    }
    my_data = my_data.replace(mapping_dict)  # 变量映射
    # 绘制相关系数矩阵
    plt.subplots(figsize=(40, 40))
    sns.heatmap(my_data.corr(), annot=True, vmax=1, square=True)
    plt.savefig("./static/png/相关系数.png")
    plt.show()
    # 查看因变量的分布
    x = my_data.drop('Class.ASD', axis=1, inplace=False)
    y = my_data.loc[:, 'Class.ASD']
    my_data['Class.ASD'].value_counts()
    # 数据标准化处理
    from sklearn.preprocessing import StandardScaler
    sc_X = StandardScaler()
    sc_X = sc_X.fit_transform(x)
    X = pd.DataFrame(data=sc_X,
                     columns=['A1_Score', 'A2_Score', 'A3_Score', 'A4_Score', 'A5_Score', 'A6_Score', 'A7_Score',
                              'A8_Score',
                              'A9_Score', 'A10_Score', 'age', 'gender', 'ethnicity', 'jundice', 'austim',
                              'contry_of_res', 'used_app_before', 'result', 'relation'])
    print(X.head())
    return X,y,my_data

'''划分训练集'''
def select_data(X,y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1234)
    return X_train, X_test, y_train, y_test

# 定义用于绘制ROC曲线的函数
def plot_roc_curve(fpr, tpr, label=None, titles=None):
    plt.figure(figsize=(12,10))
    plt.plot(fpr, tpr, linewidth=2, label=label)
    plt.plot([0,1],[0,1], "k--")
    plt.axis([0,1,0,1])
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive rate")
    plt.legend(loc="lower right",fontsize=20)
    plt.title(titles, fontsize=20)

# 随机森林建模
def randomF(X_train, X_test, y_train, y_test):

    parameters_rf = {'n_estimators': [100],
                     # 'max_features':range(1,10),
                     'min_samples_split': range(230, 250)}  ##240
    rf = RandomForestClassifier(random_state=1234)
    GS_rf = GridSearchCV(rf, parameters_rf, cv=3)  # cv交叉验证

    # 不进行过采样/欠采样
    GS_rf.fit(X_train, y_train)
    print(GS_rf.best_params_)  # 查看最好的参数选择
    best_rf = GS_rf.best_estimator_
    best_rf.fit(X_train, y_train)
    rf_pred = best_rf.predict(X_test)
    print("精度为：", accuracy_score(y_test, rf_pred))
    print("AUC值为：", roc_auc_score(y_test, rf_pred))
    print("F1值为：", f1_score(y_test, rf_pred, average=None)[0])
    f, axs = plt.subplots(figsize=(12, 10))
    sns.heatmap(confusion_matrix(y_test, rf_pred), annot=True, fmt="d", cmap="Blues", annot_kws={"size": 20})
    axs.set_title("Confusion Matrix", fontsize=24)
    axs.set_xlabel("Predict Labels", fontsize=20)
    axs.set_ylabel("True Labels", fontsize=20)
    plt.show()
    fpr, tpr, thresh = roc_curve(y_test, rf_pred)
    plot_roc_curve(fpr, tpr, label='rf AUC %0.6f' % roc_auc_score(y_test, rf_pred), titles='random_forest')
    plt.savefig('./static/dataset/随机森林.png')
    #return GS_rf.best_estimator_,[accuracy_score(y_test, rf_pred),roc_auc_score(y_test, rf_pred),f1_score(y_test, rf_pred, average=None)[0]]
    return best_rf
def  cart(X_train, X_test, y_train, y_test):
    # CART
    param_cart = {'splitter': ('best', 'random'), 'criterion': ['gini'], "max_depth": np.arange(2, 5),
                  'min_samples_leaf': range(130, 140)}
    cart = DecisionTreeClassifier(random_state=1234)
    GS_cart = GridSearchCV(cart, param_cart, cv=5, scoring='roc_auc')  # 以训练集准确率作为评价指标
    GS_cart.fit(X_train, y_train)
    print(GS_cart.best_params_)  # 查看最好参数选择
    best_cart = GS_cart.best_estimator_  # 选择最好的参数
    best_cart.fit(X_train, y_train)  # 基于最好的参数进行cart建模
    cart_pred = best_cart.predict(X_test)  # 预测
    f, axs = plt.subplots(figsize=(12, 10))
    sns.heatmap(confusion_matrix(y_test, cart_pred), annot=True, fmt="d", cmap="Blues", annot_kws={"size": 20})
    axs.set_title("Confusion Matrix", fontsize=24)
    axs.set_xlabel("Predict Labels", fontsize=20)
    axs.set_ylabel("True Labels", fontsize=20)
    plt.show()
    print("精度为：", accuracy_score(y_test, cart_pred))
    print("AUC值为：", roc_auc_score(y_test, cart_pred))
    print("F1值为：", f1_score(y_test, cart_pred, average=None)[0])
    fpr, tpr, thresh = roc_curve(y_test, cart_pred)
    plot_roc_curve(fpr, tpr, label='CART AUC %0.6f' % roc_auc_score(y_test, cart_pred), titles='CART')
    plt.savefig('./static/dataset/cart.png')
    return GS_cart.best_estimator_,[accuracy_score(y_test, cart_pred),roc_auc_score(y_test, cart_pred),f1_score(y_test, cart_pred, average=None)[0]]

def xgb(X_train, X_test, y_train, y_test):
    cv_params = {'min_child_weight': range(20, 40)}
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 100, 'min_child_weight': 21, 'seed': 0,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    model = xgb.XGBRegressor(**other_params)
    optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='r2', cv=5, verbose=1, n_jobs=4)
    optimized_GBM.fit(X_train, y_train)
    # evalute_result = optimized_GBM.grid_scores_

    means = optimized_GBM.cv_results_['mean_test_score']
    params = optimized_GBM.cv_results_['params']
    for mean, param in zip(means, params):
        print("%f  with:   %r" % (mean, param))

    # print('每轮迭代运行结果:{0}'.format(cv_result))
    print('参数的最佳取值：{0}'.format(optimized_GBM.best_params_))
    print('最佳模型得分:{0}'.format(optimized_GBM.best_score_))

    params = {'min_child_weight': 32}
    xgb_m = xgb.XGBClassifier(**params)
    xgb_m.fit(X_train, y_train)
    Xgboost_pred = xgb_m.predict(X_test)
    # Xgboost_pred = Xgboost_pred.astype(np.int64)

    print("精度为：", accuracy_score(y_test, Xgboost_pred))
    print("AUC值为：", roc_auc_score(y_test, Xgboost_pred))
    print("F1值为：", f1_score(y_test, Xgboost_pred, average=None)[0])
    f, axs = plt.subplots(figsize=(12, 10))

    sns.heatmap(confusion_matrix(y_test, Xgboost_pred), annot=True, fmt="d", cmap="Blues", annot_kws={"size": 20})
    axs.set_title("Confusion Matrix", fontsize=24)
    axs.set_xlabel("Predict Labels", fontsize=20)
    axs.set_ylabel("True Labels", fontsize=20)
    plt.show()
    fpr, tpr, thresh = roc_curve(y_test, Xgboost_pred)
    plot_roc_curve(fpr, tpr, label='rf AUC %0.6f' % roc_auc_score(y_test, Xgboost_pred), titles='Xgboost')
    plt.savefig('./static/dataset/xgb.png')
    return xgb_m.best_estimator_,[accuracy_score(y_test, Xgboost_pred),roc_auc_score(y_test, Xgboost_pred),f1_score(y_test, Xgboost_pred, average=None)[0]]

#特征排序
def feature_sort(X_train, X_test, y_train, y_test,my_data):
    parameters_rf = {'n_estimators': [100],
                     # 'max_features':range(1,10),
                     'min_samples_split': range(230, 250)}  ##240
    rf = RandomForestClassifier(random_state=1234)
    GS_rf = GridSearchCV(rf, parameters_rf, cv=3)  # cv交叉验证

    # 不进行过采样/欠采样
    GS_rf.fit(X_train, y_train)
    print(GS_rf.best_params_)  # 查看最好的参数选择
    best_rf = GS_rf.best_estimator_
    best_rf.fit(X_train, y_train)
    rf_pred = best_rf.predict(X_test)
    print(rf_pred)
    print("精度为:", accuracy_score(y_test, rf_pred))
    print("AUC值为:", roc_auc_score(y_test, rf_pred))
    print("F1值为:", f1_score(y_test, rf_pred, average=None)[0])
    f, axs = plt.subplots(figsize=(12, 10))
    sns.heatmap(confusion_matrix(y_test, rf_pred), annot=True, fmt="d", cmap="Blues", annot_kws={"size": 20})
    axs.set_title("Confusion Matrix", fontsize=24)
    axs.set_xlabel("Predict Labels", fontsize=20)
    axs.set_ylabel("True Labels", fontsize=20)
    plt.show()
    fpr, tpr, thresh = roc_curve(y_test, rf_pred)
    plot_roc_curve(fpr, tpr, label='rf AUC %0.6f' % roc_auc_score(y_test, rf_pred), titles='random_forest')
    importances = best_rf.feature_importances_
    print(len(importances))
    import numpy as np
    indices = np.argsort(importances)[::-1]  # 取反后是从大到小
    feat_labels = my_data.columns[1:]
    for i in range(X_train.shape[1]):
        print("%2d) %-*s %f" % (i + 1, 30, feat_labels[indices[i]], importances[indices[i]]))
    plt.figure(figsize=(12, 10))
    plt.title('Feature Importance')
    plt.bar(range(X_train.shape[1]), importances[indices],
            # color = ['lightskyblue','darkseagreen','lightsalmon','wheat','lightcoral','cornflowerblue','plum','pink'], align='center')
            )
    plt.xticks(range(X_train.shape[1]), feat_labels[indices], rotation=90)
    plt.xlim([-1, X_train.shape[1]])
    plt.tight_layout()
    plt.savefig('./static/png/sort_rf.png')
    plt.show()
