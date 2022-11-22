import analysis



def main():
    #X,y,my_data = analysis.data_process()
    #X_train, X_test, y_train, y_test = analysis.train_test_split(X,y)
    #analysis.randomF(X_train, X_test, y_train, y_test)
    #analysis.cart(X_train, X_test, y_train, y_test)
    #analysis.xgb(X_train, X_test, y_train, y_test)
    #analysis.feature_sort(X_train, X_test, y_train, y_test)
    #best_model = analysis.randomF(X_train, X_test, y_train, y_test)
    new_X, new_y, new_data = analysis.data_process('./static/dataset/xxx.csv')

if __name__ == '__main__':
    main()