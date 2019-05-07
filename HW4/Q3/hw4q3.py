## Data and Visual Analytics - Homework 4
## Georgia Institute of Technology
## Applying ML algorithms to detect eye state

import numpy as np
import pandas as pd
import time

from sklearn.model_selection import cross_val_score, GridSearchCV, cross_validate, train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sklearn.svm import SVC
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, normalize
from sklearn.decomposition import PCA

######################################### Reading and Splitting the Data ###############################################
# XXX
# TODO: Read in all the data. Replace the 'xxx' with the path to the data set.
# XXX
data = pd.read_csv('eeg_dataset.csv')

# Separate out the x_data and y_data.
x_data = data.loc[:, data.columns != "y"]
y_data = data.loc[:, "y"]

# The random state to use while splitting the data.
random_state = 100

# XXX
# TODO: Split 70% of the data into training and 30% into test sets. Call them x_train, x_test, y_train and y_test.
# Use the train_test_split method in sklearn with the parameter 'shuffle' set to true and the 'random_state' set to 100.
# XXX

x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, test_size=0.30, shuffle=True, random_state=100)

# ############################################### Linear Regression ###################################################
# XXX
# TODO: Create a LinearRegression classifier and train it.
# XXX
lr_model = LinearRegression()
lr_model.fit(x_train, y_train)

# XXX
# TODO: Test its accuracy (on the training set) using the accuracy_score method.
# TODO: Test its accuracy (on the testing set) using the accuracy_score method.
# Note: Round the output values greater than or equal to 0.5 to 1 and those less than 0.5 to 0. You can use y_predict.round() or any other method.
# XXX
y_train_pred = lr_model.predict(x_train)
y_test_pred = lr_model.predict(x_test)
train_score = accuracy_score(y_true=y_train, y_pred=y_train_pred.round())
test_score = accuracy_score(y_true=y_test, y_pred=y_test_pred.round())

# ############################################### Random Forest Classifier ##############################################
# XXX
# TODO: Create a RandomForestClassifier and train it.
# XXX
rfc_model=RandomForestClassifier()
rfc_model.fit(x_train, y_train)

# XXX
# TODO: Test its accuracy on the training set using the accuracy_score method.
# TODO: Test its accuracy on the test set using the accuracy_score method.
# XXX
y_train_pred = rfc_model.predict(x_train)
y_test_pred = rfc_model.predict(x_test)
train_score = accuracy_score(y_true=y_train, y_pred=y_train_pred)
test_score = accuracy_score(y_true=y_test, y_pred=y_test_pred)

# XXX
# TODO: Determine the feature importance as evaluated by the Random Forest Classifier.
#       Sort them in the descending order and print the feature numbers. Then report the most important and the least important feature.
#       Mention the features with the exact names, e.g. X11, X1, etc.
#       Hint: There is a direct function available in sklearn to achieve this. Also checkout argsort() function in Python.
# XXX
feature_importance = rfc_model.feature_importances_
a=np.argsort(feature_importance)
print("feature importance in descending order (attributes): ", a[::-1] + 1)
print("feature importance in descending order: ",feature_importance[a][::-1])
# [ 7  6  2 13 14  1 12  4  8 11 10  3  5  9]

# XXX
# TODO: Tune the hyper-parameters 'n_estimators' and 'max_depth'.
#       Print the best params, using .best_params_, and print the best score, using .best_score_.
# XXX

hyper_params = {'n_estimators':[120, 140, 160, 180], 'max_depth': [10, 15, 20, 25, 30]}
grid_rfc = GridSearchCV(rfc_model, hyper_params, cv=10)
grid_rfc.fit(x_train, y_train)
print('best params', grid_rfc.best_params_, 'best score', grid_rfc.best_score_)

tuned_rfc_model=RandomForestClassifier(n_estimators=140, max_depth=30)
tuned_rfc_model.fit(x_train, y_train)

y_train_pred = tuned_rfc_model.predict(x_train)
y_test_pred = tuned_rfc_model.predict(x_test)
train_score = accuracy_score(y_true=y_train, y_pred=y_train_pred)
test_score = accuracy_score(y_true=y_test, y_pred=y_test_pred)

# ############################################ Support Vector Machine ###################################################
# XXX
# TODO: Pre-process the data to standardize or normalize it, otherwise the grid search will take much longer
scaler = StandardScaler()
scaler.fit(x_train)
x_train_norm = scaler.transform(x_train)
x_test_norm = scaler.transform(x_test)

# TODO: Create a SVC classifier and train it.
# XXX
svc_model = SVC()
svc_model.fit(x_train_norm, y_train)
#svc_model.fit(x_train, y_train)

# XXX
# TODO: Test its accuracy on the training set using the accuracy_score method.
# TODO: Test its accuracy on the test set using the accuracy_score method.
# XXX

y_train_pred = svc_model.predict(x_train_norm)
y_test_pred = svc_model.predict(x_test_norm)
train_score = accuracy_score(y_true=y_train, y_pred=y_train_pred)
test_score = accuracy_score(y_true=y_test, y_pred=y_test_pred)

# XXX
# TODO: Tune the hyper-parameters 'C' and 'kernel' (use rbf and linear).
#       Print the best params, using .best_params_, and print the best score, using .best_score_.
# XXX

hyper_params = {'C': [0.01, 0.1, 1, 10, 100], 'kernel': ['linear', 'rbf']}
gridsvc_model = GridSearchCV(svc_model, hyper_params, cv=10)
gridsvc_model.fit(x_train_norm, y_train)
print('best params', gridsvc_model.best_params_,
      'best score', gridsvc_model.best_score_)

best_index = gridsvc_model.cv_results_['mean_test_score'].argmax()
print(gridsvc_model.cv_results_['mean_test_score'][best_index])
print(gridsvc_model.cv_results_['mean_train_score'][best_index])
print(gridsvc_model.cv_results_['mean_fit_time'][best_index])
# 0.8194735838260537
# 0.8319981542760353
# 5.442560386657715 s

tuned_svc_model = SVC(C=100, kernel='rbf')
tuned_svc_model.fit(x_train_norm, y_train)

y_train_pred = tuned_svc_model.predict(x_train_norm)
y_test_pred = tuned_svc_model.predict(x_test_norm)
train_score = accuracy_score(y_true=y_train, y_pred=y_train_pred)
test_score = accuracy_score(y_true=y_test, y_pred=y_test_pred)

# ######################################### Principal Component Analysis #################################################
# XXX
# TODO: Perform dimensionality reduction of the data using PCA.
#       Set parameters n_component to 10 and svd_solver to 'full'. Keep other parameters at their default value.
#       Print the following arrays:
#       - Percentage of variance explained by each of the selected components
#       - The singular values corresponding to each of the selected components.
# XXX

pca_model = PCA(n_components=10, svd_solver='full')
pca_model.fit(x_data, y_data)
print(pca_model.explained_variance_ratio_)
print(pca_model.singular_values_)
# [5.05244700e-01 3.76936309e-01 1.17729460e-01 4.59941145e-05
# 1.92788922e-05 1.12325809e-05 6.78642894e-06 1.88971137e-06
# 1.54088292e-06 8.24289754e-07]
#[886690.55021511 765870.22149031 428019.7135883    8460.03827621
#   5477.2458465    4180.81523164   3249.68937137   1714.82156063
#   1548.48148676   1132.55981354]
