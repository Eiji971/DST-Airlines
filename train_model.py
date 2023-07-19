import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import f1_score, classification_report, confusion_matrix, accuracy_score, precision_score, recall_score, f1_score
from joblib import dump, load
from sklearn import metrics
from sklearn.base import clone


# Load the preprocessed data from CSV files
X_train_preprocessed = np.loadtxt('./data/preprocessedcsv/X_train_preprocessed.csv', delimiter=',')
y_train_encoded = np.loadtxt('./data/preprocessedcsv/y_train_encoded.csv', delimiter=',')
X_test_preprocessed = np.loadtxt('./data/preprocessedcsv/X_test_preprocessed.csv', delimiter=',')
y_test_encoded = np.loadtxt('./data/preprocessedcsv/y_test_encoded.csv', delimiter=',')

rf = RandomForestClassifier(random_state=42, n_estimators=300, min_samples_split=2, max_depth=10)
gb = GradientBoostingClassifier(random_state=42, learning_rate=0.05, max_depth=5, max_features='sqrt', min_samples_leaf=1, min_samples_split=2, n_estimators=100, subsample=0.8)

# Fit the base models on the training data
rf.fit(X_train_preprocessed, y_train_encoded)
gb.fit(X_train_preprocessed, y_train_encoded)

# Make predictions on the test data using the base models
y_rf_pred = rf.predict(X_test_preprocessed)
y_gb_pred = gb.predict(X_test_preprocessed)

# Perform model blending
y_blend_pred = np.round((y_rf_pred + y_gb_pred) / 2)

# Print classification report
print(classification_report(y_test_encoded, y_blend_pred))

# Calculate the confusion matrix
cm = confusion_matrix(y_test_encoded, y_blend_pred)

# Convert the confusion matrix array into a DataFrame
cm_df = pd.DataFrame(cm, index=['Class 0', 'Class 1', 'Class 2', 'Class 3'], columns=['Predicted Class 0', 'Predicted Class 1', 'Predicted Class 2', 'Predicted Class 3'])
print(confusion_matrix(y_test_encoded, y_test_encoded))
# Display the confusion matrix DataFrame
print(cm_df)

# Calculate blend accuracy
blend_accuracy = accuracy_score(y_test_encoded, y_blend_pred)

# Calculate blend precision, recall, and F1-score for each class
blend_precision = precision_score(y_test_encoded, y_blend_pred, average=None)
blend_recall = recall_score(y_test_encoded, y_blend_pred, average=None)
blend_f1_score = f1_score(y_test_encoded, y_blend_pred, average=None)

# Print blend scores and metrics
print("Blend Accuracy:", blend_accuracy)
print("Blend Precision:", blend_precision)
print("Blend Recall:", blend_recall)
print("Blend F1-Score:", blend_f1_score)

print("Score train set - Random Forest:", rf.score(X_train_preprocessed, y_train_encoded))
print("Score test set - Random Forest:", rf.score(X_test_preprocessed, y_test_encoded))

print("Score train set - Gradient Boosting:", gb.score(X_train_preprocessed, y_train_encoded))
print("Score test set - Gradient Boosting:", gb.score(X_test_preprocessed, y_test_encoded))

# Save the trained models to pickle files
dump(gb, './models/gradient_boost_model.pkl')
dump(rf, './models/random_forest_model.pkl')