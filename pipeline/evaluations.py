import datetime

import pandas as pd

from sklearn import metrics
from descriptors import cachedproperty


class Evaluations(object):
    def __init__(self, test_matrix, label_column='status', model=None, model_id=None):
        self.matrix = test_matrix
        self.model = model
        self.model_id = model_id
        self.label_column = label_column
        self.predictions = test_matrix[[label_column]]
        self.predictions['score'] = self.model.predict_proba(self.matrix.drop(self.label_column, axis=1, inplace=False))[:, 1]

    @cachedproperty
    def evaluated_matrix(self):
        evaluated = self.matrix.copy()
        evaluated['score'] = self.predictions['score']
        evaluated['entity_id'] = evaluated.index
        evaluated['model_id'] = [self.model_id]*len(evaluated)
        evaluated['rank_abs'] = evaluated['score'].rank(ascending=False, method='first')
        evaluated['rank_pct'] = evaluated['score'].rank(ascending=False, pct=True)
        evaluated['label'] = evaluated[self.label_column].apply(lambda x: True if x == 1 else False)
        return evaluated

    @cachedproperty
    def feature_importance(self):
        try:
            feature_importances = self.model.feature_importances_
        except AttributeError:
            try:
                feature_importances = self.model.coef_[0]
            except AttributeError:
                feature_importances = None
        feature_names = self.matrix.drop([self.label_column], axis=1, inplace=False).columns
        importance = pd.DataFrame({
            "feature": feature_names,
            "importance": feature_importances
        })
        return importance.sort_values(by='importance', ascending=False)

    def topK(self, k):
        return self.matrix[self.evaluated_matrix['rank_abs'] <= k]

    def precision_at_top(self, k):
        ranked = self.predictions.sort_values(by='score', ascending=False)
        return ranked[:k][self.label_column].sum() / k

    def recall_at_top(self, k):
        ranked = self.predictions.sort_values(by='score', ascending=False)
        return ranked[:k][self.label_column].sum() / ranked[self.label_column].sum()

    def compute_AUC(self):
        '''
        Utility function to generate ROC and AUC data to plot ROC curve
        Returns (tuple):
            - (false positive rate, true positive rate, thresholds, AUC)
        '''

        label_ = self.predictions[self.label_column]
        score_ = self.predictions['socre']
        fpr, tpr, thresholds = metrics.roc_curve(
            label_, score_, pos_label=1)

        return (fpr, tpr, thresholds, metrics.auc(fpr, tpr))


