import datetime

import pandas as pd
import numpy as np

from sklearn import metrics
from descriptors import cachedproperty

def generate_binary_at_x(test_predictions, x_value, unit="top_k"):
    """Assign predicted classes based based on top% or absolute rank of score
    Args:
        test_predictions (np.array) A predictions, sorted by risk score descending
        x_value (int) The percentile or absolute value desired
        unit (string, default 'top_k') The thresholding method desired,
            either percentile or top_k
    Returns: (np.array) The predicted classes
    """
    len_predictions = len(test_predictions)
    if len_predictions == 0:
        return np.array([])
    if unit == "percentile":
        cutoff_index = int(len_predictions * (x_value / 100.00))
    else:
        cutoff_index = int(x_value)
    num_ones = cutoff_index if cutoff_index <= len_predictions else len_predictions
    num_zeroes = len_predictions - cutoff_index if cutoff_index <= len_predictions else 0
    test_predictions_binary = np.concatenate(
        (np.ones(num_ones, np.int8), np.zeros(num_zeroes, np.int8))
    )
    return test_predictions_binary


class Evaluations(object):
    def __init__(self, test_matrix, label_column='status', model=None, model_id=None):
        self.matrix = test_matrix
        self.matrix_size = test_matrix.shape
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

    def precision_at_topk(self, k):
        # if '%' in str(k):
        #     unit = 'percentile'
        #     k = int(k.split('%')[0])
        # elif k == 'all':
        #     unit = 'top_k'
        #     k = self.matrix_size[0]
        # else:
        #     unit = 'top_k'
        #     k = int(k)
        # predicted_classes = generate_binary_at_x(self.evaluated_matrix['score'].values, k, unit)
        # return  predicted_classes.sum() / k
        ranked = self.evaluated_matrix.sort_values(by='score', ascending=False)
        return ranked[:k]['label'].sum() / k

    def recall_at_topk(self, k):
        # if '%' in str(k):
        #     unit = 'percentile'
        #     k = int(k.split('%')[0])
        # elif k == 'all':
        #     unit = 'top_k'
        #     k = self.matrix_size[0]
        # else:
        #     unit = 'top_k'
        #     k = int(k)
        # predicted_classes = generate_binary_at_x(self.evaluated_matrix['score'].values, k, unit)
        # return predicted_classes.sum() / self.evaluated_matrix['label'].sum()
        ranked = self.evaluated_matrix.sort_values(by='score', ascending=False)
        return ranked[:k]['label'].sum() / ranked['label'].sum()

    def roc_auc_score(self):
        return metrics.roc_auc_score(self.evaluated_matrix['label'], self.evaluated_matrix['score'])

    def average_precision_score(self):
        return metrics.average_precision_score(self.evaluated_matrix['label'], self.evaluated_matrix['score'])

    def f1(self):
        pass

