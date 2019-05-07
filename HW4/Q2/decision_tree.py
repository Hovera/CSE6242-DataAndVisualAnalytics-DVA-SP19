from util import entropy, information_gain, partition_classes
import numpy as np
import ast
from collections import Counter

class DecisionTree(object):
    def __init__(self):
        # Initializing the tree as an empty dictionary or list, as preferred
        #self.tree = []
        self.tree = {}

    def learn(self, X, y):
        # TODO: Train the decision tree (self.tree) using the the sample X and labels y
        # You will have to make use of the functions in utils.py to train the tree

        # One possible way of implementing the tree:
        #    Each node in self.tree could be in the form of a dictionary:
        #       https://docs.python.org/2/library/stdtypes.html#mapping-types-dict
        #    For example, a non-leaf node with two children can have a 'left' key and  a
        #    'right' key. You can add more keys which might help in classification
        #    (eg. split attribute and split value)
        self.depth = 0
        self.group = None

        y_entropy = entropy(y)

        max_info_gain = -1
        split_attribute = -1
        split_val = ''
        x_left = []
        x_right = []
        y_left = []
        y_right = []

        if self.depth < 15 and y_entropy > 0:
            for column in range(len(X[0])):
                col_vals = [row[column] for row in X]
                trial_split_val = sum(col_vals) / (len(col_vals) * 1.0)
                x_l, x_r, y_l, y_r = partition_classes(X, y, column, trial_split_val)
                current_y = [y_l, y_r]
                info_gain = information_gain(y, current_y)
                if info_gain > max_info_gain:
                    max_info_gain = info_gain
                    split_attribute = column
                    split_val = trial_split_val
                    x_left = x_l
                    x_right = x_r
                    y_left = y_l
                    y_right = y_r

            self.tree['left'] = DecisionTree()
            self.tree['right'] = DecisionTree()
            self.tree['split_attribute'] = split_attribute
            self.tree['split_val'] = split_val
            self.tree['left'].learn(x_left, y_left)  # create tree within tree
            self.tree['right'].learn(x_right, y_right)
            self.tree['left'].depth = self.depth + 1
            self.tree['right'].depth = self.depth + 1

        else:
            self.group = y[0]
            return


    def classify(self, record):
        # TODO: classify the record using self.tree and return the predicted label
        node = self.tree
        while self.group == None:
            split_val = node['split_val']
            split_attribute = node['split_attribute']

            if isinstance(record[split_attribute], str):
                if record[split_attribute] == split_val:
                    label = node['left'].classify(record)
                else:
                    label = node['right'].classify(record)
            else:
                if record[split_attribute] <= split_val:
                    label = node['left'].classify(record)
                else:
                    label = node['right'].classify(record)
            return label

        label = self.group
        return label
