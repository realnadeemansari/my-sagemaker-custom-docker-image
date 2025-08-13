# This is the file that implements a flask server to do inferences. It's the file that you will modify to
# implement the scoring for your own algorithm.

# Import Libraries

# Standard lib
import os
import re

import pickle
import numpy as np
import pandas as pd
import xgboost as xgb
import flask
from flask import json
import joblib


prefix = '/opt/ml/'
model_path = os.path.join(prefix, 'model')

# A singleton for holding the model. This simply loads the model and holds it.
# It has a predict function that does a prediction based on the model and the input data.

class ScoringService(object):
    model = None  # Where we keep the model when it's loaded
    encoder = None
    
    @classmethod
    def get_model(cls):
        # Load Encoder if it is not loaded
        if cls.encoder == None:
            cls.encoder = joblib.load(os.path.join(model_path, "label_encoder.pkl"))
            # with open(os.path.join(model_path, "label_encoder.pkl"), "rb") as inp:
            #     cls.encoder = pickle.load(inp)
        
        # Get the model object for this instance, loading it if it's not already loaded.
        if cls.model == None:
            cls.model = joblib.load(os.path.join(model_path,"xgboost_model.pkl"))
            # with open(os.path.join(model_path,"xgboost_model.pkl"), "rb") as inp:
            #     cls.model = pickle.load(inp)
        
        # Return both models
        return cls.encoder, cls.model

    @classmethod
    def predict(cls, input):
        """For the input, do the predictions and return them.

        Args:
            input (a pandas dataframe): The data on which to do the predictions. There will be
                one prediction per row in the dataframe"""
        encoder, model = cls.get_model()
        prediction = model.predict(input)
        proba = model.predict_proba(input)
        pred_ = encoder.inverse_transform(prediction.astype(np.int32))
        return pred_, proba


# The flask app for serving predictions
app = flask.Flask(__name__)


def preprocess_data(input_object):
    print("INPUT: ", input_object)
    temp_lst = []
    columns = ['PerilTypeID_DRAIN BCK', 'PerilTypeID_HAIL', 'PerilTypeID_OTHER',
       'PerilTypeID_PTFR', 'PerilTypeID_PTTF', 'PerilTypeID_PTWD',
       'PerilTypeID_PTWR', 'PerilTypeID_WIND', 'Status_CSLS', 'Status_CSLT',
       'Status_CSUD', 'Status_NA', 'Levels_LSOF', 'Levels_LSTM', 'Levels_NA',
       'Rooms affected_NA', 'Rooms affected_RAMF', 'Rooms affected_RANA',
       'Rooms affected_RANA,RAOT', 'Rooms affected_RAOT',
       'Rooms affected_RAOT,RATF', 'Rooms affected_RATE',
       'Rooms affected_RATF', 'materials_MDCB', 'materials_MDCE',
       'materials_MDCO', 'materials_MDFI', 'materials_MDFL',
       'materials_MDFL,MDFL', 'materials_MDNA', 'materials_MDWA',
       'materials_MDWA,MDWA', 'materials_NA', 'Engagement_ETAT',
       'Engagement_ETCO', 'Engagement_ETNO', 'Engagement_ETPA',
       'Engagement_NA', 'Engagement_TPNO', 'Engagement_TPPA']

    for object in input_object:
        input_obj_lst = [str(k)+'_'+ str(v) for k, v in object.items()]
        print("input_obj_lst",input_obj_lst)
        for i in range(len(input_obj_lst)):
            if input_obj_lst[i] == 'Status no longer leaking, wet <1 day':
                input_obj_lst[i] = 'Status no longer leaking  wet less than 1 day'
            elif input_obj_lst[i] == 'Status no longer leaking, wet >1 day':
                input_obj_lst[i] = 'Status no longer leaking  wet more than 1 day'
            input_obj_lst[i] = re.sub('[,.()-/]+', ' ', input_obj_lst[i])

        temp_lst.append([1 if col in input_obj_lst else 0 for col in columns])

    df_test = pd.DataFrame(columns=columns, data=np.array(temp_lst), index=None)
    # x_test = xgb.DMatrix(data=df_test)  
    return df_test

@app.route("/ping", methods=["GET"])
def ping():
    """Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully."""
    health = ScoringService.get_model() is not None  # You can insert a health check here

    status = 200 if health else 404
    return flask.Response(response="\n", status=status, mimetype="application/json")


@app.route("/invocations", methods=["POST"])
def transformation():
    """Do an inference on a single batch of data. In this sample server, we take data as JSON, preprocess
    it, make predictions, and return the results along with confidence scores in the specified JSON format.
    """
    data = None

    json_data = flask.request.get_json(force=True)
    preprocessed_data = preprocess_data(json_data)
    # print("pre_processed_data",preprocessed_data)
    
    # # Do the prediction
    # encoder, model = ScoringService.get_model()
    # raw_predictions = model.predict(preprocessed_data, output_margin=True)
    # print("raw_predictions",raw_predictions)
    
    # # Convert raw predictions to probabilities using the sigmoid function
    # confidence_scores = 1 / (1 + np.exp(-raw_predictions))
    # print("confidence_score",confidence_scores)
    
    # # Assuming binary classification, convert to predicted class (0 or 1)
    
    # max_confidence_index = np.argmax(confidence_scores)
    # max_confidence_score = confidence_scores[max_confidence_index]
    # print("max_confidence_score",max_confidence_score)
    # predicted_class = encoder.inverse_transform(np.round(confidence_scores).astype(np.int32).ravel())[max_confidence_index]
    # print("predicted_class",predicted_class)

    # # max_confidence_score_scalar = float(max_confidence_score[0]) if max_confidence_score.size > 0 else None
    # # max_confidence_score_scalar1 = max_confidence_score_scalar * 100
    # max_confidence_score_scalar = float(max_confidence_score[0]) * 100 if max_confidence_score.size > 0 else None
    # print("max_confidence_score_scalar",max_confidence_score_scalar)


    # # Construct the response
    # response = {
    #     "confidenceScore": max_confidence_score_scalar,
    #     "recommendedServiceCode": predicted_class
    # }

    prediction, proba = ScoringService.predict(preprocessed_data)
    response = []
    for pred, prob in zip(prediction, proba):
        response.append({
            "confidenceScore": "{:.2f}".format(max(prob) * 100),
            "recommendedServiceCode": pred
        })
    response_json = json.dumps(response)
    
    return flask.Response(response=response_json, status=200, mimetype="application/json")