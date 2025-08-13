import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
import numpy as np
import matplotlib.pylab as plt
from sklearn.preprocessing import LabelEncoder
import argparse
import os
import joblib
import json
import re

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-data-dir', type=str, default=os.environ['SM_OUTPUT_DATA_DIR'])
    parser.add_argument('--model-dir', type=str, default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train', type=str, default=os.environ['SM_CHANNEL_TRAIN'])
    
    args = parser.parse_args()
    
    file = os.path.join(args.train, 'complete_wrangled_data.csv')
    df = pd.read_csv(file)
    train = df[['Status', 'Levels', 'Rooms', 'Materials', 'Engagement', 'source']]
    train_df = pd.get_dummies(train)
    train_df = train_df.rename(columns = {'Status_no longer leaking, wet <1 day': 'Status_no longer leaking, wet less than 1 day',
                                     'Status_no longer leaking, wet >1 day': 'Status_no longer leaking, wet more than 1 day'})
    train_df.columns=train_df.columns.str.replace(r'[^0-9a-zA-Z ]', ' ', regex=True)
    
    le = LabelEncoder()
    y = le.fit_transform(df[['labels']])
    X_train, X_test, y_train, y_test = train_test_split(train_df, y, random_state=42, stratify=y)
    
    dtrain = xgb.DMatrix(data=X_train, label=y_train)
    dtest = xgb.DMatrix(data=X_test)
    
    params = {
        'max_depth': 3,
        'objective': 'multi:softmax',  # error evaluation for multiclass training
        'num_class': 5,
        'n_gpus': 0
    }
    bst = xgb.train(params, dtrain)
    joblib.dump(bst, os.path.join(args.model_dir, 'model.joblib'))
    joblib.dump(le, os.path.join(args.model_dir, 'encoder.joblib'))
    
def model_fn(model_dir):
    model = joblib.load(os.path.join(model_dir, 'model.joblib'))
    return model

def input_fn(request_body, request_content_type):
    try:
        return json.loads(request_body)
    except Exception as e:
        print("Error in input_fn:", str(e))
        
def predict_fn(input_object, model, model_dir='/opt/ml/model'):
#     try:
    print("INPUT:", input_object)
    temp_lst = []
    columns = ['Status active leak  currently wet', 
               'Status livable still in dwelling   building', 
               'Status livable temporarily displaced', 
               'Status no longer leaking  wet less than 1 day', 
               'Status no longer leaking  wet more than 1 day', 
               'Status no  no opening', 
               'Status unlivable displaced', 
               'Status yes  covered tarped', 
               'Status yes  uncovered not tarped', 
               'Levels 1 floor', 
               'Levels 2 floors or more', 
               'Levels no', 
               'Rooms 1 2 rooms', 
               'Rooms 3 rooms', 
               'Rooms 4 rooms', 
               'Rooms 5 or more rooms', 
               'Rooms no', 
               'Materials cabinets', 
               'Materials ceiling', 
               'Materials contents  personal belongings ', 
               'Materials exterior contents  grill  patio furniture  etc  ', 
               'Materials exterior structure', 
               'Materials fixtures', 
               'Materials floors', 
               'Materials hardscapes   fence', 
               'Materials no', 
               'Materials roof', 
               'Materials siding', 
               'Materials vinyl floor covering', 
               'Materials walls', 
               'Materials windows', 
               'Engagement attorney', 
               'Engagement contractor', 
               'Engagement no', 
               'Engagement public adjuster', 
               'source electrical', 
               'source fireplace', 
               'source lightning', 
               'source other', 
               'source stove', 
               'source unknown', 
               'source water', 
               'source wildfire', 
               'source wind']
        
    input_obj_lst = [k+' '+v for k, v in input_object.items()]
    for i in range(len(input_obj_lst)):
        if input_obj_lst[i] == 'Status no longer leaking, wet <1 day':
            input_obj_lst[i] = 'Status no longer leaking  wet less than 1 day'
        elif input_obj_lst[i] == 'Status no longer leaking, wet >1 day':
            input_obj_lst[i] = 'Status no longer leaking  wet more than 1 day'
        input_obj_lst[i] = re.sub('[,.()-/]+', ' ', input_obj_lst[i])
    
    for col in columns:
        if col in input_obj_lst:
            temp_lst.append(1)
        else:
            temp_lst.append(0)
    df_test = pd.DataFrame(columns=columns, data=np.array(temp_lst).reshape(1, 44), index=None)
    x_test = xgb.DMatrix(data=df_test)
    pred = model.predict(x_test)
    encoder = joblib.load(os.path.join(model_dir, "encoder.joblib"))
    pred_ = encoder.inverse_transform(pred.astype(np.int32))
    res = {'service': pred_[0]}
    return res
#     except Exception as e:
#         print("ERROR:", str(e))
        
def output_fn(prediction, content_type):
    if content_type == "application/json":
        print("OUTPUT: ", prediction)
        return prediction
    else:
        print("Error in output_fn")
        