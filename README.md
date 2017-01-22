# WSDM Cup 2017: Vandalism Detection 

- Challenge webpage: http://www.wsdm-cup-2017.org/vandalism-detection.html
- The goal of the competition is to predict whether a wikidata edit will be rolled back or not
- Slides with the solution description: http://www.slideshare.net/AlexeyGrigorev/wsdm-cup-2017-vandalism-detection

Running the solution: 

- download all the data, unpack, and put the files to the `data` folder, 
- run `01_xml_to_csv.py` for converting the wikidata dump files into a bunch of csv files
- run `02_join_data.py` to join the data from the xml files with the meta information and labels 
- `03_extract_features.py` processes the data so it can be used for the model. This includes
    - specifying the training, validation and testing folds
    - processing the information about the users (including the meta information) 
    - extracting useful features from the comments
- `04_train_svm.py` creates two models:
    - vectorizer for creating a large one-hot-encoding matrix for all the string features
    - a linear SVM model with L1 penalty for performing the classification
- `tira_client.py` is used for running the model on http://tira.io/

