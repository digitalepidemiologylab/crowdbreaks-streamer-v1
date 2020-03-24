import logging
from app.utils.process_text import preprocess
from app.ml.sagemaker import Sagemaker
from helpers import report_error
import json

logger = logging.getLogger(__name__)


class Predict:
    def __init__(self, endpoint_name, model_type):
        self.model_type = model_type
        self.endpoint_name = endpoint_name
        self.batch_size = self.get_batch_size(model_type)
        self.sagemaker = Sagemaker()

    def predict(self, texts):
        # run prediction in batches
        num_docs = len(texts)
        output = []
        can_convert_label_to_int = True
        for i in range(0, num_docs, self.batch_size):
            texts_slice = texts[i:(i+self.batch_size)]
            texts_slice = self.preprocess_text(texts_slice)
            resp = self.sagemaker.predict(self.endpoint_name,  {'text': texts})
            status_code = resp['ResponseMetadata']['HTTPStatusCode']
            if status_code != 200:
                report_error(logger, msg='Prediction on endpoint {self.endpoint_name} unsuccessful.')
            preds = json.loads(resp['Body'].read())['predictions']
            _output = [{
                'labels': _pred['labels'],
                'probabilities': _pred['probabilities']} for pred_obj, _pred in zip(texts_slice, preds)]
            output.extend(_output)
        label_vals = [self.labels_to_int(_output['labels']) for _output in output]
        if all(label_vals):
            output = [{'label_vals': _label_vals, **_output} for _output, _label_vals in zip(output, label_vals)]
        return output

    @staticmethod
    def labels_to_int(labels):
        """Heuristic to convert label to numeric value. Parses leading numbers in label tags such as 1_worried -> 1.
        If any conversion fails this function will return None
        """
        label_vals = []
        for label in labels:
            if label == 'positive':
                label_vals.append(1)
            elif label == 'negative':
                label_vals.append(-1)
            elif label == 'neutral':
                label_vals.append(0)
            else:
                label_split = label.split('_')
                try:
                    label_val = int(label_split[0])
                except ValueError:
                    return
                else:
                    label_vals.append(label_val)
        return label_vals

    def preprocess_text(self, texts):
        """Preprocess text for prediction """
        if self.model_type == 'fasttext':
            texts = [preprocess(
                t,
                min_num_tokens=0,
                lower_case=True,
                remove_punct=True,
                remove_accents=False,
                expand_contractions=False,
                lemmatize=True,
                remove_stop_words=False,
                replace_user_tags_with='user',
                replace_url_tags_with='url') for t in texts]
        else:
            logger.info(f'No tokenization applied for model type {self.model_type}')
        return texts

    def get_batch_size(self, model_type):
        if model_type == 'fasttext':
            return 100
        logger.warning(f'Model type {model_type} unknown. Using default batch size.')
        return 1
