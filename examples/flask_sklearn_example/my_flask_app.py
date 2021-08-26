import datetime
import io
import json

import torchvision.transforms as transforms
from flask import Flask, jsonify, request
from PIL import Image
from smart_open import open
from torchvision import models

from whylogs import get_or_create_session

session = get_or_create_session()
logger = session.logger(dataset_name="my_deployed_model",
                        dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), 
                        with_rotation_time="5s")

app = Flask(__name__)




def get_prediction(image_bytes):
    tensor = transform_image(image_bytes=image_bytes)

    logger.log({"batch_size": tensor.shape[0]})
    outputs = model.forward(tensor)

    conf, y_hat = outputs.max(1)
    logger.log({"confidence": conf.item()})
    predicted_idx = str(y_hat.item())
    return imagenet_class_index[predicted_idx]


@app.route("/predict", methods=["POST"])
def predict():

    if request.method == "POST":
        request_query = request.json
        
        logger.log(request_query)
        
        output = get_prediction(image_bytes=img_bytes)

        logger.log({"output": output})

        return jsonify({"output": output})


if __name__ == "__main__":
    app.run(debug=True)
