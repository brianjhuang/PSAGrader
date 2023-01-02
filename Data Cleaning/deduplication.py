import cv2
import pytesseract
import numpy as np
import os
import pandas as pd
import hashlib

def image_vector(dir, grayscale=False, denoise=False):
    image = cv2.imread(dir)
    if grayscale:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    if denoise:
        threshold, binary = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV)
        kernel = np.ones((1,1), np.uint8)
        image = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=1)
    return image

def hash_image_vector(image_vector):
    # Convert the image vector to a string
    image_string = image_vector.tobytes()
    # Calculate the hash of the image string
    image_hash = hashlib.sha256(image_string).hexdigest()
    return image_hash

def deduplicate(path_dir):
    images_set = set()
    dataset = []

    for name in os.listdir(path_dir):
        row = {}

        image = image_vector(os.path.join(path_dir, name))

        hashed = hash_image_vector(image)

        row['filename'] = name
        row['hash'] = hashed
      
        if hashed in images_set:
            row['is_duplicate'] = True
            os.remove(os.path.join(path_dir, name))
        else:    
            row['is_duplicate'] = False
            images_set.add(hashed)

        dataset.append(row)

    df = pd.DataFrame(dataset)
    df.to_csv('deduplicated_log.csv')
    
    return "Complete"



path_dir = ''

deduplicate(path_dir)
