import pandas as pd
from transformers import pipeline

#loading the exisiting models
arabic_model = pipeline("sentiment-analysis", model="CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment")
english_model = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")

df = pd.read_csv("manual_label_sample.csv")

#function to classify by language
def classify(row):
    text = row["text"]
    lang = row["language"]
    if not isinstance(text, str) or len(text.strip()) < 30:
        return "neutral"
    try:
        if lang == "ar":
            result = arabic_model(text[:512])[0]
        else:
            result = english_model(text[:512])[0]
        return result["label"].lower()
    except:
        return "neutral"

df["label"] = df.apply(classify, axis=1)

#save the new file with suggested labels
df.to_csv("manual_label_auto.csv", index=False, encoding="utf-8")
print("✅ manual_label_auto.csv created with suggested labels.")
