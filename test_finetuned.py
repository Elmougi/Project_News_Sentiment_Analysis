from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

def load_and_predict(model_dir, text):
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=128)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=-1)[0].tolist()
    pred_id = int(logits.argmax(-1))
    id2label = model.config.id2label
    return id2label[pred_id], probs[pred_id]

if __name__ == "__main__":
    print("Arabic:", load_and_predict("./arabic_model", "أنا متفائلة جدًا بالمستقبل"))
    print("English:", load_and_predict("./english_model", "The market is collapsing"))
    print("Arabic:", load_and_predict("./arabic_model", "الوضع سيء جدًا اليومل"))
    print("English:", load_and_predict("./english_model", "I am very happy with the results"))
    #some news articles
    print("Arabic: ", load_and_predict("./arabic_model", "تشهد البلاد تحسنًا ملحوظًا في الوضع الاقتصادي بعد الإجراءات الجديدة."))
    print("English:", load_and_predict("./english_model", 
    "Financial markets meltdown fears spread as US interest rate cut hopes fade"))
    print("English:", load_and_predict("./english_model", 
    "US official says Iran seized oil tanker in Strait of Hormuz"))
    print("English:", load_and_predict("./english_model", 
    "US warns of real human cost if UN Security Council doesn’t back its Gaza resolution"))
    print("English:", load_and_predict("./english_model", 
    "Jensen Huang says his mom taught him English without speaking it"))

