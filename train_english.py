import pandas as pd
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer
import evaluate

# إعدادات أساسية
DATA_FILE = "autoLabeled_verified.csv"
#MODEL_NAME = "distilbert-base-uncased"   # موديل إنجليزي بسيط وفعال
MODEL_NAME = "bert-base-uncased"
OUTPUT_DIR = "./english_model"

# تحويل الـ labels لأرقام
label2id = {"positive": 0, "negative": 1, "neutral": 2}
id2label = {v: k for k, v in label2id.items()}

# قراءة البيانات الإنجليزية
df = pd.read_csv(DATA_FILE)
df_en = df[df["language"] == "en"][["text", "label"]].dropna()
df_en["label"] = df_en["label"].str.lower().map(label2id)
df_en = df_en.dropna(subset=["label"])

# تحويل لــ Dataset وتقسيم Train/Test
dataset = Dataset.from_pandas(df_en)
dataset = dataset.train_test_split(test_size=0.2, seed=42)

# تحميل tokenizer والموديل
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME, num_labels=3, id2label=id2label, label2id=label2id
)

# Tokenization
def tokenize(batch):
    return tokenizer(batch["text"], truncation=True, padding="max_length", max_length=128)

dataset = dataset.map(tokenize, batched=True)

# Metrics
accuracy = evaluate.load("accuracy")
f1 = evaluate.load("f1")

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = logits.argmax(-1)
    return {
        "accuracy": accuracy.compute(predictions=preds, references=labels)["accuracy"],
        "macro_f1": f1.compute(predictions=preds, references=labels, average="macro")["f1"]
    }

# إعدادات التدريب
args = TrainingArguments(
    output_dir=OUTPUT_DIR,
    learning_rate=2e-5,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    num_train_epochs=5, #3,
    weight_decay=0.01,
    logging_dir="./logs",
    logging_steps=50,
    report_to="none"
)

# Trainer
trainer = Trainer(
    model=model,
    args=args,
    train_dataset=dataset["train"],
    eval_dataset=dataset["test"],
    tokenizer=tokenizer,
    compute_metrics=compute_metrics
)

# تدريب وحفظ
trainer.train()
trainer.save_model(OUTPUT_DIR)
print("✅ English model trained and saved to", OUTPUT_DIR)
