import pandas as pd

''' Prepare a balanced sample for manual labeling from cleaned merged news data '''
'''
هذا السكربت:

يقرأ merged_news_clean.csv.

يعمل عينة متوازنة حسب اللغة (25 عربي + 25 إنجليزي).

يحافظ على الأعمدة المهمة ويضيف عمود label فاضي.

يحفظ النتيجة في manual_label_sample.csv.
'''
INPUT_FILE = "merged_news_clean.csv"
OUTPUT_FILE = "manual_label_sample.csv"
# N_TOTAL = 200 #too much (no data available)
N_TOTAL = 50  # total rows in the sample
N_PER_LANG = N_TOTAL // 2  # 100 ar + 100 en

# Read cleaned merged file
df = pd.read_csv(INPUT_FILE)

# Basic validations
required_cols = {"source", "language", "category", "text"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Remove empty or very short texts (safety)
df = df.dropna(subset=["text"])
df = df[df["text"].str.len() > 30].copy()

# Balance by language
df_ar = df[df["language"] == "ar"]
df_en = df[df["language"] == "en"]

if len(df_ar) < N_PER_LANG or len(df_en) < N_PER_LANG:
    raise ValueError(
        f"Not enough rows per language. ar={len(df_ar)}, en={len(df_en)}. "
        f"Need at least {N_PER_LANG} per language."
    )

sample_ar = df_ar.sample(N_PER_LANG, random_state=42)
sample_en = df_en.sample(N_PER_LANG, random_state=42)

sample = pd.concat([sample_ar, sample_en], ignore_index=True)

# Keep only essential columns and add empty label
sample = sample[["source", "language", "category", "text"]].copy()
sample.insert(len(sample.columns), "label", "")  # empty label column

# Optional: create a simple id
sample.insert(0, "id", range(1, len(sample) + 1))

# Save
sample.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"✅ {OUTPUT_FILE} created — rows: {len(sample)}")
print("Please open and label: positive / negative / neutral")
