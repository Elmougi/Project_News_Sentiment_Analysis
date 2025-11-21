import pandas as pd
import re

# دالة لتطبيع النصوص العربية
def normalize_arabic(text):
    if not isinstance(text, str):
        return ""
    # إزالة التشكيل
    text = re.sub(r'[\u0617-\u061A\u064B-\u0652]', '', text)
    # توحيد الألف
    text = re.sub(r'[إأآا]', 'ا', text)
    # توحيد الياء
    text = re.sub(r'ى', 'ي', text)
    # توحيد الهاء/ة
    text = re.sub(r'ة', 'ه', text)
    # إزالة أي رموز أو أرقام غير ضرورية
    text = re.sub(r'[^\w\s]', ' ', text)
    return text.strip()


df = pd.read_csv("merged_news.csv")

# remove empty or very short texts
df = df.dropna(subset=["text"])
df = df[df["text"].str.len() > 30]

#drop duplicate texts
df = df.drop_duplicates(subset=["text"])

# راجع التصنيفات
df["category"] = df["category"].fillna("Uncategorized")

# طبّع النصوص العربية فقط
df.loc[df["language"] == "ar", "text"] = df.loc[df["language"] == "ar", "text"].apply(normalize_arabic)

#save the cleaned merged file
df.to_csv("merged_news_clean.csv", index=False, encoding="utf-8")

print(" Done! merged_news_clean.csv created — rows:", len(df))
