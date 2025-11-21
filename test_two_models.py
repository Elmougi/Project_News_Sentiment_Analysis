#الملف ده مجرد أداة اختبار سريعة للموديلات الجاهزة

from transformers import pipeline #from the library transformers we bring HuggingFace Transformers
#الـ pipeline دي بتخلينا نستخدم موديلات جاهزة بسهولة من غير ما نكتب كود معقد.

print("⏳ Loading Arabic Model...")
arabic_model = pipeline(
    "sentiment-analysis",
    #model="akhooli/bert-base-arabic-sentiment" #didn't work well
    #?model="asafaya/bert-base-arabic-sentiment"
    model="CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment"  #مخصوص لتحليل المشاعر بالعربي باستخدام موديل جاهز HuggingFace
    #try this one if the above doesn't work: model="aubmindlab/bert-base-arabic-ner"
    #model="CAMeL-Lab/bert-base-arabic-camelbert-da-sentiment"
)

print("⏳ Loading English Model...")
english_model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest" #نفس الفكرة بس للموديل الإنجليزي (RoBERTa مدرّب على تويتر).
)

while True:
    text = input("🔵 اكتب جملة بالعربي أو الإنجليزي ('exit' للخروج): ")
    if text.lower() == "exit":
        break

    # لو النص فيه حروف عربية
    if any("\u0600" <= ch <= "\u06FF" for ch in text):
        print(" Arabic →", arabic_model(text)[0])
    else:
        print(" English →", english_model(text)[0])
