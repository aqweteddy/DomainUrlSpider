import datasets
from tokenizers import Tokenizer
TOK = Tokenizer.from_pretrained('bert-base-chinese')


def filter_js(x):
    if len(x['body']) < SHORT_LEN:
        return False
    return 'javascript' not in x['body'].lower()

def clean(x):
    sep_body = x['body'].split('\n')
    prev_short = True
    
    # check x1 is short or not
    result = []
    for x1, x2 in zip(sep_body, sep_body[1:]):
        x1_tok = TOK.encode(x1)
        x2_tok = TOK.encode(x2)
        next_short = len(x2_tok) < SHORT_LEN
        cur_short = len(x1_tok) < SHORT_LEN

        if not prev_short or not cur_short or not next_short:
            result.append(x1)
            prev_short = cur_short
    result = '\n'.join(result)
    return {"body": result}

CASE1 = """建物密度大於50%的地段按測量類別有 ...更多\n更多\n社群動態\nFacebook\n精彩影像\n林右昌部長出席112年國家警光獎頒獎典禮\n112-10-31\n更多\n影音專區\n美好從停讓開始\n美好從停讓開始\n112-10-23\n更多\n相關連結\n更多\n選單\n:::\n本部簡介\n職掌及組織\n部長專區\n施政計畫\n內政概要\n內政防疫成果\n聯絡資訊\n本部單位及所屬機關\n史料專區\n主題政策\n人民安心，簡政便民\n國土永續，居住正義\n公民參與，擁抱國際\n訊息快遞\n新聞發布\n即時新聞澄清\n行政公告\n活動訊息\n本部徵才\n多媒體專區\n主題服務\nCOVID-19 防疫訊息專區\n政府資訊公開\n內政部憑證管理中心\n重要指標查詢\n申辦表單\n轉型正義業務專區\n綜合規劃司專區\n民政服務專區\n戶政司\n地政司\n宗教禮制殯葬專區\n國土專區\n合作及人民團體專區\n工程施工查核專區\n公共工程生態檢核資訊專區\n人事主題專區\n政風專區\n統計主題專區\n法規及訴願\n檔案推廣與應用網站\n機關檔案管理專區\n內政財團法人登錄系統\n互動交流\nRSS\n1996\nFacebook\nYouTube\nFlickr\n常見問答\n雙語詞彙\n民意信箱\n網站資訊\n主題政策\n相關連結\n快捷服務\n內政部聽證專區\n下方連結\n網站安全政策\n隱私權保護政策\n政府網站資料開放宣告\n保有及管理個人資料\n地址：100218臺北市中正區徐州路5號 (\n位置圖\n)\n總機：1996 內政服務熱線；(02)7750-5096\n更新日期\n112-11-03"""

DIR = './result/'
SHORT_LEN = 15
# print(clean({'body': CASE1}))
ds = datasets.load_dataset('json',
                           data_files='./chunk_4.jsonl',
                        #    data_dir=DIR, 
                           cache_dir='.cache')['train']
ds = ds.filter(filter_js, num_proc=8)
ds = ds.map(clean, num_proc=8)
ds.to_json('result/train.jsonl', lines=True, force_ascii=False)