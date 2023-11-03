import re
from html5_parser import parse
from lxml.etree import tostring
import jieba
import numpy as np

class HtmlParser:
    def __init__(self, html: str):

        self.title = ''
        self.urls = []
        self.text = []
        
        self.re_pattern = re.compile(r'\s+')

        self.root = parse(html)
        self.__travel(self.root)
        
    def __travel(self, now):
        for node in now:
            if node.tag == 'script':
                continue

            if node.tag == 'title':
                self.title = node.text if node.text else ''
            elif node.tag == 'a':
                href = node.get('href')
                if href:
                    self.urls.append(href)
        
            if node.tag in ['a', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'span', 'div']:
                #result = f'{node.text if node.text else ""}{self.__extract_all_children(node)}{node.tail if node.tail else ""}'
                
                #self.text.append(self.remove_space(result))

                result = f'{node.text if node.text else ""}{node.tail if node.tail else ""}'
                if len(result)>0:
                    self.text.append(self.remove_space(result))
                self.text  += self.__extract_all_children(node)
                continue
            self.__travel(node)

    def remove_space(self, text):
        return re.sub(self.re_pattern, ' ', text)

    def __extract_all_children(self, now):
        tmp = []
        for node in now:
            if node.tag == 'script':
                continue
            if node.tag == 'a':
                self.urls.append(node.get('href'))
            text = ''
            if node.text:
                text += node.text
            if node.tail:
                text += node.tail
            if len(text)>0:
                tmp.append (self.remove_space(text))
            try:
                tmp += self.__extract_all_children(node)
            except RecursionError:
                return tmp
        return tmp

    def get_hrefs(self):
        return self.urls

    def get_title(self):
        return self.title
    
    def get_main_content(self):

        sentences = self.text
        word_list=[]
        segmented_sentences=[]
        tf_list = []
        for i, sentence in enumerate(sentences):
            words = jieba.lcut(sentence)
            segmented_sentences.append(words)
            tf_dict={}
            for w in words:
                if w not in word_list:
                    word_list.append(w)
                try:
                    tf_dict[w]+=1
                except:
                    tf_dict[w]=1
            words_len = len(words)
            for t in tf_dict.keys():
                tf_dict[t] = tf_dict[t]/words_len
            tf_list.append(tf_dict)

        idf = {}
        sentences_len = len(sentences)
        for w in word_list:
            f = 0
            for s in segmented_sentences:
                if w in s:
                    f+=1
            idf[w] = np.log(sentences_len/f)


        tfidf=tf_list.copy()
        for s in tf_list:
            for w in s.keys():
                s[w] = s[w]*idf[w]

        score_list = np.zeros(sentences_len)
        for i,s in enumerate(tfidf):
            score = 0
            for w in s.keys():
                score += s[w]
            score_list[i] = score
        """
        for i,score in enumerate(score_list):
            print(score, segmented_sentences[i])
        """

        if len(sentences)>20:

            top_sentences_list = np.argsort(-score_list)
            count=0
            top_sentences=[]
            for idx in top_sentences_list:
                if len(sentences[idx])<20:
                    continue
                else:
                    top_sentences.append(idx)
                    count+=1
                if count==20:
                    break
            """
            for i in np.argsort(-frequency_list):
                print(frequency_list[i],sentences[i])
            """
            result = [] 
            for i in range(len(sentences)):
                if i in top_sentences:
                    result.append(sentences[i])     
        else:
            result = sentences

        return result


if __name__ == '__main__':
    with open('stackoverflow.html', 'r') as f:
        html = f.read()
    parser = HtmlParser(html)
    content_list = parser.get_main_content()

    for i,t in enumerate(content_list):
        print(t)

