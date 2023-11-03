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
        full_text = ''
        """
        for t in self.text:
            if len(t) > len(full_text):
                full_text = t  
        """
        #rule = re.compile(r"[^a-zA-Z0-9\u4e00-\u9fa5]")
        #sentences = rule.split(full_text)
        sentences = self.text
        word_dict={}
        word_list=[]
        frequency_list = np.zeros(len(sentences))
        for i, sentence in enumerate(sentences):
            words = jieba.lcut(sentence)
            word_list.append(words)
            for w in words:
                try:
                    word_dict[w]+=1
                except:
                    word_dict[w]=1
        
        word_score_list = np.zeros(len(word_dict.keys()))
        
        for i,t in enumerate(word_dict.keys()):
            word_score_list[i] = word_dict[t]
        
        median = np.sort(word_score_list)[int(len(word_score_list)/2)]

        for i,t in enumerate(word_dict.keys()):
            if word_dict[t]>median:
                word_dict[t]=median

        for i, words in enumerate(word_list):
            for j in words:
                frequency_list[i]+=word_dict[j]
            frequency_list[i] = frequency_list[i]

        if len(sentences)>20:
            #bottom = int((len(sentences)-20)/2)
            bottom = 0
            #top = int((len(sentences)+20)/2)
            top = 20
            top_sentences = np.argsort(-frequency_list)[bottom:top]
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

