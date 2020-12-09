from nltk.corpus import stopwords
from document import Document
import time
from stemmer import Stemmer


class Parse:
    def __init__(self, stemming=False):
        self.stemming = stemming
        self.toStem = Stemmer()
        self.terms_dic_to_document = {}
        #self.lower_set = set()
        #self.upper_set = set()
        self.numberList = {"thousand": 'K', "million": 'M',
                           "billion": 'B', "percentage": '%', "percent": '%', "dollar": '$'}
        self.stop_words = stopwords.words('english')
        # contains of all stop words acording to thiers first letter
        self.dict_stop_words = {
            'a': [], 'b': [], 'c': [], 'd': [], 'e': [], 'f': [], 'g': [], 'h': [], 'i': [], 'j': [], 'k': [], 'l': [], 'm': [], 'n': [],
            'o': [], 'p': [], 'q': [], 'r': [], 's': [], 't': [], 'u': [], 'v': [], 'w': [], 'x': [], 'y': [], 'z': []
        }
        # build the dic of stop Word
        for w in self.stop_words:
            self.dict_stop_words[w[0]].append(w)
        # all operator we dont want and all parentheses character and all separators character
        self.skip_list = {',', ';', ':', ' ', '\n',
                          '(', ')', '[', ']', '{', '}', '*', '+', '-', '/', '<', '>', '&', '=', '|', '~', '"'}
        # all wired symbols
        self.wird_symbols = {'!', '#', '$', '%', '&', '(', ')', ',', '*', '+', '-', '.', '/', ':', ';', '<', '=', '>', '?',
                             '@', '[', "'\'", ']', '^', '`', '{', '|', '}', '~', '}'}

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        indices = doc_as_list[4]
        retweet_text = doc_as_list[5]
        retweet_urls = doc_as_list[6]
        retweet_indices = doc_as_list[7]
        quoted_text = doc_as_list[8]
        quote_urls = doc_as_list[9]
        quoted_indices = doc_as_list[10]
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_indice = doc_as_list[13]
        term_dict = {}

        # return a list do nothing just add to term dic no index
        self.tokenSplit(full_text, term_dict)
        self.convertURL(url, term_dict)
        self.convertURL(retweet_urls, term_dict)
        self.convertURL(quote_urls, term_dict)
        self.tokenSplit(retweet_text, term_dict)
        self.tokenSplit(quoted_text, term_dict)

        document = Document(tweet_id, tweet_date, full_text, url, indices, retweet_text, retweet_urls, retweet_indices,
                            quoted_text, quote_urls, quoted_indices, retweet_quoted_text, retweet_quoted_urls, retweet_quoted_indice, term_dict)
        return document

    def tokenSplit(self, text, term_dict={}):
        """
            This function tokenize, remove stop words and apply lower case for every word within the text
            :param text:
            :return:
        """
        # if text empty return empty list
        if text == None:
            return []
        # list of all terms
        term = ''  # a term to add
        #lst = []
        # run on all character in the text and build terms
        prev_term = ''  # start with empty
        for character in text:
            # make sure character is not int the forbiden list (divide word)
            if character not in self.skip_list:
                term += character  # keep building the term
            if (character == ' ' or character == '/' or character == ":" or character == '"' or character == '\n') and len(term) > 0:
                self.addToken(prev_term, term, term_dict)
                prev_term = term
                term = ''
        if len(term) > 0:
            self.addToken(prev_term, term, term_dict)

    def addToEntitys(self, term, term_dict):
        if term not in term_dict.keys():
            term_dict[term.upper()] = 1
        else:
            term_dict[term.upper()] += 1

    def checkWordWithApostrophes(self, word):
        if "'" in word:
            head, sep, tail = word.partition("'")
        else:
            head, sep, tail = word.partition("’")
        if head.lower() not in self.dict_stop_words[head[0].lower()]:
            return head
        return word

    def addToken(self, prev_term, word, term_dict):
        # if has a . ? ! remove from word
        if word[-1] == '.' or word[-1] == '?' or word[-1] == '!':
            word = word[:-1]
            # if its the all word return empty list
            if len(word) == 0:
                return word.lower()
        # if doesnt end with ... and letter
        if word[-1] != '…' and word[0].lower() in self.dict_stop_words.keys():
            # if not a stop word
            if word.lower() not in self.dict_stop_words[word[0].lower()]:
                # if a Million/dollar and exc..
                if "’" in word or "'" in word:
                    newWord = self.checkWordWithApostrophes(word)
                    self.add_term_to_dict(newWord, term_dict)
                    word = word.replace("'", "")
                    word = word.replace("’", "")
                if word.lower() in self.numberList.keys() and prev_term != '':
                    # check if a number
                    if prev_term.isnumeric():
                        # add dymbol
                        prev_term += self.numberList[word.lower()]
                        self.add_term_numbers_to_dict(prev_term, term_dict)
                # if this and prev are upper creante a new Term
                if prev_term != '' and prev_term[0].isupper() and prev_term.upper() in self.terms_dic_to_document.keys() and word[0].isupper():
                    word2 = prev_term + " " + word
                    self.addToEntitys(word2, term_dict)
                self.add_term_to_dict(word, term_dict)
        # if not emoji and not a wiired symbol  or a number or # not end in ... or start with '
        elif ((word.isascii() and word not in self.wird_symbols) or word.isnumeric() or word[0] == '#') and word[-1] != '…' and word[0] != "'":
            # remove ,
            word = word.replace(",", "")
            # if first symbol is nubmer
            if word[0].isdigit():
                # if word is a number
                if word.isnumeric():
                    try:
                        newW = self.convertNumber(int(word))
                    except:
                        try:
                            newW = self.convertNumber(float(word))
                        except:
                            newW = word
                    # if diffrent after convert
                    if newW != word:
                        # lst.append(newW.lower())
                        self.add_term_to_dict(word, term_dict)
                elif len(word) > 7:
                    for i in self.numberList.keys():
                        if i in word:
                            word = word.replace(i, self.numberList[i])
            elif word[0] == "#":
                self.find_sub_text_indexes(word, term_dict)
            self.add_term_to_dict(word, term_dict)
        return word

    def convertURL(self, URL, term_dict):
        if URL == None:
            return
        word = ''
        for i in URL:
            if i not in self.skip_list:
                word += i
            if (i == '/' or i == ":" or i == '"') and len(word) >= 1:
                #lst = self.addToken(lst, word)
                self.add_term_to_dict(word, term_dict)
                word = ''
        #lst = self.addToken(lst, word)
        if len(word) >= 1:
            self.add_term_to_dict(word, term_dict)

    def convertNumber(self, num):
        if num == None:
            return ""
        if num >= 1000 and num < 1000000:
            return str(num/1000)+'K'
        if num >= 1000000 and num < 1000000000:
            return str(num/1000000)+'M'
        if num >= 1000000000:
            return str(num/1000000000)+'B'
        return str(num)

    def add_term_numbers_to_dict(self, term, term_dict):
        if term.lower() not in self.terms_dic_to_document.keys() and term.upper() not in self.terms_dic_to_document.keys():
            # self.upper_set.add(term.upper())
            term_dict[term.upper()] = 1
            self.terms_dic_to_document[term.upper()] = 1
        elif term.upper() in self.terms_dic_to_document.keys():
            if term not in term_dict.keys():
                term_dict[term.upper()] = 1
            else:
                term_dict[term.upper()] += 1
            self.terms_dic_to_document[term.upper()] += 1

    def add_term_to_dict(self, term, term_dict):
        # if word not empty
        if len(term) > 0:
            # if we want to steam steam
            if self.stemming:
                term = self.toStem(term)
            # if a new word (didn't see it before) in all doc
            if term.lower() not in self.terms_dic_to_document.keys() and term.upper() not in self.terms_dic_to_document.keys():
                if (term[0].isupper() and (term[1:].islower() or term[1:].isupper()) and term.isalpha()) or term[-1].upper() in self.numberList.values():
                    if (term[0].isupper() and (term[1:].islower() or term[1:].isupper()) and term.isalpha()) or term[-1].upper() in self.numberList.values():
                        term_dict[term.upper()] = 1
                        self.terms_dic_to_document[term.upper()] = 1
                    else:
                        # self.lower_set.add(term)
                        term_dict[term.lower()] = 1
                        self.terms_dic_to_document[term.lower()] = 1
                else:
                    # self.lower_set.add(term)
                    term_dict[term.lower()] = 1
                    self.terms_dic_to_document[term.lower()] = 1
            # if appear as lower in all doc
            elif term.lower() in self.terms_dic_to_document.keys():
                # if first time in doc crate new else ++
                if term not in term_dict.keys():
                    term_dict[term.lower()] = 1
                else:
                    term_dict[term.lower()] += 1
                # inc the lower count
                self.terms_dic_to_document[term.lower()] += 1
            # if appear as upper in all doc
            elif term.upper() in self.terms_dic_to_document.keys():
                if term[0].isupper() or term[-1].upper() in self.numberList.values():
                    if term.upper() not in term_dict.keys():
                        term_dict[term.upper()] = 1
                    else:
                        term_dict[term.upper()] += 1
                    self.terms_dic_to_document[term.upper()] += 1
                else:
                    # self.upper_set.remove(term.upper())
                    # self.lower_set.add(term.lower())
                    self.terms_dic_to_document[term.lower()] = self.terms_dic_to_document[term.upper()]+1
                    del self.terms_dic_to_document[term.upper()]
                    if term.upper() in term_dict.keys():
                        term_dict[term.lower()] = term_dict[term.upper()]+1
                        del term_dict[term.upper()]
                    else:
                        term_dict[term.lower()] = 1

    def find_sub_text_indexes(self, hashtag, term_dict):
        i = 0
        word = ''
        for letter in hashtag[1:]:
            i += 1
            if (i == len(hashtag)-1):
                i -= 1
            if letter.isupper():
                if (not word.isupper() and len(word) > 0) or hashtag[i + 1].islower():
                    if word != '#' and len(word) > 0:
                        self.add_term_to_dict(word, term_dict)
                    word = ''
            elif word.isupper() and len(word) != 1:
                self.add_term_to_dict(word, term_dict)
                word = ''
            elif letter == '_' and len(word) > 0:
                self.add_term_to_dict(word, term_dict)
                word = ''
                letter = ''
            word += letter
        if word != '':
            self.add_term_to_dict(word, term_dict)


if __name__ == '__main__':
    p = Parse()
    x = p.tokenSplit(
        '5K', {})
    print(p.terms_dic_to_document)
