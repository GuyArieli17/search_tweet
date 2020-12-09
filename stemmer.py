from nltk.stem import snowball, PorterStemmer

class Stemmer:

    def __init__(self):
        self.stemmer = PorterStemmer()  # snowball.SnowballStemmer("english")

    def stem_term(self, word):
        """
        This function stem a token
        :param token: string of a token
        :return: stemmed token
        """
        if word[0] != '#' and word[0] != '@' and word.isalpha():
            return self.stemmer.stem(word)
        return word


if __name__ == '__main__':
    s = Stemmer()
    x = s.stem_term("caressesdasdasd")
    print(x)

    """
    
    def diceStemmers(self, word):
        lst1 = self.breakingDownWord(word)
        for w in self.inverted_idx.keys():
            count = 0
            lst2 = self.breakingDownWord(w)
            UnionList = list(set(lst1) | set(lst2))
            for i in lst1:
                count += lst2.count(i)
            if count / len(UnionList) >= 0.8:
                return w
        return word

    def breakingDownWord(self, word):
        n = 2
        y = [word[i:i + n] for i in range(0, len(word), 1)]
        return y[:len(y) - 1]
    """
