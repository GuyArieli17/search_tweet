class ConfigClass:

    def __init__(self,corpusPath='',output_path='', stemming=False ):
        self.corpusPath = corpusPath
        self.savedFileMainFolder = output_path
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = stemming
        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_stemmig(self):
        return self.toStem
