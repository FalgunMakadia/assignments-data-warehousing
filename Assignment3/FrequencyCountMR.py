from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sConf = SparkConf()
sContext = SparkContext(conf=sConf)

keywords = ["flu", "snow", "emergency"]

sSession = SparkSession(sContext)


class MapReduceFunctions():
    def mapFunction(self, wordMap):
        return wordMap.map(lambda word: (word.lower(), 1))

    def reduceFunction(self, resultMap):
        return resultMap.reduceByKey(lambda a, b: a+b)


def splitBySpace(file):
    return file.flatMap(lambda line: line.split(" "))


def filterWords(wordMap):
    return wordMap.filter(lambda word: word.lower() in keywords)


class MapReduce():

    def readData(self):

        mapfunc = MapReduceFunctions()

        finalWordCount = sContext.parallelize([])

        for i in range(1, 41):
            tweetFile = sContext.textFile("tweetCollection" + str(i) + ".txt")
            words = splitBySpace(tweetFile)
            filteredWords = filterWords(words)
            countOccurences = mapfunc.mapFunction(filteredWords)
            groupCountOccurences = mapfunc.reduceFunction(countOccurences)
            wordsCounts = words.filter(lambda word: word.lower() in keywords).map(lambda word: (
                word.lower(), 1)).reduceByKey(lambda a, b: a+b)
            finalWordCount = finalWordCount + groupCountOccurences
        return finalWordCount

if __name__ == '__main__':

    mr = MapReduce()

    finalWordCount = mr.readData()
    output = finalWordCount.reduceByKey(lambda a, b: a+b)
    output = sSession.createDataFrame(output).toDF("keyword", "frequency")
    output.show()