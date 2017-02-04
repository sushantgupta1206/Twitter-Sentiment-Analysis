from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    #print pwords
    nwords = load_wordlist("negative.txt")
    #nwords.pprint()
    counts = stream(ssc, pwords, nwords, 100)
    #print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    pos=[]
    neg=[]
    for i in counts:
       pos.append(i[0][1])
       neg.append(i[1][1])
    
    timesteps = [i for i in range(len(counts))]
    plt.plot(timesteps, pos, 'bo-', label='Positive')
    plt.plot(timesteps, neg, 'go-', label='Negative')
    plt.xlabel('Time Step')
    plt.ylabel('Word Count')
    plt.legend(loc='upper left')
    plt.axis([0, len(counts), 0, max(max(pos), max(neg)) + 100])
    # '+100' to avoid a clash of the legend with the max positive point
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    print filename
    # YOUR CODE HERE
    with open(filename, 'r') as f:
       pos_neg_words= f.read().split("\n")
    return set(pos_neg_words)

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount=0
    return sum(newValues,runningCount)

def posneg(word,pwords,nwords):
    if word in pwords:
        return ('Positive',1)
    elif word in nwords:
        return ('Negative',1)
    else:
        return None

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    #tweets.pprint()
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    line= tweets.flatMap(lambda line: line.split(" ")).map(lambda x: x.lower())
    
    #line.pprint()
    words= line.map(lambda word: posneg(word,pwords,nwords)).filter(lambda word: False if word is None else True)
    #words.pprint()
    
    wordCount= words.reduceByKey(lambda x,y: x+y)
    #wordCount= posCount+negCount 
    #wordCount.pprint()
    runningCounts=words.updateStateByKey(updateFunction)
    #runningCounts.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    wordCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
