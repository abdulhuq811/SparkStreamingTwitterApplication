/**
 * @author Mohamed Abdul Huq Ismail
 * 
 * Description:
 * A simple Spark Streaming program to get the top 25 hash tags within
 * a given time interval filtered on a specific category or categories 
 * and to display the tweet that was retweeted the most within a given 
 * time interval.
 * 
 * Build jar file for the program with all its dependencies and then
 * run the program by using the jar file with the following command
 * ./spark-submit --class SparkStreaming.twitterHashTags.publicStreamTwitter 
 * --master local[n] twitterHashTags-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
 * local_file_path checkpoint_file_path twitter_credentials_file_path category_to_filter(s) (if multiple categories, pass each
 * as a separate argument
 */

package SparkStreaming.twitterHashTags;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;

import java.io.*;
import java.util.*;

import scala.Tuple2;
import org.apache.spark.streaming.twitter.*;


public class publicStreamTwitter {

	/**
	 * 
	 * @param args is used to get the input from command line arguments.
	 * The first argument is a file location to save output locally to a 
	 * text file and arguments from second till n are the values of 
	 * topics or categories to filter
	 */
	public static void main(String[] args)throws Exception {
		
		if(args.length < 4)
		{
			System.out.println("Insufficient Arguments");
			System.exit(1);
		}
		
		/**
		 * @param outputFilePath stores the value of the local_file_location.
		 * @param checkpointFilePath stores the value of the file point for
		 * checkpoint.
		 */
		final String outputFilePath = args[0];
		final String checkpointFilePath = args[1];
		final String twitterCredentialsPath = args[2];
		//Hide logs
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		/*
		 * Read twitter authorization values from file.
		 * Assumption the file is of the following format.
		 * consumerKey = value
		 * consumerSecret = value
		 * accessToken = value
		 * accessTokenSecret = value
		 */
		File file = new File(twitterCredentialsPath);
		BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
		Map<String,String> twitterValues = new HashMap<String,String>();
		String s;
		while((s = br.readLine())!=null)
		{
			String[] temp = s.split("=");
			twitterValues.put(temp[0].trim(), temp[1].trim());
		}
		
		/**
		 * @param ckey,csecret,atoken,asecret are used to store authorization values
		 * for my twitter application.
		 */
		final String ckey = twitterValues.get("consumerKey");
		final String csecret = twitterValues.get("consumerSecret");
		final String atoken = twitterValues.get("accessToken");
		final String asecret = twitterValues.get("accessTokenSecret");
		
		/*
		 * Initialize Spark Configuration Object and Java Streaming Context
		 * with 1 minute duration. This means RDDs are computed in batch for
		 * every 1 minute.
		 */
		SparkConf conf = new SparkConf().setAppName("TwitterTesting");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(60*1*1000));
		
		//Set authentication values of twitter oauth object for twitter application access.
		System.setProperty("twitter4j.oauth.consumerKey", ckey);
		System.setProperty("twitter4j.oauth.consumerSecret", csecret);
		System.setProperty("twitter4j.oauth.accessToken", atoken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", asecret);
		
		/**
		 * @param filter_values stores the topic(s) to filter from stream of tweets
		 * which is provided from the command line argument. Using list to support
		 * use of multiple values.
		 */
		List<String> filter_values = new ArrayList<String>();
		for(int i=3;i<args.length;i++)
			filter_values.add(args[i]);
		
		/**
		 * @param topics stores the list values of topics as a string array.
		 */
		String[] topics = filter_values.toArray(new String[filter_values.size()]);
		
		//FilterQuery to filter objects using topics
		FilterQuery fq = new FilterQuery();
		fq.track(topics);
		
		//Get the stream of tweets
		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc,topics);
		
		/*
		 * To get the top 25 hashTags of tweets in the specified window interval
		 */
		
		/**
		 * @param statuses stores the tweets alone in a RDD.
		 */
		JavaDStream<String> statuses = twitterStream.map(
				new Function<Status,String>()
				{
					public String call(Status status)
					{
						return status.getText();
					}
				});
	

		/**
		 * @param hashTags stores words that contain hash by first splitting the tweets
		 * into words using a flatMap and then filter words that start with a #. Finally
		 * removes # from the word. The last filter() is used to check if there is no word
		 * following a #.
		 */
		JavaDStream<String> hashTags = statuses.flatMap(
				new FlatMapFunction<String,String>(){
					public Iterable<String> call(String s)
					{
						return Arrays.asList(s.split(" "));
					}
				}).filter(
						new Function<String,Boolean>()
						{
							public Boolean call(String s)
							{
								return s.startsWith("#");
							}
						}).map(
							new Function<String,String>()
							{
								public String call(String s)
								{
									return s.substring(1);
								}
							}).filter(
									new Function<String,Boolean>()
									{
										public Boolean call(String s)
										{
											return !s.equals(" ");
										}
									});
		
		
		/**
		 * @param tuples is a PairDStream RDD that stores the pair of (word,1) using mapToPair
		 */
		JavaPairDStream<String,Integer> tuples = hashTags.mapToPair(new PairFunction<String,String,Integer>()
				{
					public Tuple2<String,Integer> call(String s)
					{
						return new Tuple2<String,Integer>(s,1);
					}
				}).reduceByKeyAndWindow(
						new Function2<Integer,Integer,Integer>(){
							public Integer call(Integer x,Integer y)
							{
								return x + y;
							}
						},
						new Function2<Integer,Integer,Integer>(){
							public Integer call(Integer x,Integer y)
							{
								return x - y;
							}
						},
						new Duration(60*5*1000),
						new Duration(60*5*1000)
						);
		
		/**
		 * @param swappedCounts is a PairDStream RDD that is used to 
		 * store (K,V) pair after swapping key with value in tuples using
		 * mapToPair. Then I sort the RDD in terms of descending order of
		 * Keys using transformToPair to get top N hashTags.
		 */
		JavaPairDStream<Integer,String> swappedCounts = tuples.mapToPair(
				new PairFunction<Tuple2<String,Integer>,Integer,String>()
				{
					public Tuple2<Integer,String> call(Tuple2<String,Integer> s)
					{
						return s.swap();
					}
				}
				).transformToPair(
						new Function<JavaPairRDD<Integer,String>,JavaPairRDD<Integer,String>>()
						{
							public JavaPairRDD<Integer,String> call(JavaPairRDD<Integer,String> s) throws Exception
							{
								return s.sortByKey(false);
							}
						}
						);
	
		/*
		 * This block of code is used to print the top 25 hash tag words.
		 * We can print top N hash tag words by passing value of N as
		 * a command line argument. Output is saved in the file for 
		 * debugging purposes. In a Hadoop cluster, results can be saved
		 * to a file in a hdfs location.
		 */
		swappedCounts.foreach(
				new Function<JavaPairRDD<Integer,String>,Void>()
				{
					public Void call(JavaPairRDD<Integer,String> rdd) throws IOException
					{
						File file = new File(outputFilePath);
						if(!file.exists())
							file.createNewFile();
						
						FileWriter fw = new FileWriter(file.getAbsolutePath(),true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write("\nWindow of 5 minutes\n");
						String out = "\nTop 25 hashtags:\n";
						System.out.println(out);
						bw.write(out);
						
						for(Tuple2<Integer,String> t: rdd.take(25))
						{
							System.out.println(t.toString());
							bw.write(t.toString()+"\n");
						}
						bw.close();
						return null;
					}
				}
				);
	
		/*
		 * Get the most retweeted tweets within the window of 5 minutes
		 */
		
		/**
		 * @param retweetStatus is a DStream RDD that stores tweets only if
		 * they have been retweeted using a filter transformation.
		 */
		JavaDStream<Status> retweetStatus = twitterStream.filter(
				new Function<Status,Boolean>()
				{
					public Boolean call(Status s)
					{
						return s.isRetweet();
					}
				}
				);
		
		/**
		 * @param retweetPair is used to store (Tweet,Retweet_Count) --> (K,V) pair
		 * within the window interval of 5 minutes.
		 */
		JavaPairDStream<String,Integer> retweetPair = retweetStatus.mapToPair(
				new PairFunction<Status,String,Integer>()
				{
					public Tuple2<String,Integer> call(Status s)
					{
						return new Tuple2<String,Integer>(s.getText(),1);
					}
				}
				).reduceByKeyAndWindow(
						new Function2<Integer,Integer,Integer>(){
							public Integer call(Integer x,Integer y)
							{
								return x + y;
							}
						},
						new Function2<Integer,Integer,Integer>(){
							public Integer call(Integer x,Integer y)
							{
								return x - y;
							}
						},
						new Duration(60*5*1000),
						new Duration(60*5*1000)
						);

		/**
		 * @param swappedRetweetCounts swaps the Key with Value using mapToPair and then
		 * sorts the RDD in descending order by Key.
		 */
		JavaPairDStream<Integer,String> swappedRetweetCounts = retweetPair.mapToPair(
				new PairFunction<Tuple2<String,Integer>,Integer,String>()
				{
					public Tuple2<Integer,String> call(Tuple2<String,Integer> s)
					{
						return s.swap();
					}
				}
				).transformToPair(
						new Function<JavaPairRDD<Integer,String>,JavaPairRDD<Integer,String>>()
						{
							public JavaPairRDD<Integer,String> call(JavaPairRDD<Integer,String> s) throws Exception
							{
								return s.sortByKey(false);
							}
						}
						);

		/*
		 * This block of function gets the first element of the RDD.
		 * Print the value of the first element as that is tweet with
		 * most retweets. I save the tweet in the same file for debugging 
		 * purpose.
		 */

		swappedRetweetCounts.foreach(
				new Function<JavaPairRDD<Integer,String>,Void>()
				{
					public Void call(JavaPairRDD<Integer,String> rdd)throws IOException
					{
						File file = new File(outputFilePath);
						if(!file.exists())
							file.createNewFile();
						
						FileWriter fw = new FileWriter(file.getAbsolutePath(),true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write("\nMost Retweeted Tweet\n");
						System.out.println("\nMost Retweeted Tweet\n");
						Tuple2<Integer,String> t = rdd.first();
						System.out.println(t._2);
						bw.write(t._2);
						bw.close();
						return null;
					}
				}
				);

		/*
		 * Attempt to do Sentiment Analysis on tweets classifying it into Positive or Negative.
		 * I made use of the coreNLP package provided by Stanford. findSentiment returns a value,
		 * higher the value, happier the sentiment.
		 * Resource could be found here: http://rahular.com/twitter-sentiment-analysis/
		 */

//		//Initializes the NLP class
//		NLP.init();
//
//		/**
//		 * @param sentiTweet is a PairDStream RDD that stores a pair of (tweet,sentiment)
//		 * using a mapToPair. This is done for a window interval of 5 minutes and sliding
//		 * window interval of 5 minutes which is provided by window(). A second mapToPair
//		 * is used to swap Key and Value.
//		 */
//		JavaPairDStream<String,String> sentiTweet = statuses.mapToPair(
//		new PairFunction<String,String,String>()
//		{
//			public Tuple2<String,String> call(String s)
//			{
//				/*
//				 * Reason for checking if value equals to 2 is cause I was getting
//				 * values of 1 and 2 only.
//				 */
//				
//				return new Tuple2<String,String>(s,NLP.findSentiment(s)==2?"Positive":"Negative");
//			}
//		}
//		).window(new Duration(60*5*1000),new Duration(60*5*1000)).mapToPair(
//				new PairFunction<Tuple2<String,String>,String,String>()
//				{
//					public Tuple2<String,String> call(Tuple2<String,String> s)
//					{
//						return s.swap();
//					}
//				}
//				);
//		
//		/**
//		 * @param sentiList is a PairDStream of <String,Iterable<String>> with key being
//		 * either positive or negative and value being corresponding tweets.
//		 */
//		JavaPairDStream<String,Iterable<String>> sentiList = sentiTweet.groupByKey();
//		
//		/**
//		 * This block of function is used to print the Positive tweets followed by
//		 * the negative tweets.
//		 * Expected Output:
//		 * Positive Tweets
//		 * <List of Tweets>\n
//		 * Negative Tweets
//		 * <List of Tweets>\n
//		 */
//		sentiList.foreach(
//				new Function<JavaPairRDD<String,Iterable<String>>,Void>()
//				{
//					public Void call(JavaPairRDD<String,Iterable<String>> rdd)
//					{
//						for(Tuple2<String,Iterable<String>> t: rdd.take(2))
//						{
//							Iterator<String> it = t._2.iterator();
//							System.out.println("\n"+t._1+" Tweets\n");
//							while(it.hasNext())
//								System.out.println(it.next());
//						}
//						return null;
//					}
//				}
//				);
//		

		//Checkpoints during windowed execution for fault-tolerance
		jssc.checkpoint(checkpointFilePath);
	
		jssc.start();
		jssc.awaitTermination();
	}
}