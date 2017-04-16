Program Execution Steps
Merge flume files using the following command

hdfs dfs -cat /vnavulu/dir3/tweet*.csv | hdfs dfs -put - /vnavulu/final_tweet_data.csv

Execute the following command to run the map reduce job
hadoop jar Phase4.jar JsonExtractor /vnavulu/final_tweet_data.csv /vnavulu/JsonExtract

Sample Output of the above execution steps as follows:

FoxNews "text":"#FoxNews just keeps winning - sorry CNN (Cheating News Network) haha ! https://t.co/xLE19hpwDd
FoxNews "text":".@FoxNews anchor Heather Nauert in talks to be State Dept. spokesperson https://t.co/53MI8x4Xlt https://t.co/D6hEr8g1s4
FoxNews "text":".@FoxNews anchor Heather Nauert in talks to be State Dept. spokesperson https://t.co/53MI8x4Xlt https://t.co/D6hEr8g1s4
FoxNews "text":"I ABSOLUTELY AGREE WITH YOU !! He's as bad as CNN !! Come on FOX NEWS
FoxNews "text":"@FoxNews ??head Shep Smith has to go! What is Fox trying to do become like \"very fake news\" CNN? #boycottshepardsmith
FoxNews "text":"boycottshepardsmith"}]
FoxNews "text":"Fox News Poll:  Voters divided over trusting Trump or the media via the @FoxNews App https://t.co/5N6hSgWU0t
FoxNews "text":"@FoxNews Website running misleading banner on news alert!Citizens trusted Trump more than media! https://t.co/LqsNIIdTfe
FoxNews "text":"@FoxNews @jessebwatters this logic is simply dumbfounding. Faux News at its best
FoxNews "text":"Fox News Poll: Compared to President Obama
FoxNews "text":"Fox News Poll: Compared to President Obama
FoxNews "text":"The people are not dumb. \n\n https://t.co/XwHXAkWGwb
FoxNews "text":"RT @NolteNC: The people are not dumb. \n\n https://t.co/XwHXAkWGwb
FoxNews "text":"RT @JoyAnnReid: .@realDonaldTrump so \"fake news\" or nah? https://t.co/Fdj4cc6SEV
FoxNews "text":".@realDonaldTrump so \"fake news\" or nah? https://t.co/Fdj4cc6SEV
FoxNews "text":"Fox News Poll: It would be better for the country if journalists\u2026 https://t.co/vPydZyykBV
FoxNews "text":"Fox News Poll: It would be better for the country if journalists\u2026 https://t.co/vPydZyykBV
FoxNews "text":"RT @FoxNews: Fox News Poll: Who voters trust more to tell the public the truth. #First100 https://t.co/123AYF67Xn
FoxNews "text":"First100"}]
FoxNews "text":"Fox News Poll: Who voters trust more to tell the public the truth. #First100 https://t.co/123AYF67Xn
FoxNews "text":"@FoxNews @ShamsiAli2 you should know your shitty religion created it and your religion intentionally kills innocent ppl use as human shields
FoxNews "text":"Crimea under Russia: Locals fear more mayhem if forced to return to Ukraine https://t.co/eyv2Y1Bfqf via the @FoxNews Android app
FoxNews "text":"Fox News Poll: Compared to President Obama
FoxNews "text":"RT @FoxNews: Fox News Poll: Compared to President Obama
FoxNews "text":"@FoxNews @ShamsiAli2 Cutting someone's head off because they don't follow your religion seems pretty discriminatory too.
FoxNews "text":"@FoxNews @ShepNewsTeam @FearUS_FL @DeplorablePIxie Get rid of leftist Shep Smith-belongs with CNN &amp; Fake News-tired of his lib diatribes
FoxNews "text":"Fox News Poll: It would be better for the country cat: Filesystem closed

