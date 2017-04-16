Program Execution Steps
hadoop jar Phase4.jar NoOfTweets /vnavulu/UserEmotion/part-r-00000 /vnavulu/NoOfTweets

Command to get the list of first 50 users with top number of tweets
hdfs dfs -cat /vnavulu/NoOf/part-r-00000 | sort -n -k2 -r | head -50

Sample Output of the above execution steps as follows:

nytimes 37500
rweingarten     3326
realDonaldTrump 3071
Gurmeetramrahim 2813
SkyNiews        2649
SethAbramson    2136
shineonmag      1899
melaninist      1814
POTUS   1612
FoxNews 1459
mikandynothem   1436
chucktodd       1378
NewYorker       1222
CNN     1169
Shincheonji_11  1138
mitchellvii     1126
RBReich 1116
SsjKiyoshi      1064
GordonRamsay    1060
RealJamesWoods  985
thehill 984
UrselaQuinn     966
OriginalFunko   912
NPR     895
