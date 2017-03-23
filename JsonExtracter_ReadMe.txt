/*
 * Author -Project-Group 3
 * Date Created - -3/15/2017
 * Description- The Program takes three arguments -Input Path ,Output Path and generates csv file which contains user and tweets
*/

Program Execution Steps

1. javac -cp $CLASSPATH:. JsonExtracter.java
2. java -cp $CLASSPATH:. JsonExtracter /atgoyal/folder100/FlumeData.1489726865889 /nishigk/dir3 <file_count>

Once the output files are generated, merge output files using the following command

hdfs dfs -cat /vnavulu/dir3/tweet*.csv | hdfs dfs -put - /vnavulu/final_tweet_data.csv

Sample Output of the above execution steps as follows:

dangmcaIIister,"text":"my moms friend is scared to go outside bc because of donald trump and she has panic attacks daily and won't leave her house :(
nytimes,"text":"The FAKE NEWS media (failing @nytimes
nytimes,"text":"RT @realDonaldTrump: The FAKE NEWS media (failing @nytimes
NorfolkDaily,"text":"Norfolk State opens baseball season with a 4-1 win over Villanova https://t.co/teqIKcxK2P
StreetFightHub,"text":"All new way of playing coming to Ultra Street Fighter II https://t.co/JWc8oVXnAE
arleneangerfist,"text":"@arleneangerfist @criscyborg now this is what you call no fear!!
techdirt,"text":"Looking for a good cause to donate to? Vital news org @Techdirt is fighting for its life and needs your support: https://t.co/dUF2wWVMrU
techdirt,"text":"RT @FreedomofPress: Looking for a good cause to donate to? Vital news org @Techdirt is fighting for its life and needs your support: https:\u2026
LV_Sports,"text":"My mission: be so busy loving my life that I have no time for hate
LV_Sports,"text":"RT @LV_Sports: My mission: be so busy loving my life that I have no time for hate
mitchellvii,"text":"John McCain is now a world class asshole: https://t.co/GI9B3AGryS
mitchellvii,"text":"RT @mitchellvii: John McCain is now a world class asshole: https://t.co/GI9B3AGryS
PeterShirChat,"text":"@PeterShirChat Oh my.  Wonder when &amp; the journey of it?
StewartCinkFans,"text":"MGOLF Faces Big Challenge in Puerto Rico https://t.co/k0aHB0m16p
davidaxelrod,"text":"Every president is irritated by the news media.  No other president would have described the media as \"the enemy of the people.\"
davidaxelrod,"text":"The last one would've just tapped their phone and subpoenaed their sources. Much more civilized https://t.co/CeDkk9gedT
_harukimurakami,"text":"\u201cThis uneasiness comes over me from time to time
_harukimurakami,"text":"RT @_harukimurakami: \u201cThis uneasiness comes over me from time to time
TimesNow,"text":"To drag Army into politics is anti-national and disgraceful: Suhel Seth
TimesNow,"text":"StandWithArmy"}]
TimesNow,"text":"RT @TimesNow: To drag Army into politics is anti-national and disgraceful: Suhel Seth
ABC,"text":"NEW: Secret Service investigating after vehicle in Pres. Trump\u2019s motorcade in Florida was struck by an apparently thrown object (1/2)
ABC,"text":"NEW: Secret Service investigating after vehicle in Pres. Trump\u2019s motorcade in Florida was struck by an apparently thrown object (1/2)
ABC,"text":"Media &amp; liberal rage stirring up dangerous situation. Responsible people in govt &amp; media mustdemand an end to the a\u2026 https://t.co/E3BteAD1VX
ABC,"text":"RT @HeyTammyBruce: Media &amp; liberal rage stirring up dangerous situation. Responsible people in govt &amp; media mustdemand an end to the a\u2026
TwilightLovFan,"text":"Celebrating love in their twilight years https://t.co/JyluS4hYBP
