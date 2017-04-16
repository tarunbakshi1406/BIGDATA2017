# Read the CSV file and delimiter is ,
input <- read.csv("part-r-00000",FALSE,",")
#Take the column sum from input file except the first column
sums<-colSums(input[,-1])
#Take the mean of each emotion
piepercent<-round(sums/sums[7]*100)
#labels used for representing each column
labels <- c("joy", "sadness", "surprise", "anger","fear","disguist")
#Concatenate the labels, its pecentage and % symbol
lbls <- paste(labels, piepercent,"%")
#Create pie chart for the columns from 1 to 6 with the above mentioned labels
pie(piepercent[1:6],labels=lbls,,main="Mean emotion proportion scores",col = rainbow(6))