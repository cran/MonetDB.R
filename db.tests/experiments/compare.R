library(MonetDB.R)

# adapt as required...
con <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")
acsdir <- "/export/scratch2/hannes/acsdata/"

# 47512 tuples, 42M, 44027410
#table <- "alabama"
#csv <- paste0(acsdir,"ss10pal.csv")
#ser <- paste0(acsdir,"ss10pal.rda")

#1  read.table 4.419 44027410
#2        load 0.285  9503219
#3         sql 0.089       96
#4 monet.frame 1.111    15865


# 1.060.060 tuples, XXG
#table <- "california"
#csv <- paste0(acsdir,"california.csv")

# 9093077 tuples, 8G, 8476572042
table <- "acs3yr"
# csv <- paste0(acsdir,"ss10.all.csv") # ss10.all.csv is a concatenation of all ss10pus*.csv files
# ser <- paste0(acsdir,"ss10.all.rda")

#
# read.table DNF DNF
# sql		0.1	96
# monet.frame	1.427	15452
# 
# monet.frame virtual data objects, operations transparently mapped to DB query
frametime <- system.time(mean(subset(monet.frame(con,table),agep > 30)$adjinc))
framebytes <- monetdbGetTransferredBytes()

print(frametime)
print(framebytes)

stop()
# manually writing SQL query
mantime <- system.time(dbGetQuery(con,paste0("SELECT AVG(adjinc) FROM acs3yr ",table," where ( (agep>30) )")))
manbytes <- monetdbGetTransferredBytes()

print(mantime)
print(manbytes)

#mean(subset(d,agep>30)$adjinc)

#
#
csvtime <- system.time(mean(subset(read.table(csv,header=TRUE,sep=","),AGEP > 30)$ADJINC))
csvbytes <- list(bytes.in=file.info(csv)$size)
#
print(csvtime)
#
#if (!file.exists(ser)) {
#	r <- read.table(csv,header=TRUE,sep=",")
#	save(r,file=ser)
#	remove(r)
#}
#
#sertime <- system.time({
#	load(ser)
#	r$adjinc*1.14
#})
#serbytes <- list(bytes.in=file.info(ser)$size)
#
#print(sertime)


