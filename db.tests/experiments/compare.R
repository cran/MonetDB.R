library(MonetDB.R)

# adapt as required...
con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb",timeout=100)
acsdir <- "/export/scratch2/hannes/acsdata/"

# 47512 tuples, 42M, 44027410
#table <- "alabama"
#csv <- paste0(acsdir,"ss10pal.csv")
#ser <- paste0(acsdir,"ss10pal.rda")

#ex  time bytes.in
#1  read.table 4.450 44027410
#2        load 0.298  9503219
#3         sql 0.086       96
#4 monet.frame 3.385    15697


# 2677401 tuples, 2.4G,  2496799841
#table <- "acsa"
#csv <- paste0(acsdir,"ss10pusa.csv")
#ser <- paste0(acsdir,"ss10pusa.rda")
#ex    time   bytes.in
#1  read.table 217.398 2496799841
#2        load  31.458  460576543
#3         sql   0.137         96
#4 monet.frame   2.428      14715


# 9093077 tuples, 8G, 8476572042
table <- "acs3yr"
csv <- paste0(acsdir,"ss10.all.csv") # ss10.all.csv is a concatenation of all ss10pus*.csv files
ser <- paste0(acsdir,"ss10.all.rda")
#TODO timings

# monet.frame virtual data objects, operations transparently mapped to DB query
frametime <- system.time(mean(monet.frame(con,table)$adjinc*1.14))
framebytes <- monetdbGetTransferredBytes()

print(frametime)

# manually writing SQL query
mantime <- system.time(dbGetQuery(con,paste0("SELECT AVG((adjinc)*1.14) FROM ",table)))
manbytes <- monetdbGetTransferredBytes()

print(mantime)

csvtime <- system.time(read.table(csv,header=TRUE,sep=",")$adjinc*1.14)
csvbytes <- list(bytes.in=file.info(csv)$size)

print(csvtime)

if (!file.exists(ser)) {
	r <- read.table(csv,header=TRUE,sep=",")
	save(r,file=ser)
	remove(r)
}

sertime <- system.time({
	load(ser)
	r$adjinc*1.14
})
serbytes <- list(bytes.in=file.info(ser)$size)

print(sertime)

result <- data.frame(ex=c("read.table","load","sql","monet.frame"),time=c(csvtime[["elapsed"]],sertime[["elapsed"]],mantime[["elapsed"]],frametime[["elapsed"]]),bytes.in=c(csvbytes[["bytes.in"]],serbytes[["bytes.in"]],manbytes[["bytes.in"]],framebytes[["bytes.in"]]))

print(result)

