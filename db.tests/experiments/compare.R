library(MonetDB.R)

# adapt as required...
con <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")
acsdir <- "/Users/hannes/Documents/eclipse-ws/MonetDB.R/db.tests/experiments/"

# 47512 tuples, 42M, 44027410
table <- "alabama"
csv <- paste0(acsdir,"alabama.csv")

# 1.060.060 tuples, XXG
#table <- "california"
#csv <- paste0(acsdir,"california.csv")

# 9093077 tuples, 8G, 8476572042
#table <- "acs3yr"
# csv <- paste0(acsdir,"acs3yr.csv")

# read.table DNF DNF
# sql		0.1	96
# monet.frame	1.427	15452
# 
# monet.frame virtual data objects, operations transparently mapped to DB query

print("monet.frame")
time <- numeric()
bytes <- numeric()

for (j in 1:10) {
	time <- c(time,system.time(mean(subset(monet.frame(con,table),agep > 30)$adjinc))[[3]])
	bytes <- monetdbGetTransferredBytes()
}
print(summary(time))
print(time)
print(bytes)


# manually writing SQL query


print("SQL")
time <- numeric()
bytes <- numeric()

for (j in 1:10) {
	time <- c(time,system.time(dbGetQuery(con,paste0("SELECT AVG(adjinc) FROM ",table," where ( (agep>30) )")))[[3]])
	bytes <- monetdbGetTransferredBytes()
}
print(summary(time))
print(time)
print(bytes)


time <- numeric()
bytes <- numeric()


print("R")
for (j in 1:10) {
	time <- c(time,	system.time(mean(subset(read.table(csv,header=TRUE,sep=","),AGEP > 30)$ADJINC))[[3]])
}
bytes <- list(bytes.in=file.info(csv)$size)

print(summary(time))
print(time)
print(bytes)

