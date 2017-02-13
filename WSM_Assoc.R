library(dplyr)
library(lubridate)
library(RODBC)
library(tidyr)
setwd("C:\\Users\\pradeepsingh.naulia\\OneDrive - Xchanging\\OneDrive - Xchanging\\working\\R-Server Code work\\WSM_PV_ALL") # To be modified
source("Config.r")
source("Assoc2_3Functions.r")

Mode <- function(x) {   ux <- unique(x); ux[which.max(tabulate(match(x, ux)))] }


dbhandle <-odbcConnect(data_source_name, uid=uid, pwd=pwd, case="nochange")
wsmQuery <- "select * from LiveXMONDataBase.dbo.vw_WSMAlartStaging"
wsm <- sqlQuery(dbhandle, wsmQuery)
wsm$AlarmTime = wsm$`Alarm time`
wsm$AlarmTime <- ymd_hms(wsm$AlarmTime)
wsm <- wsm[!is.na(wsm$AlarmTime),]
wsm$ProbableCause <- as.character(wsm$ProbableCause)
wsm$SiteID <- as.character(wsm$SiteID)
wsm$Time <- as.numeric(difftime(wsm$AlarmTime,min(wsm$AlarmTime), units= "min"))
wsm$Time <- round(wsm$Time/960,2) 
str(wsm)

wp <- read.csv("WSM_PolyView_Corellation_Network_Alerts_Nov_Dec_Jan.csv", stringsAsFactors = FALSE)
head(wp)
names(wp)[2:3] <- c("PolyviewFlag", "Problem_Description")
head(wp)

wp$Problem_Description <- as.factor(wp$Problem_Description)

wsmM <- left_join(wsm, wp)

wsmM$SiteID <- as.factor(wsm$SiteID)
str(wsmM)

dfw <- wsmM[,c("SiteID", "Problem_Description", "Time")] #,"SiteName"

str(dfw)
w1 <- hclust(dist(scale(data.matrix(dfw[1:nrow(dfw),]))))

wsmMID <- as.data.frame(cbind(wsmM,cutree(w1, h = 0.5)))
#h<-cbind(wsmM,cutree(w1, h = 0.5)) #for demo only
#head(h) #for demo only
#str(h) #for demo only
names(wsmMID)[ncol(wsmMID)] <- "wsm_ID"
head(wsmMID)
str(wsmMID)
wsm_Output <- wsmMID[,c(1:12,16)]

#version1.1
W_ID <- data.frame(wsmMID %>% group_by(wsm_ID) %>% summarise(MinTime <- min(AlarmTime), SiteNum <- as.character(Mode(SiteName))))

str(W_ID)
#W_ID[,3]
#W_ID[,3] <- gsub(" ", "", W_ID[,3])
W_ID$W_Prob_ID <-  paste(W_ID[,3], as.character(month(W_ID[,2])*1000000 + as.numeric(day((W_ID[,2])))*10000 + hour(W_ID[,2])*100 + minute(W_ID[,2])))
#W_ID$W_Prob_ID <- W_ID$SiteNum*100000000+ month(W_ID[,2])*1000000 + as.numeric(day((W_ID[,2])))*10000 + hour(W_ID[,2])*100 + minute(W_ID[,2])
str(W_ID)

wsm_OutputN <- left_join(wsmMID, W_ID, by = "wsm_ID")
wsm_OutputM <-  wsm_OutputN[,c(1:11,19)]
str(wsm_OutputM)
## For Association
W_IDA <- data.frame(wsmMID %>% filter(PolyviewFlag == "Yes") %>%  group_by(wsm_ID) %>% summarise(SiteID_W <- Mode(SiteID)))
names(W_IDA)[2] <- "SiteID"
W_ID1 <- left_join(W_IDA, W_ID)
W_IDReady <- W_ID1[,c(1,2,5)]


head(W_IDReady)
View(wsm_OutputM)
substrRight <- function(x, n){  substr(x, nchar(x)-n+1, nchar(x))}
tWSM <- sapply(1:nrow(W_IDReady), function(x) as.numeric(substrRight(W_IDReady$W_Prob_ID[x], 8)))
head(tWSM)
# sqlSave(dbhandle,wsm_OutputM,"tblNMS_W_IDReady_Tmp",safer=FALSE, append = FALSE,rownames = TRUE, colnames = TRUE, fast = FALSE)
W_IDReady$TimeW <- (tWSM %/%1000000)*24*30 + (tWSM %/%10000 - (tWSM %/%1000000)*100 )*24 +(tWSM %/%100) - (tWSM %/%10000)*100+  round(tWSM %%100/60,2)
View(wsm_OutputM)
sqlSave(dbhandle,wsm_OutputM,"tblNMS_WSM_Alerts_RInterim_T",safer=FALSE, append = TRUE,rownames = TRUE, colnames = TRUE, fast = FALSE)
