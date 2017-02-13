library(dplyr)
library(lubridate)
library(RODBC)
library(tidyr)
setwd("C:\\Users\\pradeepsingh.naulia\\OneDrive - Xchanging\\OneDrive - Xchanging\\working\\R-Server Code work\\WSM_PV_ALL") # To be modified
source("Config.r")
source("Assoc2_3Functions.r")

Mode <- function(x) {   ux <- unique(x); ux[which.max(tabulate(match(x, ux)))] }


dbhandle <-odbcConnect(data_source_name, uid=uid, pwd=pwd, case="nochange")
pvQuery <- "select * from LiveXMONDataBase.dbo.vw_PV_Staging_RView"
poly <- sqlQuery(dbhandle, pvQuery) #pvQuery
head(poly)
wsmQuery <- "select * from LiveXMONDataBase.dbo.vw_WSMAlartStaging"
wsm <- sqlQuery(dbhandle, wsmQuery)
head(wsm)
poly$RaiseTime<- (dmy_hms(poly$RaiseTime))
poly$Name <- as.character(poly$Name)

poly$SiteA<-   sapply(1:nrow(poly), function(x) (unlist(strsplit(poly$Name[x], split=">"))[1]))
poly$SiteB<-   sapply(1:nrow(poly), function(x) (unlist(strsplit(poly$Name[x], split=">"))[2]))

View(poly)
idu_det <- read.csv("IP10Final.csv")  #Some alerts are unclassified. So we have to filter out
idu_det <- idu_det[,1:4]
str(idu_det)

Uncl_Oper <- idu_det[idu_det$Sev.1.Description == "Uclassified-Operational (TBD)", "Description"]
Uncl_NonOper <- idu_det[idu_det$Sev.1.Description == "Unclassified-Non Operational", "Description" ]
poly_UO  <- poly[poly$Description %in% Uncl_Oper,]
poly_UN  <- poly[poly$Description %in% Uncl_NonOper,]
poly_Cl <- poly[ !(poly$Description %in% Uncl_Oper | poly$Description %in% Uncl_NonOper), ]

poly_Cl$Time <- as.numeric(difftime(poly_Cl$RaiseTime,min(poly_Cl$RaiseTime), units= "min"))

poly_Cl$Time <- round(poly_Cl$Time/960,2)  # 3600s to 7200s in version 2.2 #changed to 240 min

poly_Cl$Description <- as.factor(poly_Cl$Description)
poly_Cl$Name <- as.factor(poly_Cl$Name)

View(poly_Cl)

dfp <- poly_Cl[,c("Name", "Description","Time")]
str(dfp)
View(dfp)
#dfp <- dfp[1:3000,] ; poly_Cl <- poly_Cl[1:3000,]# to be removed # to be able to do clustering


hc1p1 <- hclust(dist(scale(data.matrix(dfp[1:nrow(dfp),]))))

#plot(cutree(hc1p1, k=2),hang=-1)

str(hc1p1)

poly_Cl_ID <- as.data.frame(cbind(poly_Cl,cutree(hc1p1, h = 0.5)))
names(poly_Cl_ID)[ncol(poly_Cl_ID)] <- "Polyview_ID"

poly_Output <- poly_Cl_ID[,c(1:12,14)] #time column dropped
View(poly_Output)

str(poly_Cl_ID)

# modified on version 2.2
P_ID <- data.frame(poly_Cl_ID %>% group_by(Polyview_ID) %>% summarise(MinTime <- min(RaiseTime),Name= as.character(Mode(Name))))
#P_ID2 <- data.frame(poly_Cl_ID %>% group_by(Polyview_ID))
                 
#View(P_ID2)
View(P_ID)
P_ID[,3] <- gsub("_+\\S", "", P_ID[,3])
str(P_ID)
P_ID$P_Prob_ID <-  paste(P_ID[,3], as.character(month(P_ID[,2])*1000000 + as.numeric(day((P_ID[,2])))*10000 + hour(P_ID[,2])*100 + minute(P_ID[,2])))
head(P_ID)
str(P_ID)
View(P_ID)
#For Association
P_IDA <- data.frame(poly_Cl_ID %>% group_by(Polyview_ID) %>% summarise(SiteA <- Mode(SiteA),SiteB <- Mode(SiteB) ))

names(P_IDA)[2:3] <- c("SiteA", "SiteB")
str(P_IDA)
View(P_IDA)
P_IDAM <- subset(P_IDA, SiteB != "<NA>")

str(P_IDAM)
P_IDAgather <- gather(P_IDAM, Type, SiteID, SiteA:SiteB)
P_IDAgather$SiteID <-  gsub("_.*", "", P_IDAgather$SiteID)
P_IDAgather$Type <- NULL
str(P_IDAgather)

P_ID1 <- left_join(P_IDAgather, P_ID)
str(P_ID1)
P_IDReady <- P_ID1[,c(1,2,5)] #P_IDReady is  used for WSM COrrel 
str(P_IDReady)
tPoly <- as.numeric(sapply(1:nrow(P_IDReady), function(x) (unlist(strsplit(P_IDReady$P_Prob_ID[x], split=" "))[2])))
P_IDReady$TimeP <- (tPoly %/%1000000)*24*30 + (tPoly %/%10000 - (tPoly %/%1000000)*100 )*24 +(tPoly %/%100) - (tPoly %/%10000)*100+  round(tPoly %%100/60,2) 
# sqlSave(dbhandle, P_IDReady,"tblNMS_P_IDReady_Tmp",safer=FALSE, append = FALSE,rownames = TRUE, colnames = TRUE, fast = FALSE)
View(P_IDReady)
## pushing to the database
str(P_IDReady)
str(poly_Output)
poly_OutputN <- left_join(poly_Output, P_ID, by = "Polyview_ID")
poly_OutputM <-  poly_OutputN[,c(1:12,16)]
View(poly_OutputM)
sqlSave(dbhandle,poly_OutputM,"tblNMS_PV_RInterim_T",safer=FALSE, append = TRUE,rownames = TRUE, colnames = TRUE, fast = FALSE)
sqlSave(dbhandle,poly_UN,"tblNMS_PV_Unclassified_NonOpera",safer=FALSE, append = TRUE,rownames = TRUE, colnames = TRUE, fast = FALSE)
sqlSave(dbhandle,poly_UO,"tblNMS_PV_Unclassified_Opera",safer=FALSE, append = TRUE,rownames = TRUE, colnames = TRUE, fast = FALSE)
