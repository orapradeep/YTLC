#3. WSM Polyview Association

#intersect(unique(W_IDReady$SiteID),unique(P_IDReady$SiteID))

  #poly
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


#wsm finish

#Recalling Polyview and WSM tables
# polyviewIDQuery <- "SELECT *  FROM tblNMS_P_IDReady_Tmp where rownames <> 'rownames'"
# P_IDReady <- sqlQuery(dbhandle, polyviewIDQuery)
# 
# wsmIDQuery <- "select * from tblNMS_W_IDReady_Tmp where rownames <> 'rownames'"
# W_IDReady <- sqlQuery(dbhandle, wsmIDQuery)

P_IDReady$SiteID <-  gsub("[A-Z].*","",P_IDReady$SiteID )
W_IDReady$SiteID <-  gsub("[A-Z]","",W_IDReady$SiteID )
W_IDReady$rownames <- NULL
P_IDReady$rownames <- NULL
str(W_IDReady)
str(P_IDReady)
W_P_A <-inner_join(W_IDReady, P_IDReady)

head(W_P_A)
View(W_P_A)
head(W_IDReady)
W_P_A$TimeDiff <-abs(W_P_A$TimeW - W_P_A$TimeP)
lag <- 1.0  # An hour as a limit of lag between wsm and polyview flag
W_P <- filter(W_P_A, TimeDiff < lag)
str(W_P_A)
W_P1 <-W_P[,c(3,6)]
str(W_P1)
if(nrow(W_P1) <1) {
  stop("No Correlation")  }

View(W_P1)
#sqlClear(dbhandle,tblNMS_P_IDReady_Tmp,errors = FALSE )
#sqlClear(dbhandle,tblNMS_W_IDReady_Tmp,errors = FALSE )


W_P1$Prob_ID <- paste(sapply(1:nrow(W_P1), function(x) substrRight(W_P1$P_Prob_ID[x], 8)), 
                      sapply(1:nrow(W_P1), function(x) substr(W_P1$W_Prob_ID[x], 1,8)))
View(W_P1)

sqlSave(dbhandle,W_P1,"tblNMS_WSM_PV_RInterim_T",safer=FALSE, append = TRUE,rownames = TRUE, colnames = TRUE, fast = FALSE)

odbcClose(dbhandle) # closing DB connection

