args <- commandArgs()
#print (args)
setwd("C:/Service_Jobs/R_Script_and_jobs/R_Script_for_web")
require("forecast")
require(RJDBC)
require("doBy")

print ("Connecting to Db2")
jcc = JDBC("com.ibm.db2.jcc.DB2Driver","C:/Service_Jobs/R_Script_and_jobs/db2jcc4.jar")
conn = dbConnect(jcc,
                 "jdbc:db2://che-db2edw01.idx.expedmz.com:50001/EXPPRD01",
                 user=(args[6]),
                 password=(args[7]))  

rs = dbSendQuery(conn, 
                 
                 "SELECT
                 
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME as Region,
                 DM.V_TRANS_DATE_DIM.TRANS_DATE as Date,
                 SUM(DM.V_ORDER_TRANS_FACT.ORDER_GROSS_BKG_AMT_USD) as GBV
                 FROM
                 DM.V_TRANS_DATE_DIM INNER JOIN DM.V_ORDER_TRANS_FACT ON (DM.V_TRANS_DATE_DIM.TRANS_DATE_KEY=DM.V_ORDER_TRANS_FACT.TRANS_DATE_KEY)
                 INNER JOIN DM.V_OPER_UNIT_DIM ON (DM.V_ORDER_TRANS_FACT.OPER_UNIT_KEY=DM.V_OPER_UNIT_DIM.OPER_UNIT_KEY)
                 INNER JOIN DM.V_BUSINESS_PARTNR_DIM ON (DM.V_ORDER_TRANS_FACT.BUSINESS_PARTNR_KEY=DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_KEY)                
                 WHERE
                 (
                 ( ( DM.V_TRANS_DATE_DIM.TRANS_DATE ) BETWEEN (Current Date - 32 months) AND (Current Date)
                 
                 )
                 AND
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_4_NAME  IN  ( 'Expedia Affiliate Network'  )
                 AND DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_TPID  NOT IN  ( 30029, 30031  )

                 )
                 GROUP BY
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME, 
                 DM.V_TRANS_DATE_DIM.TRANS_DATE
                 
                 UNION ALL
                 
                 SELECT
                 
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME as Region,
                 DM.V_TRANS_DATE_DIM.TRANS_DATE as Date,
                 SUM(DM.V_ORDER_TRANS_FACT_ARCHV.ORDER_GROSS_BKG_AMT_USD) as GBV
                 FROM
                 DM.V_TRANS_DATE_DIM INNER JOIN DM.V_ORDER_TRANS_FACT_ARCHV ON (DM.V_TRANS_DATE_DIM.TRANS_DATE_KEY=DM.V_ORDER_TRANS_FACT_ARCHV.TRANS_DATE_KEY)
                 INNER JOIN DM.V_OPER_UNIT_DIM ON (DM.V_ORDER_TRANS_FACT_ARCHV.OPER_UNIT_KEY=DM.V_OPER_UNIT_DIM.OPER_UNIT_KEY)
                 INNER JOIN DM.V_BUSINESS_PARTNR_DIM ON (DM.V_ORDER_TRANS_FACT_ARCHV.BUSINESS_PARTNR_KEY=DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_KEY)
                 WHERE
                 (
                 ( ( DM.V_TRANS_DATE_DIM.TRANS_DATE ) BETWEEN (Current Date - 32 months) AND (Current Date)
                 
                 )
                 AND
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_4_NAME  IN  ( 'Expedia Affiliate Network'  )
                 AND DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_TPID  NOT IN  ( 30029, 30031  )

                 )
                 GROUP BY
                 DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME, 
                 DM.V_TRANS_DATE_DIM.TRANS_DATE")
df = fetch(rs, -1)


data<-df

#reformat Date field
data$Date<-as.Date(data$DATE, "%Y-%m-%d")
maxdate<-as.Date(max(data$Date))

#summarize to one row per date
data<-summaryBy(GBV~REGION+Date, data=data, FUN=sum)
n<-(nrow(data)/3)-1

#number of days to predict
d<-200

print ("add empty rows to dataframe for next d days")
#add empty rows to dataframe for next d days
newrows<-data.frame(REGION=c(rep("EAN Americas",d),rep("EAN - APAC",d),rep("EAN - Europe",d)),Date=as.Date(rep((max(data$Date)+1):(max(data$Date)+d),3),origin="1970-01-01"),GBV.sum=rep(0,(d*3)))
data2 <- rbind(newrows,data)

#create binary variables for seasonality
data2$weekday<-weekdays(data2$Date)
data2$month<-months(data2$Date)
weekdays = c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
month=c("January","February","March","April","May","June","July","August","September","October","November")


createid = function(vector,id) {
  ifelse(vector==id,1,0)
}


day.matrix <- sapply(weekdays,function(x) createid(data2$weekday,x))
dimnames(day.matrix)[[2]] <- c("MON","TUES","WED","THU","FRI","SAT")
month.matrix <- sapply(month,function(x) createid(data2$month,x))
dimnames(month.matrix)[[2]] <- c("JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV") 

data2 <- data.frame(data2,day.matrix,month.matrix)

#sort data, create lag variable and trend
data.sort<-data2[order(data2$REGION, data2$Date),]
data.sort$GBV<-c(data.sort$GBV.sum[-1], NA)
data.sort$TREND<-c(rep(1:(nrow(data2)/3),3))

print ("add empty rows to dataframe for next d days")
#This is a global function apply regression by region
reg.func <- function(data,region,time,row){
  print ("Inside regression by region")	


	#This creates a matrix for the prdictions
	create.matrix <- function(time,row,data){
			dim = seq((n+1),(n+time))
			Intercept =  rep(1,time)
			GBVlag = c(log(data$GBV[row]),rep(0,(time-1)))	
			days = cbind(data$TREND,data$MON,data$TUE,data$WED,data$THU,data$FRI,data$SAT)[dim,]
			months = cbind(data$JAN,data$FEB,data$MAR,data$APR,data$MAY,data$JUN,data$JUL,data$AUG,data$SEP,data$OCT,data$NOV)[dim,]
			cbind(Intercept,GBVlag,days,months)


	}

	#Apply the regression model to the matrix for prediction
	model.apply <- function(time,model,data) {
		mod <- function(x){model$coefficients%*%x}
		sapply(1:(time-1), function(y) data[y+1,2] <<- mod(data[y,]))
		error=2*sd(model$residuals)
		GBVUpper =  data[,2]+error
		GBVLower = data[,2]-error
		data.frame(data,GBVUpper,GBVLower)
	}

	#subset data and create linear models
	sub.dat <- subset(data,REGION==region)
	model <- lm(log(GBV)~log(GBV.sum)+TREND+MON+TUES+WED+THU+FRI+SAT+JAN+FEB+MAR+APR+MAY+JUN+JUL+AUG+SEP+OCT+NOV, data=sub.dat[1:row,])
	mat.dat <- create.matrix(time,row,sub.dat)
	list(models=model.apply(time,model,mat.dat),regions=mat.dat)


}

emea <- reg.func(data.sort,"EAN - Europe",d,n)
output.emea <- cbind(emea$models,Month=format(data.sort$Date[data.sort$REGION=="EAN - Europe"][(n+1):(n+d)],format="%b"))
amer <- reg.func(data.sort,"EAN Americas",d,n)
output.amer <- cbind(amer$models,Month=format(data.sort$Date[data.sort$REGION=="EAN Americas"][(n+1):(n+d)],format="%b"))
apac <- reg.func(data.sort,"EAN - APAC",d,n)
output.apac <- cbind(apac$models,Month=format(data.sort$Date[data.sort$REGION=="EAN - APAC"][(n+1):(n+d)],format="%b"))

print ("creating recent trading adjustments")
##Create recent Trading Adjustments
#create binary variables for seasonality
data$weekday<-weekdays(data$Date)
data$month<-months(data$Date)

createid = function(vector,id) {
  ifelse(vector==id,1,0)
}


day.matrix <- sapply(weekdays,function(x) createid(data$weekday,x))
dimnames(day.matrix)[[2]] <- c("MON","TUES","WED","THU","FRI","SAT")
month.matrix <- sapply(month,function(x) createid(data$month,x))
dimnames(month.matrix)[[2]] <- c("JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV") 

data <- data.frame(data,day.matrix,month.matrix)

#sort data, create lag variable and trend
data.sortadj<-data[order(data$REGION, data$Date),]
data.sortadj$GBV<-c(data.sortadj$GBV.sum[-1], NA)
data.sortadj$TREND<-c(rep(1:(nrow(data)/3),3))


nadj<-(nrow(data.sortadj)/3)-15
dadj<-15

#This is a global function apply regression by region
reg.funcadj <- function(data,region,time,row){
  
  
  
  #This creates a matrix for the prdictions
  create.matrix <- function(time,row,data){
    dim = seq((row+1),(row+time))
    Intercept =  rep(1,time)
    GBVlag = c(log(data$GBV[row]),rep(0,(time-1)))
    days = cbind(data$TREND,data$MON,data$TUE,data$WED,data$THU,data$FRI,data$SAT)[dim,]
    months = cbind(data$JAN,data$FEB,data$MAR,data$APR,data$MAY,data$JUN,data$JUL,data$AUG,data$SEP,data$OCT,data$NOV)[dim,]
    
    
    cbind(Intercept,GBVlag,days,months)
    
    
  }
  
  #Apply the regression model to the matrix for prediction
  model.apply <- function(time,model,data) {
    mod <- function(x){model$coefficients%*%x}
    sapply(1:(time-1), function(y) data[y+1,2] <<- mod(data[y,]))
    error=2*sd(model$residuals)
    GBVUpper =  data[,2]+error
    GBVLower = data[,2]-error
    data.frame(data,GBVUpper,GBVLower)
  }
  
  #subset data and create linear models
  sub.dat <- head(subset(data,REGION==region),n)
  model <- lm(log(GBV)~log(GBV.sum)+TREND+MON+TUES+WED+THU+FRI+SAT+JAN+FEB+MAR+APR+MAY+JUN+JUL+AUG+SEP+OCT+NOV, data=sub.dat[1:row,])
  mat.dat <- create.matrix(time,row,subset(data,REGION==region))
  list(models=model.apply(time,model,mat.dat),regions=mat.dat)
  
  
}

emeaadj <- reg.funcadj(data.sortadj,"EAN - Europe",dadj,nadj)
output.emeaadj <- emeaadj$models
ameradj <- reg.funcadj(data.sortadj,"EAN Americas",dadj,nadj)
output.ameradj <- ameradj$models
apacadj <- reg.funcadj(data.sortadj,"EAN - APAC",dadj,nadj)
output.apacadj <- apacadj$models

output.emeaadj$GBVActual<-log(subset(data.sortadj, REGION=="EAN - Europe")$GBV.sum[((nadj+1):(nadj+dadj))])
emea.adj<-mean(exp(tail(output.emeaadj$GBVActual,dadj-1))-exp(tail(output.emeaadj$GBVlag,dadj-1)))
output.ameradj$GBVActual<-log(subset(data.sortadj, REGION=="EAN Americas")$GBV.sum[((nadj+1):(nadj+dadj))])
amer.adj<-mean(exp(tail(output.ameradj$GBVActual,dadj-1))-exp(tail(output.ameradj$GBVlag,dadj-1)))
output.apacadj$GBVActual<-log(subset(data.sortadj, REGION=="EAN - APAC")$GBV.sum[((nadj+1):(nadj+dadj))])
apac.adj<-mean(exp(tail(output.apacadj$GBVActual,dadj-1))-exp(tail(output.apacadj$GBVlag,dadj-1)))
##adjustment end

print ("plotting booking estimates")
#plot booking estimates
ts.plot(exp(output.emea[,2]))
ts.plot(exp(output.amer[,2]))
ts.plot(exp(output.apac[,2]))

#apply timing cards
timing<-read.table("Timingcard.csv", sep=",",header=TRUE)

#calculate stayed GBV

create.data <- function(time,dat.two,index,adj,region){
  dat.one = matrix(data=0,nrow=time,ncol=time)
  timingsub<-subset(timing,Region==region)
  
  create.nbns <- function(x,time,dat.one,dat.two,index) {
    timingsub2<-subset(timingsub,Month==as.character(dat.two$Month[x]))
    (exp(dat.two[x,index])+adj)*timingsub2$Pct.Total[1:(time-x+1)]
  }
  
  store <- sapply(1:time,function(x) dat.one[x,x:d] <<- create.nbns(x,time,dat.one,dat.two,index))
  dat.one
}



emea.nbns <- create.data(d,output.emea,2,emea.adj,"EMEA")
emea.nbnsupper <- create.data(d,output.emea,21,emea.adj,"EMEA")
emea.nbnslower <- create.data(d,output.emea,22,emea.adj,"EMEA")


amer.nbns <- create.data(d,output.amer,2,amer.adj,"AMER")
amer.nbnsupper <- create.data(d,output.amer,21,amer.adj,"AMER")
amer.nbnslower <- create.data(d,output.amer,22,amer.adj,"AMER")


apac.nbns <- create.data(d,output.apac,2,apac.adj,"APAC")
apac.nbnsupper <- create.data(d,output.apac,21,apac.adj,"APAC")
apac.nbnslower <- create.data(d,output.apac,22,apac.adj,"APAC")


#Function to create data frame by region
create.region.frame <- function(region,date,time,data.one,data.two,data.three,origin="1970-01-01"){
	data.frame(REGION=rep(region,time),Date=as.Date(c(date+1):(date+time),origin=origin),GBV=colSums(data.one),GBVUpper=colSums(data.two),GBVLower=colSums(data.three))
}

emea=create.region.frame("EAN - Europe",maxdate,d,emea.nbns,emea.nbnsupper,emea.nbnslower)
amer=create.region.frame("EAN Americas",maxdate,d,amer.nbns,amer.nbnsupper,amer.nbnslower)
apac =create.region.frame("EAN - APAC",maxdate,d,apac.nbns,apac.nbnsupper,apac.nbnslower)


nbns<-rbind(emea,amer,apac)
nbns$StayType<-"Not Booked Not Stayed"
write.table(nbns,"amendTest.txt")

print ("stay data query")
#Bring in Stay Data

rs = dbSendQuery(conn, 
                 
                 "SELECT
  SUM(DM.V_LODG_RM_NIGHT_TRANS_FACT.GROSS_BKG_AMT_USD) as GBV,
  sum(DM.V_LODG_RM_NIGHT_TRANS_FACT.MARGN_AMT_USD) as Margin,
  DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME as Region,
  DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME as Bmodel,
  (CASE WHEN DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME IN ('Agency') Then DM.V_BEGIN_USE_DATE_DIM.BEGIN_USE_DATE else DM.V_USE_DATE_DIM.USE_DATE end) as Date
FROM
  DM.V_PRODUCT_LN_DIM INNER JOIN DM.V_LODG_RM_NIGHT_TRANS_FACT ON (DM.V_PRODUCT_LN_DIM.PRODUCT_LN_KEY=DM.V_LODG_RM_NIGHT_TRANS_FACT.PRODUCT_LN_KEY)
   INNER JOIN DM.V_BEGIN_USE_DATE_DIM ON (DM.V_BEGIN_USE_DATE_DIM.BEGIN_USE_DATE_KEY=DM.V_LODG_RM_NIGHT_TRANS_FACT.BEGIN_USE_DATE_KEY)
   INNER JOIN DM.V_USE_DATE_DIM ON (DM.V_USE_DATE_DIM.USE_DATE_KEY=DM.V_LODG_RM_NIGHT_TRANS_FACT.USE_DATE_KEY)
   INNER JOIN DM.V_OPER_UNIT_DIM ON (DM.V_LODG_RM_NIGHT_TRANS_FACT.OPER_UNIT_KEY=DM.V_OPER_UNIT_DIM.OPER_UNIT_KEY)
  
WHERE
  (
   DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_4_NAME  IN  ( 'Expedia Affiliate Network'  )
   AND
   (
    (
     DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME  IN  ( 'Agency'  )
     AND
     DM.V_BEGIN_USE_DATE_DIM.BEGIN_USE_DATE  BETWEEN  DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days ))) 
    				AND  last_day(DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days )))+ 5 months)
    )
    OR
    (
     DM.V_USE_DATE_DIM.USE_DATE   BETWEEN DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days ))) 
    				AND  last_day(DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days )))+ 5 months)
     AND
     DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME  IN  ( 'Merchant'  )
    )
   )
  )
GROUP BY
  DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME, 
  DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME, 
  (CASE WHEN DM.V_PRODUCT_LN_DIM.BUSINESS_MODEL_NAME IN ('Agency') Then DM.V_BEGIN_USE_DATE_DIM.BEGIN_USE_DATE else DM.V_USE_DATE_DIM.USE_DATE end)")

dr = fetch(rs, -1)
stay.data<-dr

names(stay.data)<-c("GBV", "MARGIN", "REGION", "BMODEL", "DATE")
stay.data$Date<-as.Date(stay.data$DATE, "%Y-%m-%d")
staydata.sort<-stay.data[order(stay.data$REGION, stay.data$Date),]

print ("Rest of PL and adjustments")
#bring in Rest of P&L assumptions and adjustments
PL.assump<-read.table("RestofPL.csv", sep=",",header=TRUE)

print ("SQL for adjustments and New Biz")
rs = dbSendQuery(conn, 
                 "SELECT SUPER_REGION, MONTH_BEGIN_DATE, ADJ_STAY_GBV, ADJ_STAY_MC, NEW_BIZ_STAY_GBV, NEW_BIZ_STAY_MC
                  FROM EANSCRATCH.EAN_SUPER_REGION_STAY_PLAN_ACT_FCST
                  WHERE MONTH_BEGIN_DATE BETWEEN  DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days ))) 
  						   AND  last_day(DATE(RTRIM(CHAR(QUARTER(Current Date- 1 days )*3 - 2)) || '/1/' || CHAR(YEAR(Current Date- 1 days )))+ 5 months)
                  WITH UR")
ds = fetch(rs, -1)
PL.adjust<-ds

#PL.adjust<-read.table("unadjusted for fpa.txt", sep=",",header=TRUE)
#PL.adjust$Month3<-as.Date(PL.adjust$Month,"%d/%m/%Y")
# PL.adjust$Month = as.Date(as.character(PL.adjust$Month,"%Y-%m-%d")) # TESTING
#if(is.na(format(as.Date(as.character(PL.adjust$Month3[1]),"%Y-%m-%d"),format="%b"))==TRUE) {
PL.adjust$Month2<-format(as.Date(PL.adjust$MONTH_BEGIN_DATE,"%Y-%m-%d"),format="%b")
#} else{PL.adjust$Month2<-format(as.Date(as.character(PL.adjust$Month3),"%Y-%m-%d"),format="%b") }
PL.adjust$SUPER_REGION[PL.adjust$SUPER_REGION=="AMER"]<-"EAN Americas"
PL.adjust$SUPER_REGION[PL.adjust$SUPER_REGION=="EMEA"]<-"EAN - Europe"
PL.adjust$SUPER_REGION[PL.adjust$SUPER_REGION=="APAC"]<-"EAN - APAC"  
  

  

#label stay or book not stay
staydata.sort$StayType<-0
staydata.sort$StayType <- ifelse(staydata.sort$Date <= maxdate, "Stayed", "Booked Not Stayed" )
#staydata.sort$StayType<-sapply(1:nrow(staydata.sort),function(x) if (staydata.sort$Date[x]<=maxdate) {staydata.sort$StayType[x]="Stayed"} else {staydata.sort$StayType[x]="Booked Not Stayed"})
#staydata.sort$StayType
#calculate Agency Margin
staydata.sort$Monthtrunc<-format(staydata.sort$Date,format="%b")
staydata.sort2<-merge(staydata.sort,PL.assump, by.x = c("Monthtrunc","REGION"), by.y=c("Month","Region"),all.x=T)
staydata.sort2$Marginnew <- ifelse(staydata.sort2$BMODEL == "Agency", staydata.sort2$GBV*staydata.sort2$AgencyRM,staydata.sort2$MARGIN)

#calculate book not stay minus cancel rate
staydata.sort2$GBVcan <- ifelse(staydata.sort2$StayType=="Booked Not Stayed",staydata.sort2$GBV*.845,staydata.sort2$GBV)
staydata.sort2$Margincan <-  ifelse(staydata.sort2$StayType=="Booked Not Stayed",staydata.sort2$Marginnew*.845,staydata.sort2$Marginnew)


#calc margin for nbns
print ("calc margin for nbns")
nbns$Month<-format(nbns$Date,format="%b")
nbns2<-merge(nbns,PL.assump, by.x = c("Month","REGION"), by.y=c("Month","Region"),all.x=T)

nbns2$Margin<-nbns2$GBV*nbns2$RM
nbns2$MarginUpper<-nbns2$GBVUpper*nbns2$RM
nbns2$MarginLower<-nbns2$GBVLower*nbns2$RM

#join not booked not stayed data
staydata.sort2$GBVcanUpper<-staydata.sort2$GBVcan
staydata.sort2$GBVcanLower<-staydata.sort2$GBVcan
staydata.sort2$MargincanUpper<-staydata.sort2$Margincan
staydata.sort2$MargincanLower<-staydata.sort2$Margincan
staydata.sort2<-summaryBy(GBVcan+GBVcanUpper+GBVcanLower+Margincan+MargincanUpper+MargincanLower~REGION+Date+StayType, data=staydata.sort2, FUN=sum)
names(staydata.sort2)<-c("REGION", "Date", "StayType", "GBV", "GBVUpper","GBVLower","Margin","MarginUpper","MarginLower")
nbns2<-summaryBy(GBV+GBVUpper+GBVLower+Margin+MarginUpper+MarginLower~REGION+Date+StayType, data=nbns2, FUN=sum)
names(nbns2)<-c("REGION", "Date", "StayType",  "GBV", "GBVUpper","GBVLower","Margin","MarginUpper","MarginLower")
GBVoutput<-rbind(nbns2,staydata.sort2)
GBVoutput$Month<-format(GBVoutput$Date,format="%Y-%m-01")
GBVoutput$Month2<-format(GBVoutput$Date,format="%b")

#summarize by month **test
testoutput<-summaryBy(GBV+GBVUpper+GBVLower+Margin+MarginUpper+MarginLower~Month, data=GBVoutput, FUN=sum)
test.sort<-testoutput[order(testoutput$Month),]
print ("plot")
plot(test.sort$GBV.sum,type="l")

#join historical booked data to future booked data
bookedoutput.emea<-data.frame(REGION=rep("EAN - Europe",d),Date=as.Date(c((maxdate+1):(maxdate+d)),origin="1970-01-01"),GBV=exp(output.emea[,2]))
bookedoutput.amer<-data.frame(REGION=rep("EAN Americas",d),Date=as.Date(c((maxdate+1):(maxdate+d)),origin="1970-01-01"),GBV=exp(output.amer[,2]))                                                                                                                  
bookedoutput.apac<-data.frame(REGION=rep("EAN - APAC",d),Date=as.Date(c((maxdate+1):(maxdate+d)),origin="1970-01-01"),GBV=exp(output.apac[,2]))

bookedoutput<-rbind(bookedoutput.emea,bookedoutput.amer,bookedoutput.apac)
data2<-data2
for(i in 1:nrow(data2))
{
  data2$GBV.sum[i]<-if(data2$Date[i]>maxdate) {bookedoutput$GBV[bookedoutput$REGION==data2$REGION[i]&bookedoutput$Date==data2$Date[i]]} else {
    data2$GBV.sum[i]}
}
data2$Month<-format(data2$Date,format="%Y-%m-01")
data2$Month2<-format(data2$Date,format="%b")
data2$Year<-format(data2$Date,format="%Y")
data2013<- subset(data2, Year>= format(maxdate,format="%Y") )
data3<-summaryBy(GBV.sum~Month2+REGION, data=data2013, FUN=sum)

#Profit Merge
GBVoutputmonth<-summaryBy(GBV+GBVUpper+GBVLower+Margin+MarginUpper+MarginLower~Month2+Month+REGION+StayType, data=GBVoutput, FUN=sum)
outputadj<-merge(GBVoutputmonth,PL.adjust, by.x = c("Month2","REGION"), by.y=c("Month2","SUPER_REGION"),all.x=T,incomparables=0)
outputadj$ADJ_STAY_GBV<-ifelse(is.na(outputadj$ADJ_STAY_GBV)==TRUE,0,outputadj$ADJ_STAY_GBV)
outputadj$ADJ_STAY_MC<-ifelse(is.na(outputadj$ADJ_STAY_MC)==TRUE,0,outputadj$ADJ_STAY_MC)
#outputadj$NEW_BIZ_STAY_GBV<-ifelse(is.na(outputadj$NEW_BIZ_STAY_GBV)==TRUE,0,outputadj$NEW_BIZ_STAY_GBV)
#outputadj$NEW_BIZ_STAY_MC<-ifelse(is.na(outputadj$NEW_BIZ_STAY_MC)==TRUE,0,outputadj$NEW_BIZ_STAY_MC)
output<-merge(outputadj,PL.assump, by.x = c("Month2","REGION"), by.y=c("Month","Region"),all.x=T,incomparables=0)
output$count<-1
Monthcount<-summaryBy(count~Month+Month2+REGION, data=output, FUN=sum)
output2<-merge(output,Monthcount, by.x = c("Month2","REGION"), by.y=c("Month2","REGION"),all.x=T,incomparables=0)
output3<-merge(output2,data3, by.x = c("Month2","REGION"), by.y=c("Month2","REGION"),all.x=T,incomparables=0)

#weight abs values by GBV.sum
print ("weight abs values by GBV")
weights<-summaryBy(GBV.sum~Month2+REGION, data=output3, FUN=sum)
output4<-merge(output3,weights, by.x = c("Month2","REGION"), by.y=c("Month2","REGION"),all.x=T,incomparables=0)
output4$GBVweight<-output4$GBV.sum/output4$GBV.sum.sum.y
output4$bkgnew<-output4$Bkg*output4$GBVweight
output4$ORMnew<-output4$ORM*output4$GBVweight
output4$GBVadjnew<-output4$ADJ_STAY_GBV*output4$GBVweight
output4$MCadjnew<-output4$ADJ_STAY_MC*output4$GBVweight
#output4$GBVnewbiz<-output4$NEW_BIZ_STAY_GBV*output4$GBVweight
#output4$MCnewbiz<-output4$NEW_BIZ_STAY_MC*output4$GBVweight
output4$BookedGBV<-output4$GBV.sum.sum.x*output4$GBVweight
output4$GBVnew<-output4$GBV.sum+output4$GBVadjnew
output4$GBVnewUpper<-output4$GBVUpper.sum+output4$GBVadjnew
output4$GBVnewLower<-output4$GBVLower.sum+output4$GBVadjnew
output4$MC2<-output4$Margin.sum+output4$bkgnew+output4$ORMnew-(output4$COS*output4$BookedGBV)-(output4$Comm*output4$GBV.sum)
output4$MCnew<-output4$MC2+output4$MCadjnew
output4$MCUpper<-output4$MarginUpper.sum+output4$bkgnew+output4$ORMnew-(output4$COS*output4$BookedGBV)-(output4$Comm*output4$GBVUpper.sum)
output4$MCnewUpper<-output4$MCUpper+output4$MCadjnew
output4$MCLower<-output4$MarginLower.sum+output4$bkgnew+output4$ORMnew-(output4$COS*output4$BookedGBV)-(output4$Comm*output4$GBVLower.sum)
output4$MCnewLower<-output4$MCLower+output4$MCadjnew


#output file for jeremy
#print ("output file for jeremy")
#unadjoutput<-summaryBy(GBV.sum+MC2~REGION+Month.x.y, data=output4, FUN=sum)
#unadjoutput$GBVadj<-0
#unadjoutput$Mcadj<-0

#unadjoutput2 <- merge(subset(unadjoutput,select=c("REGION","Month.x.y","GBV.sum.sum","MC2.sum")),
#            subset(PL.adjust,select=c("Region","Month","GBVadj","Mcadj")),by.x=c("REGION","Month.x.y"),by.y=c("Region","Month"),all.x=TRUE,incomparables=0)

#unadjoutput2$Month.x.y<-as.Date(unadjoutput2$Month.x.y)
#names(unadjoutput2)<-c("Region", "Month", "GBV", "MC","GBVadj", "Mcadj")

#unadjoutput2$GBVadj<-ifelse(is.na(unadjoutput2$GBVadj)==TRUE,0,unadjoutput2$GBVadj)
#unadjoutput2$Mcadj<-ifelse(is.na(unadjoutput2$Mcadj)==TRUE,0,unadjoutput2$Mcadj)

#write.table(unadjoutput2, file = "unadjusted for fpa.txt", sep = ",", col.names = T,row.names = F)




#cleanup file, add new biz and output
print ("cleanup file, add new biz and output")
outputfinal <- subset(output4, select = c("REGION","Month.x","StayType","GBV.sum","GBVUpper.sum","GBVLower.sum","MC2","MCUpper","MCLower","GBVnew","GBVnewUpper","GBVnewLower","MCnew","MCnewUpper","MCnewLower", "GBVadjnew", "MCadjnew") )
names(outputfinal)<-c("Region", "Month", "StayType", "GBV", "GBVUpper","GBVLower","MC", "MCUpper","MCLower","GBVadj","GBVadjUpper","GBVadjLower","MCadj","MCadjUpper","MCadjLower", "GBVadjvalue","MCadjvalue")

newbiz<-data.frame(Region=PL.adjust$SUPER_REGION,Month=PL.adjust$MONTH_BEGIN_DATE,StayType=rep("New Business",nrow(PL.adjust)),GBV=PL.adjust$NEW_BIZ_STAY_GBV, GBVUpper=PL.adjust$NEW_BIZ_STAY_GBV,GBVLower=PL.adjust$NEW_BIZ_STAY_GBV,MC=PL.adjust$NEW_BIZ_STAY_MC, MCUpper=PL.adjust$NEW_BIZ_STAY_MC,MCLower=PL.adjust$NEW_BIZ_STAY_MC,GBVadj=PL.adjust$NEW_BIZ_STAY_GBV,GBVadjUpper=PL.adjust$NEW_BIZ_STAY_GBV,GBVadjLower=PL.adjust$NEW_BIZ_STAY_GBV,MCadj=PL.adjust$NEW_BIZ_STAY_MC,MCadjUpper=PL.adjust$NEW_BIZ_STAY_MC,MCadjLower=PL.adjust$NEW_BIZ_STAY_MC, GBVadjvalue=rep(0,nrow(PL.adjust)),MCadjvalue=rep(0,nrow(PL.adjust)))
outputfinal <- rbind(outputfinal,newbiz)


outputfinal$Region<-as.character(outputfinal$Region)
outputfinal$Region[outputfinal$Region=="EAN Americas"]<-"AMER"
outputfinal$Region[outputfinal$Region=="EAN - Europe"]<-"EMEA"
outputfinal$Region[outputfinal$Region=="EAN - APAC"]<-"APAC"







outputfinal$Createdate<-Sys.Date()

print ("Write file")
filename<-paste(paste("BookedMonthlyForecast",Sys.Date(),sep="-"),".csv",sep="")
write.table(outputfinal, file = filename, sep = ",", col.names = F,row.names = F)

filename<-paste(paste("BookedMonthlyForecast"),".csv",sep="")
write.table(outputfinal, file = filename, sep = ",", col.names = T,row.names = F)




