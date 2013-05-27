## Separate out the model build and run monthly
	
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
             ( ( DM.V_TRANS_DATE_DIM.TRANS_DATE ) BETWEEN (Current Date - 24 months) AND (Current Date)

             )
             AND
             DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_4_NAME  IN  ( 'Expedia Affiliate Network'  )
             AND DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_TPID  NOT IN  ( 30029, 30031  )
             AND DM.V_BUSINESS_PARTNR_DIM.PARNT_BUSINESS_PARTNR_ID  <>  306895
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
             ( ( DM.V_TRANS_DATE_DIM.TRANS_DATE ) BETWEEN (Current Date - 24 months) AND (Current Date)

             )
             AND
             DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_4_NAME  IN  ( 'Expedia Affiliate Network'  )
             AND DM.V_BUSINESS_PARTNR_DIM.BUSINESS_PARTNR_TPID  NOT IN  ( 30029, 30031  )
             AND DM.V_BUSINESS_PARTNR_DIM.PARNT_BUSINESS_PARTNR_ID  <>  306895
             )
             GROUP BY
             DM.V_OPER_UNIT_DIM.OPER_UNIT_LVL_5_NAME, 
             DM.V_TRANS_DATE_DIM.TRANS_DATE")
	df = fetch(rs, -1)


	data<-df

## Reformat Date field
	data$Date<-as.Date(data$DATE, "%Y-%m-%d")
	maxdate<-as.Date(max(data$Date))

## Summarize to one row per date
	data<-summaryBy(GBV~REGION+Date, data=data, FUN=sum)
	n<-(nrow(data)/3)-1

## Number of days to predict
	d<-200

## Add empty rows to dataframe for next 90 days
	newrows<-data.frame(REGION=c(rep("EAN Americas",d),rep("EAN - APAC",d),rep("EAN - Europe",d)),Date=as.Date(rep((max(data$Date)+1):(max(data$Date)+d),3),origin="1970-01-01"),GBV.sum=rep(0,(d*3)))
	data <- rbind(newrows,data)

## Create binary variables for seasonality
	data$weekday<-weekdays(data$Date)
	data$month<-months(data$Date)
	weekdays = c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
	month=c("January","February","March","April","May","June","July","August","September","October","November")


	createid = function(vector,id) {
		ifelse(vector==id,1,0)
	}


	day.matrix <- sapply(weekdays,function(x) createid(data$weekday,x))
	dimnames(day.matrix)[[2]] <- c("MON","TUES","WED","THU","FRI","SAT")
	month.matrix <- sapply(month,function(x) createid(data$month,x))
	dimnames(month.matrix)[[2]] <- c("JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV") 

	data <- data.frame(data,day.matrix,month.matrix)

## Sort data, create lag variable and trend
	data.sort<-data[order(data$REGION, data$Date),]
	data.sort$GBV<-c(data.sort$GBV.sum[-1], NA)
	data.sort$TREND<-c(rep(1:(nrow(data)/3),3))
## Compute model
		model.build <- function(time,row,data,region,name.file){
		sub.dat <- subset(data,REGION==region)
		model <- lm(log(GBV)~log(GBV.sum)+Trend+MON+TUES+WED+THU+FRI+SAT+JAN+FEB+MAR+APR+MAY+JUN+JUL+AUG+SEP+OCT+NOV, 	data=sub.dat[1:row,])

	#save the model		
	dput(model, name.file)
		}

## Create coefficients and residuals and save in working directory

	model.build(d,n,data.sort,"EAN - Europe","ean.model")
	model.build(d,n,data.sort,"EAN Americas","amer.model")
	model.build(d,n,data.sort,"EAN - APAC","apac.model")
