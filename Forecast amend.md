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


## compute model
		model.build <- function(time,row,data,region,name.file){
		sub.dat <- subset(data,REGION==region)
		model <- lm(log(GBV)~log(GBV.sum)+Trend+MON+TUES+WED+THU+FRI+SAT+JAN+FEB+MAR+APR+MAY+JUN+JUL+AUG+SEP+OCT+NOV, 	data=sub.dat[1:row,])

	#save the model		
	dput(model, name.file)
		}

## create coefficients and residuals and save in working directory

	model.build(d,n,data.sort,"EAN - Europe","ean.model")
	model.build(d,n,data.sort,"EAN Americas","amer.model")
	model.build(d,n,data.sort,"EAN - APAC","apac.model")