
## Apply the regression model to the matrix for prediction
reg.func <- function(data,region,time,row){
    
	model.apply <- function(time,data,model.name) {
		model <- dget(model.name)
		coefficients = model$coefficients
    	mod <- function(x){coefficients%*%x}
    	sapply(1:(time-1), function(y) data[y+1,2] <<- mod(data[y,]))
    	error=2*sd(model$residuals)
    	GBVUpper =  data[,2]+error
    	GBVLower = data[,2]-error
    	data.frame(data,GBVUpper,GBVLower)
    	}

## Subset data and create linear models
    sub.dat <- subset(data,REGION==region)
    mat.dat <- create.matrix(time,row,sub.dat)
	list(models=model.apply(time,data,model.name),regions=sub.dat)
	}


