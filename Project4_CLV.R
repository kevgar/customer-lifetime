# download.file('http://ballings.co/hidden/aCRM/data/chapter7/customers.csv', 'customers.csv')
# download.file('http://ballings.co/hidden/aCRM/data/chapter7/products.csv', 'products.csv')
# download.file('http://ballings.co/hidden/aCRM/data/chapter7/stores.csv', 'stores.csv')
# download.file('http://ballings.co/hidden/aCRM/data/chapter7/transactiondetails.csv', 'transactiondetails.csv')
# download.file('http://ballings.co/hidden/aCRM/data/chapter7/transactions.csv', 'transactions.csv')

    clvR <- function(start_ind,
                 end_ind,
                 start_dep,
                 end_dep,
                 evaluate=TRUE) {
  f <- "%m/%d/%Y"
  t1 <- as.Date(start_ind, f)
  t2 <- as.Date(end_ind, f)
  t3 <- as.Date(start_dep, f)
  t4 <- as.Date(end_dep, f) #dump date
  length_ind <- t2 - t1
  #Load all required packages
  for (i in c("randomForest", "data.table", "lift", "dummy")) {
    if (!require(i,character.only=TRUE,quietly=TRUE)) {
      install.packages(i,
                       repos='http://cran.rstudio.com',
                       quiet=TRUE)
      require(i,
              character.only=TRUE,
              quietly=TRUE)
    }
  }
  
  readAndPrepareData <- function(train=TRUE,...){
    
    #DATA UNDERSTANDING
    ###############################################################
    cat("Reading in data:")
    time <- Sys.time()
    
    f <- "%Y-%m-%d" 
    setClass('fDate') 
    setAs(from="character",
          to="fDate",
          def=function(from) as.Date(from, format=f))
    customers <- read.csv("customers.csv", header = T, sep = ",", 
                          colClasses = c(custid = "character",
                                         rent = "factor",
                                         garden = "factor",
                                         gender = "factor",
                                         dob = "fDate"))
    products <- as.data.frame(fread("products.csv", header = T, sep = ",",
                                    colClasses = c(SKU = "character",
                                                   family = "factor",
                                                   price = "numeric",
                                                   cost = "numeric")))
    stores <- as.data.frame(fread("stores.csv", header = T, sep = ",",
                                  colClasses = c(ZIP = "factor",
                                                 storeid = "character")))
    
    transactiondetails <- as.data.frame(fread("transactiondetails.csv", header = T, sep = ",",
                                              colClasses = c(receiptnbr = "character", 
                                                             SKU = "character", 
                                                             quantity = "integer")))
    
    transactions <- read.csv("transactions.csv", header = T, sep = ",",
                             colClasses = c(custid = "character", 
                                            receiptnbr = "character", 
                                            date = "fDate", 
                                            storeid = "character", 
                                            paidcash = "numeric", 
                                            paidcheck = "numeric", 
                                            paidcard = "numeric", 
                                            total = "numeric"))
    cat(format(round(as.numeric(Sys.time()- time),1),nsmall=1,width=4),
        attr(Sys.time()- time,"units"), "\n")
    cat("Preparing basetable:")
    time <- Sys.time()
    ###############################################################
    #DATA PREPARATION
    
    if (train==FALSE){
      dots <- list(...) # list(...) evaluates all arguments and
      # returns them in a named list
      t2 <- dots$end_ind
      t1 <- t2 - dots$length_ind
      rm(dots)
    }
    
    transactionsIND <- transactions[transactions$date >= t1 &
                                      transactions$date <= t2, ]
    if (train==TRUE){
      transactionsDEP <- transactions[transactions$date >= t3 &
                                        transactions$date <= t4, ]
    }
    
    if (train==TRUE){
      transactionsDEP <- data.table(transactionsDEP)
      
      transactionsDEP_agg <- transactionsDEP[,.(DaysBtwVisits=sd(date,na.rm = T), 
                                                Earliest=min(date, na.rm = T),
                                                Latest=max(date, na.rm = T),
                                                Amount=sum(total)),
                                             by=custid]
      
      activecustomers <- transactionsDEP_agg[transactionsDEP_agg$DaysBtwVisits <= 365 & 
                                               transactionsDEP_agg$Latest >= t4 - 90, "custid"]
      
      clvdependent <- transactionsDEP[transactionsDEP$custid %in% activecustomers$custid, ]
      
      clvdependent2 <- transactiondetails[clvdependent$receiptnbr %in% transactiondetails$receiptnbr, ]
      
      clvdependent3 <- products[clvdependent2$SKU %in% products$SKU, ]
      
      SKUaggregate <- aggregate(list(SumQuantity=clvdependent2$quantity), 
                                by=list(SKU=clvdependent2$SKU,receiptnbr=clvdependent2$receiptnbr),
                                sum)
      
      prodtransdetDEP <- merge(SKUaggregate, clvdependent3, by = "SKU", all.x = T) 
      
      DEPclv <- prodtransdetDEP[clvdependent$receiptnbr %in% prodtransdetDEP$receiptnbr, ]
      
      DEPclvagg <- aggregate(list(TotalCost=DEPclv$cost, TotalPrice=DEPclv$price),
                             by=list(receiptnbr=DEPclv$receiptnbr),
                             sum)
      DEPclvagg$Profit <- DEPclvagg$TotalPrice - DEPclvagg$TotalCost
      
      dependentclv <- data.frame(merge(clvdependent, DEPclvagg, by = "receiptnbr"))
      
      dependentclv$dpd0 <- as.numeric(dependentclv$date - t3)
      dependentclv$denominator <- (1 + 0.125)^(dependentclv$dpd0/356)
      dependentclv$CLV <- dependentclv$Profit/(dependentclv$denominator)
      dependentclv <- dependentclv[complete.cases(dependentclv),]
      
      responseCLV <- aggregate(list(CLV=dependentclv$CLV),
                               by=list(custid=dependentclv$custid),
                               sum)
    }
    
    #Frequency of purchase
    frequency <- aggregate(transactionsIND[,"custid", drop = F],
                           by = list(custid = transactionsIND$custid),
                           length)
    colnames(frequency)[2] <- "FrequencyOfPurchase"
    
    #Recency of purchase
    MAX_purchase_date <- aggregate(transactionsIND[,"date",drop=FALSE],
                                   by=list(custid=transactionsIND$custid),
                                   max)
    colnames(MAX_purchase_date)[2] <- "MAX_purchase_date"
    
    REC_purchase <- data.frame(custid = MAX_purchase_date$custid,
                               REC_purchase= as.numeric(t2) -
                                 as.numeric(MAX_purchase_date$MAX_purchase_date),
                               stringsAsFactors=FALSE)
    
    #Length of relationship
    MIN_purchase_date <- aggregate(transactionsIND[,"date",drop=FALSE],
                                   by=list(custid=transactionsIND$custid),
                                   min)
    colnames(MIN_purchase_date)[2] <- "MIN_purchase_date"
    LOR <- data.frame(custid=MIN_purchase_date$custid,
                      LOR= as.numeric(t2) -
                        as.numeric(MIN_purchase_date$MIN_purchase_date),
                      stringsAsFactors=FALSE)
    
    #Variance purchase time
    VAR_purchase <- aggregate(transactionsIND[,"date",drop=FALSE],
                              by=list(custid=transactionsIND$custid),
                              var)
    colnames(VAR_purchase)[2] <- "VAR_purchase_date"
    NAs <- is.na(VAR_purchase$VAR_purchase_date)
    VAR_purchase$VAR_purchase_date[NAs] <- 0
    
    #Merge stores with transactionIND
    transactionsIND <- merge(transactionsIND, stores, by = 'storeid', all.x = T)
    
    #Merge customers with stores and transactionsIND
    transactionsIND <- merge(transactionsIND, customers, by='custid', all.x = T)
    
    #Age
    now <- Sys.Date()
    transactionsIND$Age <- (now - transactionsIND$dob)/365
    
    CustAge <- aggregate(transactionsIND[,"Age",drop = F],
                         by = list(custid = transactionsIND$custid),
                         mean)
    
    
    #Monetary
    Monetary <- aggregate(transactionsIND[,"total",drop = F],
                          by = list(custid = transactionsIND$custid),
                          sum)
    
    
    #Gender(Female=2, Male=1)
    transactionsIND$gender <- as.numeric(transactionsIND$gender)
    CustGender <- aggregate(transactionsIND[,"gender",drop = F],
                            by = list(custid = transactionsIND$custid),
                            mean)
    
    #Garden(Yes=2, No=1)
    transactionsIND$garden <- as.numeric(transactionsIND$garden)
    CustGarden <- aggregate(transactionsIND[,"garden",drop = F],
                            by = list(custid = transactionsIND$custid),
                            mean)
    
    #Rent(Yes=2, No=1)
    transactionsIND$rent <- as.numeric(transactionsIND$rent)
    CustRent <- aggregate(transactionsIND[,"rent",drop = F],
                          by = list(custid = transactionsIND$custid),
                          mean)
    
    
    #Create dummies for ZIP
    catstransIND <- categories(transactionsIND[,c('ZIP', 'rent')], p = "all")
    transINDdummies <- dummy(transactionsIND[,c('ZIP', 'rent')],
                             object=catstransIND,
                             int=TRUE)
    ZIPIND <- data.frame(transactionsIND,transINDdummies)
    
    ZIPIND <- ZIPIND[,c(1, 15:43)]
    
    ZIPIND <- aggregate(ZIPIND[,!colnames(ZIPIND) %in% 'custid'],
                        by=list(custid=ZIPIND$custid),
                        sum)
    
    #Aggregate transdetail on receiptnbr and SKU
    transactiondetails <- data.table(transactiondetails)
    transdetIND <- data.frame(transactiondetails[,.(quantity=sum(quantity)), by =.(receiptnbr,SKU)])
    
    #Merge products with transactiondetails
    prod_transdet <- merge(products, transdetIND, by='SKU', all.y = T)
    
    #Merge transactionsIND with prod_transdet
    df <- merge(transactionsIND, prod_transdet, by ='receiptnbr', all.y = T)
    
    #Create predictor for family
    df$family <- as.character(df$family)
    df_agg <- aggregate(df[,"family",drop = F], by=list(custid = df$custid), length)
    
    # Merge independents and dependent
    if (train==TRUE){
      data <- list(frequency,
                   REC_purchase,
                   LOR,
                   VAR_purchase,
                   CustAge,
                   Monetary,
                   CustGender,
                   CustGarden,
                   CustRent,
                   ZIPIND,
                   df_agg,
                   responseCLV)
    } else {
      data <- list(frequency,
                   REC_purchase,
                   LOR,
                   VAR_purchase,
                   CustAge,
                   Monetary,
                   CustGender,
                   CustGarden,
                   CustRent,
                   ZIPIND,
                   df_agg)
    }
    basetable <- Reduce(function(x,y) merge(x,y,by='custid'),
                        data)
    
    if (train==TRUE){
      basetable$custid <- NULL
      CLV <- basetable$CLV
      basetable$CLV <- NULL
    }
    
    cat(format(round(as.numeric(Sys.time()- time),1),nsmall=1,width=4),
        attr(Sys.time()- time,"units"), "\n")
    #Our basetable is now ready and we can
    #move on the modeling phase
    if (train==TRUE){
      return(list(predictors=basetable, CLV=CLV))
    } else {
      return(basetable)
    }
  }#end readAndPrepareData
  basetable <- readAndPrepareData()
  
  ###############################################################
  
  if (evaluate==TRUE){
    cat("Evaluating model:")
    time <- Sys.time()
    #We'll be using random forest and hence we will
    #Split the data in a train and test set
    ind <- 1:nrow(basetable$predictors)
    indTRAIN <- sample(ind,round(0.5*length(ind)))
    indTEST <- ind[-indTRAIN]
    #Fit the random forest on the training set
    rf <- randomForest(x=basetable$predictors[indTRAIN,],
                       y=basetable$CLV[indTRAIN],
                       ntree = 1000)
    #Deploy the forest on the test set
    pred <- predict(rf,basetable$predictors[indTEST,])
    
    cat(format(round(as.numeric(Sys.time()- time),1),nsmall=1,width=4),
        attr(Sys.time()- time,"units"), "\n")
    cat(" Number of predictors:", ncol(basetable$predictors[indTRAIN,]),
        "predictors\n")
    cat(" RMSEP:",
        round(sqrt(mean((pred-basetable$CLV[indTEST])^2)),4),"\n")
    cat(" MAE:",
        round(mean(abs((pred-basetable$CLV[indTEST]))),4),"\n")
    cat(" Top decile lift of:",
    round(TopDecileLift(pred,basetable$CLV[indTEST]),4),"\n")
  }
  cat("Creating model:")
  time <- Sys.time()
  #Build model on all data
  rf <- randomForest(x=basetable$predictors,
                     y=basetable$CLV)
  cat(format(round(as.numeric(Sys.time()- time),1),nsmall=1,width=4),
      attr(Sys.time()- time,"units"), "\n")
  l <- list(rf = rf,
            readAndPrepareData = readAndPrepareData,
            f = f,
            length_ind = length_ind)
  class(l) <- "clvR"
  return(l)
}

clvModel <- clvR(start_ind="09/27/2012",
                 end_ind="04/09/2013",
                 start_dep="04/10/2013",
                 end_dep="09/27/2015",
                 evaluate=TRUE)

predict.clvR <- function(object, dumpDate) {
  #Load all required packages
  for (i in c("randomForest", "data.table", "lift", "dummy")) {
    if (!require(i,character.only=TRUE,quietly=TRUE)) {
      install.packages(i,
                       repos='http://cran.rstudio.com',
                       quiet=TRUE)
      require(i,
              character.only=TRUE,
              quietly=TRUE)
    }
  }
  #Make sure all variables in the readAndPrepareData
  #enclosing environment (where it was defined) are removed
  #to avoid unexpected results
  environment(object$readAndPrepareData) <- environment()
  
  basetable <- object$readAndPrepareData(train=FALSE,
                                         end_ind= as.Date(dumpDate, object$f),
                                         length_ind=object$length_ind)
  cat("Predicting: ")
  time <- Sys.time()
  ans <- data.frame(Custid=basetable$custid,
                    LifetimeValue=predict(object=object$rf,
                                  newdata=basetable))
  ans <- ans[order(ans$LifetimeValue, decreasing=TRUE),]
  cat(format(round(as.numeric(Sys.time()- time),1),nsmall=1,width=4),
      attr(Sys.time()- time,"units"), "\n")
  ans
}
  
pred <- predict(object=clvModel,
                dumpDate="09/27/2015")  
head(pred)
