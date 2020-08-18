library(data.table)
library(ORCH)
library(sqldf)
library(plyr)
library(dplyr)
library(h2o)
library(lubridate)


ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='uc_cp',all=TRUE) 

ore.ls()
call_count1<-ore.pull(call_count)
call_count2<-subset(call_count1,year>=2018)
call_count2$date_<-paste(call_count2$year,call_count2$month,call_count2$date,sep="-")
call_count2$date_<-as.Date(call_count2$date_,format="%Y-%m-%d")
holiday_calender2<-ore.pull(holiday_calender)

planned<-ore.pull(bkt_planned_outage_request_hourly)
unplanned<-ore.pull(bkt_unplanned_outage_hourly)#fun
holiday_calender2<-subset(holiday_calender2,datum>="2016-01-01")
weatht<-ore.pull(weather_yesterday)
weathh<-ore.pull(weather_tomorrow)
names(weathh)[names(weathh)=="fore_temp"]<-"act_temp"
names(weathh)[names(weathh)=="fore_humidity"]<-"act_humidity"
names(weathh)[names(weathh)=="fore_windspeed"]<-"act_windspeed"
names(weathh)[names(weathh)=="fore_wind_direction"]<-"act_wind_direction"
names(weathh)[names(weathh)=="fore_cloud"]<-"act_cloud"
names(weathh)[names(weathh)=="fore_rainrate"]<-"act_rainrate"
weathh<-subset(weathh,sch_dt==as.Date((Sys.Date()+1),format="%Y-%m-%d")| sch_dt==as.Date(Sys.Date(),
                                                                                         format="%Y-%m-%d"))
weath<-rbind(weatht,weathh)
names(holiday_calender2)
weath$slot_id<-as.factor(weath$slot_id)
levels(weath$slot_id)<-c("0","0","0","0","1","1","1","1","2","2","2","2","3","3","3","3","4","4","4","4","5","5","5","5","6","6","6","6","7","7","7","7",
                         "8","8","8","8","9","9","9","9","10","10","10","10","11","11","11","11","12","12","12","12","13","13","13","13","14","14","14","14","15","15","15","15","16","16","16","16","17","17","17","17","18","18","18","18","19","19","19","19","20","20","20","20","21","21","21","21","22","22","22","22","23","23","23","23")

weath$slot_id<-as.numeric(levels(weath$slot_id))[weath$slot_id]
str(weath)
weath1<-aggregate(weath[c("act_temp","act_humidity","act_windspeed","act_wind_direction","act_cloud","act_rainrate")],by=list(weath$sch_dt,weath$slot_id),mean)
names(weath1)[names(weath1)=="Group.1"]<-"sch_dt"
names(weath1)[names(weath1)=="Group.2"]<-"slot_id"
table(is.na(weath1))

weath1$year<-format(weath1$sch_dt,format="%Y")
weath1<-subset(weath1,year>=2018)
table(weath1$year)

hol_weath<-merge(weath1,holiday_calender2,by.x="sch_dt",by.y="datum",all.x=TRUE)
table(hol_weath$ident)
str(hol_weath)
hol_weath$ident<-ifelse(is.na(hol_weath$ident),0,1)
table(hol_weath$ident)
names(hol_weath)

##joining weather and Planned and unpalnned outage details


names(planned)[names(planned)=="total_outage_count"]<-"planned_outage_count"
names(planned)[names(planned)=="total_customers_effected"]<-"planned_customer_count"
names(unplanned)[names(unplanned)=="total_outage_count"]<-"unplanned_outage_count"
names(unplanned)[names(unplanned)=="total_customers_effected"]<-"unplanned_customer_count"
planned$datep<-paste(planned$cal_year,planned$cal_month,planned$cal_date,sep="-")
planned$datep<-as.Date(planned$datep,format="%Y-%m-%d")
unplanned$datep<-paste(unplanned$cal_year,unplanned$cal_month,unplanned$cal_date,sep="-")
unplanned$datep<-as.Date(unplanned$datep,format="%Y-%m-%d")
names(hol_weath)
finald1<-merge(hol_weath,planned,by.x=c("sch_dt","slot_id"),by.y=c("datep","time_hour"),all=TRUE)
names(finald1)
finald2<-merge(finald1,unplanned,by.x=c("sch_dt","slot_id"),by.y=c("datep","time_hour"),all=TRUE)

##merging the above tabel with call details from bcm
finald22<-merge(call_count2,finald2,by.x=c("date_","hour_slab"),by.y=c("sch_dt","slot_id"),all=TRUE)
finald22$ident<-ifelse(is.na(finald22$ident),0,finald22$ident)
names(finald22)

finald22$total_talking_seconds<-as.numeric(finald22$total_talking_seconds)

finald22$weekday<-weekdays(finald22$date_)
finald22$quarter<-quarters(finald22$date_)
finald22$planned_outage_count<-as.numeric(finald22$planned_outage_count)
finald22$planned_customer_count<-as.numeric(finald22$planned_customer_count)
finald22$unplanned_outage_count<-as.numeric(finald22$unplanned_outage_count)
finald22$unplanned_customer_count<-as.numeric(finald22$unplanned_customer_count)

M1_fun1 <- function(x){
  
  x<-ifelse(is.na(x),mean(x,na.rm=TRUE),x)
  
}

finald22<-subset(finald22,date_>=as.Date("2018-04-02",format="%Y-%m-%d"))
finald22<-finald22[!duplicated(finald22[c("date_","hour_slab")]), ]
names(finald22)[names(finald22)=="year.x"]<-"year"
finald22$hour_slab<-as.factor(finald22$hour_slab)
finald22$year<-as.factor(finald22$year)
finald22$month<-as.factor(finald22$month)
finald22$date<-as.factor(finald22$date)
finald22$ident<-as.factor(finald22$ident)
finald22$quarter<-as.factor(finald22$quarter)

finald22<-finald22[order(finald22$date_,finald22$hour_slab),]
names(finald22)
finald22<-finald22[-c(3,4,5,14,16:20,23:25,29)]
finald22<-data.table(finald22)
#last 5 days hourly trend
finald3<-finald22[, shift(.SD, 24:120, NA, "lag", TRUE),.SDcols=c("total_calls_in_slab",
                                                                  "unplanned_outage_count", "unplanned_customer_count")]


# hourly trend
finald31<-finald22[, shift(.SD, 1:24, NA, "lag", TRUE),.SDcols=c("act_temp",
                                                                 "act_humidity", "act_windspeed" , "act_wind_direction" , "act_cloud", "act_rainrate",
                                                                 "planned_outage_count", "planned_customer_count")]


finald4<-cbind(finald22,finald3,finald31)
finald4$ident<-ifelse(is.na(finald4$ident),0,finald4$ident)
names(finald4)<-gsub(".","",names(finald4),fixed="TRUE")
finald4$total_calls_in_slab<-round(finald4$total_calls_in_slab,0)
str(finald4$weekday)
finald4$weekend<-ifelse(finald4$weekday=="Saturday"|finald4$weekday=="Sunday",1,0)
finald4$weekday<-as.factor(finald4$weekday)

finald4$weekend<-as.factor(finald4$weekend)

#last 30 days training data

train<-subset(finald4,date_>=(Sys.Date()-1)-days(30) &date_<=(Sys.Date()-1)|date_==Sys.Date()-years(1))
sapply(train, function(x) sum(is.na(x)))
train$ident<-as.factor(train$ident)
train$weekday<-as.factor(train$weekday)
train$weekend<-as.factor(train$weekend)

#taking testing data of current day
test<-subset(finald4,date_==Sys.Date())
sapply(test, function(x) sum(is.na(x)))
test$ident<-as.factor(test$ident)
test$weekday<-as.factor(test$weekday)
test$weekend<-as.factor(test$weekend)

###REAL TIME VALIDATION of yesterday
ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 

ore.ls()
yest<-ore.pull(currentday_callforecast_cp)

test_v<-subset(finald4,date_==Sys.Date()-1)
test_a<-cbind(test_v,yest[,"predicted_calls"])
names(test_a)
test_a<-test_a[,c(1:16,500:501)]
names(test_a)[ncol(test_a)]<-"Predicted_Calls"
test_a<-transform(test_a,APE=abs(Predicted_Calls-total_calls_in_slab)/total_calls_in_slab)
test_a<-transform(test_a,error=abs(Predicted_Calls-total_calls_in_slab))
summary(test_a$error)
mape=sum(test_a$APE)/NROW(test_a)
mape
summary(test_a$total_calls_in_slab)
summary(test_a$Predicted_Calls)
ore.drop("yesterday_actualvspred_cp")
ore.create(test_a,"yesterday_actualvspred_cp")

#####################MODEL BUILDING################

M1_funout<- function(x)
{quantiles <- quantile(x, c(.01, .99),na.rm=TRUE )
x<-ifelse(x<quantiles[1],quantiles[1],x)
x<-ifelse(x>quantiles[2],quantiles[2],x)
}

#Data preprocess on training data
num_var=sapply(train,is.numeric)
train<-data.frame(train)
train[num_var]<-sapply(train[num_var],function(x){M1_fun1(x)})
train1<-train##for getting unscaled values
train[num_var]<-sapply(train[num_var],function(x){M1_funout(x)})

#Data preprocess on testing data
num_var=sapply(test,is.numeric)
test<-data.frame(test)
test[num_var]<-sapply(test[num_var],function(x){M1_fun1(x)})
test1<-test##for getting unscaled values
test[num_var]<-sapply(test[num_var],function(x){M1_funout(x)})
#test[num_var]<-sapply(test[num_var],function(x){(x-min(x))/(max(x)-min(x))})

train$total_calls_in_slab<-round(train$total_calls_in_slab,0)
train$sqrty<-sqrt(train$total_calls_in_slab)
train$logy<-log(train$total_calls_in_slab)

h2o.init(
  nthreads=-1,            ## -1: use all available threads
  max_mem_size = "300G")    ## specify the memory size for the H2O cloud
h2o.removeAll()           ## Clean slate - just in case the cluster was already running

########################creating second training data of 12 days

train2<-subset(train,date_>=(Sys.Date()-1)-days(12)& date_<=(Sys.Date()-1)|date_==Sys.Date()-years(1))#starting date of training data

ntrees_opt <- c(200) #nos of trees
maxdepth_opt <- c(4,5,6) #size of tree
learnrate_opt <- c(0.1,0.15,0.2) #shrinkage value/learning rate
hypeper_parameters <- list(max_depth=maxdepth_opt, learn_rate=learnrate_opt,stopping_rounds = 10,
                           stopping_metric = "RMSE",stopping_tolerance = 0.0001,col_sample_rate=0.9,col_sample_rate_per_tree=0.9)
foot_<-as.h2o(train,destination_frame="foot_.hex")
foot2_<-as.h2o(train2,destination_frame="foot2_.hex")
foot_test<-as.h2o(test,destination_frame="foot_test.hex")

#HUBER DISTRIBUTION
names(foot_)

grid_gbm1 <- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="huber", training_frame =
                        foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models1 <- lapply(grid_gbm1@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae1<-c()
for (i in 1:length(grid_models1)) {
  mae1[i]<-h2o.mae(grid_models1[[i]])
}

#POISSON DISTRIBUTION
grid_gbm2<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="poisson", training_frame =
                       foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models2<- lapply(grid_gbm2@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae2<-c()
for (i in 1:length(grid_models2)) {
  mae2[i]<-h2o.mae(grid_models2[[i]])
}

#GAUSSIAN DISTRIBUTION
grid_gbm3<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502), hyper_params = hypeper_parameters,distribution="gaussian", training_frame =
                       foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models3<- lapply(grid_gbm3@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae3<-c()
for (i in 1:length(grid_models3)) {
  mae3[i]<-h2o.mae(grid_models3[[i]])
}


#HUBER DISTRIBUTION
grid_gbm4<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502), hyper_params = hypeper_parameters,distribution="huber", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models4<- lapply(grid_gbm4@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae4<-c()
for (i in 1:length(grid_models4)) {
  mae4[i]<-h2o.mae(grid_models4[[i]])
}

#POISSON DISTRIBUTION
grid_gbm5<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="poisson", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models5<- lapply(grid_gbm5@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae5<-c()
for (i in 1:length(grid_models5)) {
  mae5[i]<-h2o.mae(grid_models5[[i]])
}

#GAUSSIAN DISTRIBUTION
grid_gbm6<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="gaussian", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models6<- lapply(grid_gbm6@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae6<-c()
for (i in 1:length(grid_models6)) {
  mae6[i]<-h2o.mae(grid_models6[[i]])
}


final_predictions1<-h2o.predict(
  object = grid_models1[[which.min(mae1)]],
  newdata = foot_)

final_predictions2<-h2o.predict(
  object = grid_models2[[which.min(mae2)]],
  newdata = foot_)

final_predictions3<-h2o.predict(
  object = grid_models3[[which.min(mae3)]],
  newdata = foot_)

final_predictions4<-h2o.predict(
  object = grid_models4[[which.min(mae4)]],
  newdata = foot2_)

final_predictions5<-h2o.predict(
  object = grid_models5[[which.min(mae5)]],
  newdata = foot2_)

final_predictions6<-h2o.predict(
  object = grid_models6[[which.min(mae6)]],
  newdata = foot2_)

varimp1<-h2o.varimp(grid_models1[[which.min(mae1)]])
varimp1<-as.data.frame(varimp1)
varimp2<-h2o.varimp(grid_models2[[which.min(mae2)]])
varimp2<-as.data.frame(varimp2)
varimp3<-h2o.varimp(grid_models3[[which.min(mae3)]])
varimp3<-as.data.frame(varimp3)
varimp4<-h2o.varimp(grid_models5[[which.min(mae5)]])
varimp4<-as.data.frame(varimp4)
varimp5<-h2o.varimp(grid_models4[[which.min(mae4)]])
varimp5<-as.data.frame(varimp5)
varimp6<-h2o.varimp(grid_models6[[which.min(mae6)]])
varimp6<-as.data.frame(varimp6)

Pred_calls_gbm=(as.data.frame(final_predictions1$predict)+as.data.frame(final_predictions2$predict)+as.data.frame(final_predictions3$predict))/3

Pred_calls_gbm2=(as.data.frame(final_predictions4$predict)+as.data.frame(final_predictions5$predict)+as.data.frame(final_predictions6$predict))/3

trainf<-cbind(train,Pred_calls_gbm$predict)
names(trainf)[NCOL(trainf)]<-"Pred_calls_gbm"
trainf$Pred_calls_gbm<-exp(trainf$Pred_calls_gbm)
trainf$Pred_calls_gbm<-round(trainf$Pred_calls_gbm,0)
trainf$total_calls_in_slab<-round(trainf$total_calls_in_slab,0)
trainf<-transform(trainf,APE=abs(Pred_calls_gbm-total_calls_in_slab)/total_calls_in_slab)
trainf<-transform(trainf,error=abs(Pred_calls_gbm-total_calls_in_slab))
summary(trainf$error)
mape=sum(trainf$APE)/NROW(trainf)
names(trainf)
d11<-trainf[c("date_","hour_slab","total_calls_in_slab","Pred_calls_gbm",
              "APE" ,"error","weekend","weekday","ident")]


trainf2<-cbind(train2,Pred_calls_gbm2$predict)
names(trainf2)[NCOL(trainf2)]<-"Pred_calls_gbm2"
trainf2$Pred_calls_gbm2<-exp(trainf2$Pred_calls_gbm2)
trainf2$Pred_calls_gbm2<-round(trainf2$Pred_calls_gbm2,0)
trainf2$total_calls_in_slab<-round(trainf2$total_calls_in_slab,0)
trainf2<-transform(trainf2,APE=abs(Pred_calls_gbm2-total_calls_in_slab)/total_calls_in_slab)
trainf2<-transform(trainf2,error=abs(Pred_calls_gbm2-total_calls_in_slab))
summary(trainf2$error)
mape=sum(trainf2$APE)/NROW(trainf2)
mape
names(trainf2)
d2<-trainf2[c("date_","hour_slab","total_calls_in_slab","Pred_calls_gbm2",
              "APE" ,"error")]


final_predictionstest1<-h2o.predict(
  object = grid_models1[[which.min(mae1)]],
  newdata = foot_test)

final_predictionstest2<-h2o.predict(
  object = grid_models2[[which.min(mae2)]],
  newdata = foot_test)

final_predictionstest3<-h2o.predict(
  object = grid_models3[[which.min(mae3)]],
  newdata = foot_test)

final_predictionstest4<-h2o.predict(
  object = grid_models4[[which.min(mae4)]],
  newdata = foot_test)

final_predictionstest5<-h2o.predict(
  object = grid_models5[[which.min(mae5)]],
  newdata = foot_test)

final_predictionstest6<-h2o.predict(
  object = grid_models6[[which.min(mae6)]],
  newdata = foot_test)

Pred_calls_testgbm=(as.data.frame(final_predictionstest1$predict)+as.data.frame(final_predictionstest2$predict)+as.data.frame(final_predictionstest3$predict)+
                      as.data.frame(final_predictionstest4$predict)+as.data.frame(final_predictionstest5$predict)+as.data.frame(final_predictionstest6$predict))/6


testf<-cbind(test,Pred_calls_testgbm$predict)
names(testf)[NCOL(testf)]<-"Pred_calls_testgbm"
testf$Pred_calls_testgbm<-exp(testf$Pred_calls_testgbm)
testf$total_calls_in_slab<-round(testf$total_calls_in_slab,0)
testf$Pred_calls_testgbm<-round(testf$Pred_calls_testgbm,0)

d22<-testf[c("date_","hour_slab","total_calls_in_slab","Pred_calls_testgbm"            
             ,"total_talking_seconds", "act_temp", "act_humidity",
             "act_windspeed" , "act_wind_direction" , "act_cloud" ,"act_rainrate",
             "planned_outage_count","planned_customer_count","weekend","weekday","ident" ,"unplanned_outage_count" , "unplanned_customer_count" )]

names(d22)
d22$ident<-as.character(d22$ident)
d22$ident[d22$ident=="1"]<-"0"
d22$ident[d22$ident=="2"]<-"1"

names(d22)[names(d22)=="Pred_calls_testgbm"]<-"Predicted Calls"
names(d22)[names(d22)=="act_temp"]<-"Temperature"
names(d22)[names(d22)=="act_humidity"]<-"Humidity"
names(d22)[names(d22)=="act_windspeed"]<-"Windspeed"
names(d22)[names(d22)=="act_wind_direction"]<-"Wind Direction"
names(d22)[names(d22)=="act_cloud"]<-"Cloud"
names(d22)[names(d22)=="act_rainrate"]<-"Rainrate"
names(d22)[names(d22)=="planned_outage_count"]<-"Count of Planned Outage"
names(d22)[names(d22)=="planned_customer_count"]<-"Affected Customers (Planned)"
names(d22)[names(d22)=="unplanned_outage_count"]<-"Count of UnPlanned Outage"
names(d22)[names(d22)=="unplanned_customer_count"]<-"Affected Customers (UnPlanned)"
names(d22)[names(d22)=="ident"]<-"Holiday Flag"
names(d22)[names(d22)=="weekend"]<-"Weekend Flag"
names(d22)[names(d22)=="weekday"]<-"Weekday Flag"

##yesterday 
d22y<-d22

ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 

ore.drop("currentday_callforecast_cp")
ore.create(d22y,"currentday_callforecast_cp")

#################DAY AHEAD FORECAST#######################################################################

hypeper_parameters <- list(max_depth=maxdepth_opt, learn_rate=learnrate_opt,stopping_rounds = 10,
                           stopping_metric = "RMSE",stopping_tolerance = 0.0001)

#last 30 days training data

train<-subset(finald4,date_>=Sys.Date()-days(30) & date_<=Sys.Date() | date_==(Sys.Date()+1)-years(1))
sapply(train, function(x) sum(is.na(x)))
train$ident<-as.factor(train$ident)
train$weekday<-as.factor(train$weekday)
train$weekend<-as.factor(train$weekend)

#taking testing data of current day
test<-subset(finald4,date_==Sys.Date()+days(1))

sapply(test, function(x) sum(is.na(x)))
test$ident<-as.factor(test$ident)
test$weekday<-as.factor(test$weekday)
test$weekend<-as.factor(test$weekend)

#####################MODEL BUILDING################


#Data preprocess on training data
num_var=sapply(train,is.numeric)
train<-data.frame(train)
train[num_var]<-sapply(train[num_var],function(x){M1_fun1(x)})
train1<-train##for getting unscaled values
train[num_var]<-sapply(train[num_var],function(x){M1_funout(x)})

#Data preprocess on testing data
num_var=sapply(test,is.numeric)
test<-data.frame(test)
test[num_var]<-sapply(test[num_var],function(x){M1_fun1(x)})
test1<-test##for getting unscaled values
test[num_var]<-sapply(test[num_var],function(x){M1_funout(x)})
#test[num_var]<-sapply(test[num_var],function(x){(x-min(x))/(max(x)-min(x))})

train$total_calls_in_slab<-round(train$total_calls_in_slab,0)
train$sqrty<-sqrt(train$total_calls_in_slab)
train$logy<-log(train$total_calls_in_slab)

h2o.init(
  nthreads=-1,            ## -1: use all available threads
  max_mem_size = "300G")    ## specify the memory size for the H2O cloud
h2o.removeAll()           ## Clean slate - just in case the cluster was already running

########################creating second training data of 12 days

train2<-subset(train,date_>=Sys.Date()-days(12)& date_<=Sys.Date() | date_==(Sys.Date()+1)-years(1))#starting date of training data
foot_<-as.h2o(train,destination_frame="foot_.hex")
foot2_<-as.h2o(train2,destination_frame="foot2_.hex")

foot_test<-as.h2o(test,destination_frame="foot_test.hex")

#HUBER DISTRIBUTION
names(foot_)

grid_gbm1 <- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="huber", training_frame =
                        foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models1 <- lapply(grid_gbm1@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae1<-c()
for (i in 1:length(grid_models1)) {
  mae1[i]<-h2o.mae(grid_models1[[i]])
}

#POISSON DISTRIBUTION
grid_gbm2<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="poisson", training_frame =
                       foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models2<- lapply(grid_gbm2@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae2<-c()
for (i in 1:length(grid_models2)) {
  mae2[i]<-h2o.mae(grid_models2[[i]])
}

#GAUSSIAN DISTRIBUTION
grid_gbm3<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502), hyper_params = hypeper_parameters,distribution="gaussian", training_frame =
                       foot_,nfolds=6,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models3<- lapply(grid_gbm3@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae3<-c()
for (i in 1:length(grid_models3)) {
  mae3[i]<-h2o.mae(grid_models3[[i]])
}


#HUBER DISTRIBUTION
grid_gbm4<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502), hyper_params = hypeper_parameters,distribution="huber", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models4<- lapply(grid_gbm4@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae4<-c()
for (i in 1:length(grid_models4)) {
  mae4[i]<-h2o.mae(grid_models4[[i]])
}

#POISSON DISTRIBUTION
grid_gbm5<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="poisson", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models5<- lapply(grid_gbm5@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae5<-c()
for (i in 1:length(grid_models5)) {
  mae5[i]<-h2o.mae(grid_models5[[i]])
}

#GAUSSIAN DISTRIBUTION
grid_gbm6<- h2o.grid("gbm", y = "logy",x=-c(1,3,4,14,15,501:502),hyper_params = hypeper_parameters, distribution="gaussian", training_frame =
                       foot2_,nfolds=4,keep_cross_validation_predictions=TRUE,seed=23456)

grid_models6<- lapply(grid_gbm6@model_ids, function(model_id) { model = h2o.getModel(model_id) })
mae6<-c()
for (i in 1:length(grid_models6)) {
  mae6[i]<-h2o.mae(grid_models6[[i]])
}


final_predictions1<-h2o.predict(
  object = grid_models1[[which.min(mae1)]],
  newdata = foot_)

final_predictions2<-h2o.predict(
  object = grid_models2[[which.min(mae2)]],
  newdata = foot_)

final_predictions3<-h2o.predict(
  object = grid_models3[[which.min(mae3)]],
  newdata = foot_)

final_predictions4<-h2o.predict(
  object = grid_models4[[which.min(mae4)]],
  newdata = foot2_)

final_predictions5<-h2o.predict(
  object = grid_models5[[which.min(mae5)]],
  newdata = foot2_)

final_predictions6<-h2o.predict(
  object = grid_models6[[which.min(mae6)]],
  newdata = foot2_)

varimp1<-h2o.varimp(grid_models1[[which.min(mae1)]])
varimp1<-as.data.frame(varimp1)
varimp2<-h2o.varimp(grid_models2[[which.min(mae2)]])
varimp2<-as.data.frame(varimp2)
varimp3<-h2o.varimp(grid_models3[[which.min(mae3)]])
varimp3<-as.data.frame(varimp3)
varimp4<-h2o.varimp(grid_models5[[which.min(mae5)]])
varimp4<-as.data.frame(varimp4)
varimp5<-h2o.varimp(grid_models4[[which.min(mae4)]])
varimp5<-as.data.frame(varimp5)
varimp6<-h2o.varimp(grid_models6[[which.min(mae6)]])
varimp6<-as.data.frame(varimp6)

Pred_calls_gbm=(as.data.frame(final_predictions1$predict)+as.data.frame(final_predictions2$predict)+as.data.frame(final_predictions3$predict))/3

Pred_calls_gbm2=(as.data.frame(final_predictions4$predict)+as.data.frame(final_predictions5$predict)+as.data.frame(final_predictions6$predict))/3

trainf<-cbind(train,Pred_calls_gbm$predict)
names(trainf)[NCOL(trainf)]<-"Pred_calls_gbm"
trainf$Pred_calls_gbm<-exp(trainf$Pred_calls_gbm)
trainf$Pred_calls_gbm<-round(trainf$Pred_calls_gbm,0)
trainf$total_calls_in_slab<-round(trainf$total_calls_in_slab,0)
trainf<-transform(trainf,APE=abs(Pred_calls_gbm-total_calls_in_slab)/total_calls_in_slab)
trainf<-transform(trainf,error=abs(Pred_calls_gbm-total_calls_in_slab))
summary(trainf$error)
mape=sum(trainf$APE)/NROW(trainf)
mape
names(trainf)
d11<-trainf[c("date_","hour_slab","total_calls_in_slab","Pred_calls_gbm",
              "APE" ,"error","weekend","weekday","ident")]


trainf2<-cbind(train2,Pred_calls_gbm2$predict)
names(trainf2)[NCOL(trainf2)]<-"Pred_calls_gbm2"
trainf2$Pred_calls_gbm2<-exp(trainf2$Pred_calls_gbm2)
trainf2$Pred_calls_gbm2<-round(trainf2$Pred_calls_gbm2,0)
trainf2$total_calls_in_slab<-round(trainf2$total_calls_in_slab,0)
trainf2<-transform(trainf2,APE=abs(Pred_calls_gbm2-total_calls_in_slab)/total_calls_in_slab)
trainf2<-transform(trainf2,error=abs(Pred_calls_gbm2-total_calls_in_slab))
summary(trainf2$error)
mape=sum(trainf2$APE)/NROW(trainf2)
mape
names(trainf2)
d2<-trainf2[c("date_","hour_slab","total_calls_in_slab","Pred_calls_gbm2",
              "APE" ,"error")]


final_predictionstest1<-h2o.predict(
  object = grid_models1[[which.min(mae1)]],
  newdata = foot_test)

final_predictionstest2<-h2o.predict(
  object = grid_models2[[which.min(mae2)]],
  newdata = foot_test)

final_predictionstest3<-h2o.predict(
  object = grid_models3[[which.min(mae3)]],
  newdata = foot_test)

final_predictionstest4<-h2o.predict(
  object = grid_models4[[which.min(mae4)]],
  newdata = foot_test)

final_predictionstest5<-h2o.predict(
  object = grid_models5[[which.min(mae5)]],
  newdata = foot_test)

final_predictionstest6<-h2o.predict(
  object = grid_models6[[which.min(mae6)]],
  newdata = foot_test)

Pred_calls_testgbm=(as.data.frame(final_predictionstest1$predict)+as.data.frame(final_predictionstest2$predict)+as.data.frame(final_predictionstest3$predict)+
                      as.data.frame(final_predictionstest4$predict)+as.data.frame(final_predictionstest5$predict)+as.data.frame(final_predictionstest6$predict))/6


testf<-cbind(test,Pred_calls_testgbm$predict)
names(testf)[NCOL(testf)]<-"Pred_calls_testgbm"
testf$Pred_calls_testgbm<-exp(testf$Pred_calls_testgbm)
testf$total_calls_in_slab<-round(testf$total_calls_in_slab,0)
testf$Pred_calls_testgbm<-round(testf$Pred_calls_testgbm,0)

d22<-testf[c("date_","hour_slab","total_calls_in_slab","Pred_calls_testgbm"            
             ,"total_talking_seconds", "act_temp", "act_humidity",
             "act_windspeed" , "act_wind_direction" , "act_cloud" ,"act_rainrate",
             "planned_outage_count","planned_customer_count","weekend","weekday","ident" ,"unplanned_outage_count" , "unplanned_customer_count" )]

d22$ident<-as.character(d22$ident)
d22$ident[d22$ident=="1"]<-"0"
d22$ident[d22$ident=="2"]<-"1"

names(d22)[names(d22)=="Pred_calls_testgbm"]<-"Predicted Calls"
names(d22)[names(d22)=="act_temp"]<-"Temperature"
names(d22)[names(d22)=="act_humidity"]<-"Humidity"
names(d22)[names(d22)=="act_windspeed"]<-"Windspeed"
names(d22)[names(d22)=="act_wind_direction"]<-"Wind Direction"
names(d22)[names(d22)=="act_cloud"]<-"Cloud"
names(d22)[names(d22)=="act_rainrate"]<-"Rainrate"
names(d22)[names(d22)=="planned_outage_count"]<-"Count of Planned Outage"
names(d22)[names(d22)=="planned_customer_count"]<-"Affected Customers (Planned)"
names(d22)[names(d22)=="unplanned_outage_count"]<-"Count of UnPlanned Outage"
names(d22)[names(d22)=="unplanned_customer_count"]<-"Affected Customers (UnPlanned)"
names(d22)[names(d22)=="ident"]<-"Holiday Flag"
names(d22)[names(d22)=="weekend"]<-"Weekend Flag"
names(d22)[names(d22)=="weekday"]<-"Weekday Flag"

d221<-d22
ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 
ore.drop("dayahead_callforecast_cp")
ore.create(d221,"dayahead_callforecast_cp")
