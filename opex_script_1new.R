###############################
###############################
# libraries used
###############################
###############################

library(proto)
library(RSQLite)
library(gsubfn)
library(sqldf)
library(tcltk)
library(sqldf)
library(tcltk)
library(data.table)
library(dtplyr)
library(dplyr)
library(zoo)
library(xts)
library(corrplot)
library(lubridate)
library(timeDate)

###############################
###############################
# Major Task
###############################
###############################

###############################
# Monor Task
###############################

##################################################################################
##################################################################################

# stock unit is eod inventory

###############################
###############################
# data stored /uploaded
###############################
###############################
product_details=read.csv("product_details.csv")
eod_inventory=read.csv("eod_inventory.csv")
product_movement=read.csv("product_movement.csv")

###############################
#date format fix
###############################
eod_inventory$date<- strptime(eod_inventory$date, format = "%m/%d/%Y")
product_movement$date<- strptime(product_movement$date, "%m/%d/%Y")

eod_inventory$date=as.Date(eod_inventory$date, format = "%m/%d/%Y" )
product_movement$date=as.Date(product_movement$date, format = "%m/%d/%Y" )

class(eod_inventory$date) #check
class(product_movement$date) #check

###############################
#material fix
###############################
product_details$material=factor(product_details$material)
eod_inventory$material=factor(eod_inventory$material)
product_movement$material=factor(product_movement$material)

dim(product_details) # 1804 products
dim(eod_inventory)
dim(product_movement)

###############################
# produce movement aggregation
###############################
#no need
product_movement=sqldf("select 
        material,location,date, move_type, sum(total_units) as total_units
        from product_movement
        group by  material,location,date, move_type")

###############################
###############################
#EDA/ summary stats
###############################
###############################

# data structure 
str(product_details)
str(eod_inventory)
str(product_movement)

# head in each data set
head( product_details)
head(eod_inventory)
head(product_movement)

# summary
summary(product_details)
summary(eod_inventory)
summary(product_movement)

#min and max date in the dataset
paste("first date in the dataset is",min(min(product_movement$date),min(eod_inventory$date)))
paste("last date in the dataset is",max(max(product_movement$date),max(eod_inventory$date)))
max(max(product_movement$date),max(eod_inventory$date))-min(min(product_movement$date),min(eod_inventory$date))+1


###############################
###############################
# master data structure 
###############################
###############################

#every date in the data
date.range.array=seq(as.Date("2011-11-30"), as.Date("2014-12-09"), by="days")
length(date.range.array)

###############################
#creating the data frame for the repeat
###############################
date.array.full=rep(date.range.array,length(unique(product_details$material)))
length(date.array.full)

paste("the dataframe has",length(date.array.full),"rows") #check

#data fram conversion
date.array.full=as.data.frame(date.array.full)
dim(date.array.full)

head(date.array.full)
tail(date.array.full)

#items from the data: repeating earch material by the time frame
items=rep(unique(product_details$material),length(date.range.array))
items=sort(items)
length(items)

# an ID and product name added to the data
cxcx=cbind.data.frame(ID=1:dim(date.array.full)[1],date=date.array.full,material=items )
colnames(cxcx)=c("ID","date","material")
dim(cxcx)

###############################
###############################
# merging data sources
###############################
###############################

###############################
#first merging data from eod inventory and cxcx which is combined date.
###############################
cxcx1=merge(x=cxcx,y=eod_inventory,by =c("date","material"),all.x=TRUE)
cxcx1$location="Plant A"
dim(cxcx1)

###############################
# any ship to customer or stock transfer is negative(outflow) OW inflow
###############################
product_movement$total_units=ifelse(product_movement$move_type=="Goods Receipt",product_movement$total_units,-1*product_movement$total_units)
dim(product_movement)
product_movement=sqldf("select material,location,date, sum(total_units) as total_units
          from product_movement
           group by  material,location,date")
dim(product_movement)
hist(product_movement$total_units)

###############################
#second merge for prduct movement
###############################
cxcx2=merge(x=cxcx1,y=product_movement,by =c("date","material","location"),all.x=TRUE)

cxcx3=cxcx2[ order(cxcx2[,2], cxcx2[,1]), ]

###############################
###############################
# Imputation
###############################
###############################

#  now let's impute
cxcx4=cxcx3
head(cxcx4,50)

###############################
# setting na's in total unit flows equal to 0, assuming we only have info on 3 kind of transfer
###############################
cxcx4$total_units[is.na(cxcx4$total_units)] <- 0

###############################
# if the first date (strating eod inventory) is non zero then put 0
cxcx4$stock_units=ifelse(is.na(cxcx4$stock_units) & cxcx4$date=='2011-11-30',0,cxcx4$stock_units)
###############################

###############################
# replaced all na values on the first date with 0 and then apply the below to fill the na's with previous value
###############################
require(zoo)
cxcx6=cxcx4 %>% group_by(material) %>% mutate(stock_units_imputed=zoo::na.locf(stock_units))

#print.data.frame(cxcx6)
print.data.frame(cxcx6[1:50,])

#trying correlation by item: doesn make much sense
summary(cxcx6)
cor(cxcx6$stock_units_imputed,cxcx6$total_units)
dt <- data.table(cxcx6)
dtCor <- dt[, .(mCor = cor(total_units,stock_units_imputed)), by=material]

dtCor$mCor=round(dtCor$mCor,2)

###############################
#converting to data frame
###############################
cxcx6=data.frame(cxcx6)
dim(cxcx6)

head(cxcx6)

###############################
###############################
#feature engineering
###############################
###############################

###############################
# min of stockout in the 14-28 days out
###############################

library(dplyr)
library(zoo)
df=cxcx6


# This formula is applying min of a rolling window for 2 to 4 weeks out. simiar formula to be applyed for other features to be engineered
# this is the output
#output flag: Min EOD inventory in 2-4 weeks time frame (today+14 to today+28)

###############################
# some rolling features
###############################

# below is the number of stockotuts in the prior 4 weeks
df=df %>% group_by(material) %>%
        mutate(min_future_stock_units_imputed=rollapply(stock_units_imputed,list(14:27), min,fill = NA)#y -output: This formula is applying min of a rolling window for 2 to 4 weeks out. simiar formula to be applyed for other features to be engineered
                ,cnt_stockouts_past4_wks=rollapply(stock_units_imputed,list(-28:0), function(a)sum(a==0),fill = NA) # stockout in the past 4 weeks
                ,cnt_stockouts_past13_wks=rollapply(stock_units_imputed,list(-118:-27), function(a)sum(a==0),fill = NA) # stockout in the past 3 months prior to the last 4 weeks weeks
                ,cnt_stockouts_all=rollapplyr(stock_units_imputed,seq_along(stock_units_imputed), function(a)sum(a==0)) # all stockouts counts on this item
                ,mean_flow_past4_wks=rollapply(total_units,list(-28:0), FUN=mean,fill = NA) # past 4 weeks mean flow
                ,mean_flow_all=rollapplyr(total_units, seq_along(total_units),mean)# total mean of flow
                ,sd_flow_past4_wks=rollapply(total_units,list(-28:0), FUN=sd,fill = NA) # past 4 weeks sd of flow
                ,sd_flow_all=rollapplyr(total_units,seq_along(total_units),sd)) # total sd of flow

class(df)
df=data.frame(df)
head(df,100)
#write.csv(df,"df_20170327_1505.csv",row.names = F)
getwd()
df=read.csv("/Users/Cyrus/Dropbox/The University of Chicago/11. Winter 2017/StockoutPrediction -2016-selected/df_20170327_1505.csv")
###############################
# date features
###############################


df1=df
dim(df1)
head(df1)
names(df1)

df1$DayOfWeek=wday(df1$date)#day of week number
df1$MonthNumber=month(df1$date) #month number
df1$IsWeekend=ifelse(df1$DayOfWeek %in% c(1,7), 1,0)#weekend flag
df1$QuarterNumber=quarter(df1$date) # quarter

#isHoliday(df1$date[1:10]) skipped to complexity

###############################
# ts shifts
###############################

shift <- function(x, n){
        c(x[-(seq(n))], rep(NA, n))
}

dim(df1)

df1=df1 %>% group_by(material) %>%
        mutate(lag1.stockout=shift(min_future_stock_units_imputed,1)
               ,lag2.stockout=shift(min_future_stock_units_imputed,2)
               ,lag3.stockout=shift(min_future_stock_units_imputed,3)
               ,mean_stockout_past2day=rollapply(min_future_stock_units_imputed,list(-1:0), FUN=mean,fill = NA)
               ,mean_stockout_past3day=rollapply(min_future_stock_units_imputed,list(-2:0), FUN=mean,fill = NA)
               ,mean_stockout_past7day=rollapply(min_future_stock_units_imputed,list(-6:0), FUN=mean,fill = NA)
               ,mean_stockout_past28day=rollapply(min_future_stock_units_imputed,list(-27:0), FUN=mean,fill = NA)
               ,sd_stockout_past2day=rollapply(min_future_stock_units_imputed,list(-1:0), FUN=sd,fill = NA)
               ,sd_stockout_past3day=rollapply(min_future_stock_units_imputed,list(-2:0), FUN=sd,fill = NA)
               ,sd_stockout_past7day=rollapply(min_future_stock_units_imputed,list(-6:0), FUN=sd,fill = NA)
               ,sd_stockout_past28day=rollapply(min_future_stock_units_imputed,list(-27:0), FUN=sd,fill = NA)
               ,min_stockout_past7day=rollapply(min_future_stock_units_imputed,list(-6:0), FUN=min,fill = NA)
               ,max_stockout_past7day=rollapply(min_future_stock_units_imputed,list(-6:0), FUN=max,fill = NA)
               
               ,new_cnt_stockouts_past4_wks=rollapply(min_future_stock_units_imputed,list(-28:0), function(a)sum(a==0),fill = NA) # stockout in the past 4 weeks
               ,new_cnt_stockouts_past13_wks=rollapply(min_future_stock_units_imputed,list(-118:-27), function(a)sum(a==0),fill = NA) # stockout in the past 3 months prior to the last 4 weeks weeks
               ,new_cnt_stockouts_all=rollapplyr(min_future_stock_units_imputed,seq_along(min_future_stock_units_imputed), function(a)sum(a==0)) # all stockouts counts on this item
               ,mean_stockout_past4_wks=rollapply(min_future_stock_units_imputed,list(-28:0), FUN=mean,fill = NA) # past 4 weeks stockout
               ,mean_stockout_all=rollapplyr(min_future_stock_units_imputed, seq_along(min_future_stock_units_imputed),mean)# total mean of stockout
               ,sd_stockout_past4_wks=rollapply(min_future_stock_units_imputed,list(-28:0), FUN=sd,fill = NA) # past 4 weeks sd of stockout
               ,sd_stockout_all=rollapplyr(min_future_stock_units_imputed,seq_along(min_future_stock_units_imputed),sd)) # total sd of stockout
               
        
df1=data.frame(df1)

summary(df1)

dim(df1)
names(df1)
head(df1,20)
getwd()

#write.csv(df1,"df_20170327_2312.csv",row.names = F)

###############################
# classification flag
###############################
df1$stockouts=ifelse(df1$min_future_stock_units_imputed>0,0,1)
df1$stockouts=as.factor(df1$stockouts)
str(df1)





head(data.frame(df2),30)
example$z <- shift(example$z, 2)

data.frame(df2)[1100:1150,]


wday(df$date[1:20])

dim(df)
df1=df
df1[is.na(df1)]=0
names(df1)



M=cor(df1[,c(8:15)])
corrplot(M,method = "ellipse")









