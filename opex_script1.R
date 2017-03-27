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
#output flag: Min EOD inventory in 2-4 weeks time frame (today+14 to today+28)
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

# below is the number of stockotuts in the prior 4 weeks
df=df %>% group_by(material) %>%
        mutate(min_future_stock_units_imputed=rollapply(stock_units_imputed,list(14:27), min,fill = NA)#y -output: This formula is applying min of a rolling window for 2 to 4 weeks out. simiar formula to be applyed for other features to be engineered
                ,cnt_stockouts_past4_wks=rollapply(stock_units_imputed,list(-28:0), function(a)sum(a==0),fill = NA) # stockout in the past 4 weeks
                ,cnt_stockouts_past13_wks=rollapply(stock_units_imputed,list(-118:-27), function(a)sum(a==0),fill = NA) # stockout in the past 3 months prior to the last 4 weeks weeks
                ,cnt_stockouts_all=sum(stock_units_imputed==0) # all stockouts counts on this item
                ,mean_flow_past4_wks=rollapply(total_units,list(-28:0), FUN=mean,fill = NA) # past 4 weeks mean flow
                ,mean_flow_all=mean(total_units)# total mean of flow
                ,sd_flow_past4_wks=rollapply(total_units,list(-28:0), FUN=sd,fill = NA) # past 4 weeks sd of flow
                ,sd_flow_all=sd(total_units)) # total sd of flow

class(df)
df=data.frame(df)
head(df,100)

df1=df[is.na(df)]=0
names(df)
round(cor(df[,c(8:15)]),2)



df2=df %>% group_by(material) %>%
        mutate(mean_flow=rollapplyr(total_units,seq_along(total_units),mean))
                              

df1=data.frame(df1)
df1[1100:1200,]






















# adding variables for week or 2 weeks
week_window=rep(sort(rep(1:158,7)),length(unique(product_details$material)))
two_week_window=rep(sort(rep(1:79,14)),length(unique(product_details$material)))
class(week_window)
class(two_week_window)
class(cxcx6)
cxcx6=data.frame(cxcx6)

dim(cxcx6)

cxcx6=merge(x=cxcx6,y=product_details,by =c("material","location"),all.x=TRUE) # adding class and group names

cxcx6=cbind(cxcx6,week_window,two_week_window)
head(cxcx6,100)

cxcx6[120:170,]
dim(cxcx6)
cxcx6=cxcx6[ order(cxcx6[,1], cxcx6[,3]), ]
names(cxcx6)



###############################
###############################
# weekly aggregate values
###############################
###############################

cxcx6_weekly=sqldf('select material,location,subsegment,"group", week_window,
                sum(total_units) as total_units_weekly,
                min(stock_units_imputed) as min_stock_units_imputed,
                max(stock_units_imputed) as max_stock_units_imputed,
                round(avg(stock_units_imputed),0) as average_stock_units_imputed

          from cxcx6
                       group by  material,location,week_window,subsegment')

dim(cxcx6_weekly)
head(cxcx6_weekly)
#sort(unique(cxcx6_weekly$material))
cxcx6_weekly=cxcx6_weekly[ order(cxcx6_weekly[,1], cxcx6_weekly[,5]), ]



#correlation of min units in a week and totol units movement 
# again we see that there is not much of correlation 
cor(cxcx6_weekly$min_stock_units_imputed,cxcx6_weekly$total_units_weekly)
dt1 <- data.table(cxcx6_weekly)
dtCor1 <- dt1[, .(mCor = cor(min_stock_units_imputed,total_units_weekly)), by=material]

dtCor1$mCor=round(dtCor1$mCor,2)
print.data.frame(dtCor)

plot(cxcx6_weekly$min_stock_units_imputed,cxcx6_weekly$total_units_weekly)

###############################
###############################
# bi weekly aggregate values
###############################
###############################
names(cxcx6)

cxcx6_Bi_weekly=sqldf('select material,location,subsegment,"group", two_week_window,
                sum(total_units) as total_units_weekly,
                   min(stock_units_imputed) as min_stock_units_imputed,
                   max(stock_units_imputed) as max_stock_units_imputed,
                   round(avg(stock_units_imputed),0) as average_stock_units_imputed
                   
                   from cxcx6
                   group by  material,location,two_week_window,subsegment')

cxcx6_Bi_weekly=cxcx6_Bi_weekly[ order(cxcx6_Bi_weekly[,1], cxcx6_Bi_weekly[,5]), ]
dim(cxcx6_Bi_weekly)
head(cxcx6_Bi_weekly)


cor(cxcx6_Bi_weekly$min_stock_units_imputed,cxcx6_Bi_weekly$total_units_weekly)

plot(cxcx6_Bi_weekly$group,cxcx6_Bi_weekly$min_stock_units_imputed)

#subset of data that have an out of stock instance
yek=subset(cxcx6_Bi_weekly,cxcx6_Bi_weekly$min_stock_units_imputed==0)
names(yek)

# group 13 and 6 seems to have the largest number of weeks with at least a day of stockout
# group 1 may also be a good candidate given the number of stockouts per item is large
sqldf('select subsegment,"group", count(*) as cnt, sum(total_units_weekly), count(distinct material), round(count(*)/count(distinct material),2)
      
      from yek
      group by subsegment, "group"
      order by cnt desc')


sqldf('select subsegment,"group", count(*) as cnt, sum(total_units_weekly), count(distinct material), round(count(*)/count(distinct material),2)
      
      from cxcx6_Bi_weekly
      group by subsegment, "group"
      order by cnt desc')

names(cxcx6)
str(cxcx6)

tres=subset(cxcx6_Bi_weekly, cxcx6_Bi_weekly$material==6024)
dim(tres)
plot(tres$two_week_window,tres$min_stock_units_imputed)


# week_window=sort(rep(1:158,7))
# two_week_window=sort(rep(1:79,14))

# now we can group at variables level


eod_inventory_test=read.csv("eod_inventory.csv")
product_movement_test=read.csv("product_movement.csv")

eod_inventory_test$date<- strptime(eod_inventory_test$date, format = "%m/%d/%Y")
product_movement_test$date<- strptime(product_movement_test$date, "%m/%d/%Y")

eod_inventory_test$date=as.Date(eod_inventory_test$date, format = "%m/%d/%Y" )
product_movement_test$date=as.Date(product_movement_test$date, format = "%m/%d/%Y" )

subset(eod_inventory_test,eod_inventory_test$date=='2011-12-09' & eod_inventory_test$material=='2')
subset(product_movement_test,product_movement_test$date=='2011-12-09' & product_movement_test$material=='2')

# just some testing
# subset(eod_inventory,eod_inventory$date=='2014-11-26' & eod_inventory$material=='12')
# subset(product_movement,product_movement$date=='2014-11-26' & product_movement$material=='12')
# 
# subset(eod_inventory,eod_inventory$date=='2014-11-27' & eod_inventory$material=='12')
# subset(product_movement,product_movement$date=='2014-11-27' & product_movement$material=='12')
# 
# subset(eod_inventory,eod_inventory$date=='2014-11-28' & eod_inventory$material=='12')
# subset(product_movement,product_movement$date=='2014-11-28' & product_movement$material=='12')
# 
# subset(eod_inventory,eod_inventory$date=='2014-12-08' & eod_inventory$material=='12')
# subset(product_movement,product_movement$date=='2014-12-08' & product_movement$material=='12')
# 
# options(max.print=1000)
# 
# subset(cxcx6,cxcx6$date =='2014-1-28')
# subset(eod_inventory,eod_inventory$material=='12')
# subset(product_movement, product_movement$material=='12')

subset(product_movement_test, product_movement_test$material=='2')












# for stock units just use the rpevious row


# na.imputer <- function(x) {
#         if (length(x) > 0L) {
#                 non.na.idx <- which(!is.na(x))
#                 if (is.na(x[1L])) {
#                         non.na.idx <- c(1L, non.na.idx)
#                 }
#                 rep.int(x[non.na.idx], diff(c(non.na.idx, length(x) + 1L)))
#         }
# }
# 
# cxcx5=cxcx4
# library(data.table)
# cxcx5[,newcol:=na.imputer(cxcx4$stock_units[1:100]),by=material]
# 
# cbind(na.imputer(cxcx4$stock_units[500:700]),cxcx4$stock_units[500:700])
# 
# cxcx6=cxcx5[1:100,]


# length(unique(product_movement$date))
# length(unique(cxcx1$date))
# 
# yes=product_movement
# head(yes,20)
# yes$total_units=ifelse(yes$move_type=="Ship to Customer",-1*yes$total_units,yes$total_units)
# 
# require(sqldf)
# 
# yes2=sqldf("select material,location,date, sum(total_units) as total_units
#           from yes
#           group by  material,location,date")
# summary(yes)
# dim(yes2)
# dim(yes)
# hist(yes$)
# 
# dim(cxcx2)
# 
# product_movement[duplicated(product_movement[,1:4]),]


#duplicated(product_movement[,1:4]!=T)

#I have to figure out why the duplicates are happenning here?







# items=rep(unique(product_details$material),length(date.range.array))
#length(unique(items))
#head(items)

# library(data.table)
# setDT(cxcx); setDT(eod_inventory)
# merge(df1,df2,by="id",allow.cartesian=T)
# 
# 
# library(dplyr)
# cxcx1=left_join(cxcx, eod_inventory, by = "date")
# cxcx1=cxcx
# ?match

# cxcx1=merge(x=cxcx,y=eod_inventory,by ="date",all.x=TRUE)
# head(cxcx1,100)
# neew=cxcx1[order(cxcx1$ID),]
# summary(neew)
# head(neew,1000)




# ?merge
# 
# names(product_details)
# names(eod_inventory)
# names(product_movement)

# try association rules on 
#think about some clustering method

