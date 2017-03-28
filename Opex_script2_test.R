
eod_inventory_test=read.csv("eod_inventory.csv")
product_movement_test=read.csv("product_movement.csv")
product_details_test=read.csv("product_details.csv")

eod_inventory_test$date<- strptime(eod_inventory_test$date, format = "%m/%d/%Y")
product_movement_test$date<- strptime(product_movement_test$date, "%m/%d/%Y")

eod_inventory_test$date=as.Date(eod_inventory_test$date, format = "%m/%d/%Y" )
product_movement_test$date=as.Date(product_movement_test$date, format = "%m/%d/%Y" )

dim(eod_inventory_test)
dos=merge(x=eod_inventory_test,y=product_details_test,by =c("material","location"),all.x=TRUE) # adding class and group names
dim(dos)
head(dos)

sqldf('select subsegment,"group", count(*),material, count(distinct material),count(*)/count(distinct material)
    from dos
    where stock_units=0
    group by subsegment,"group",material
    order by count(*)/count(distinct material) desc')




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

