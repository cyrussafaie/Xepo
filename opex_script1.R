library(proto)
library(RSQLite)
library(gsubfn)
library(sqldf)
library(tcltk)

library(sqldf)
library(tcltk)


# data stored 
product_details=read.csv("product_details.csv")
eod_inventory=read.csv("eod_inventory.csv")
product_movement=read.csv("product_movement.csv")

#date transfer
#dat fix
eod_inventory$date<- strptime(eod_inventory$date, format = "%m/%d/%Y")
product_movement$date<- strptime(product_movement$date, "%m/%d/%Y")

eod_inventory$date=as.Date(eod_inventory$date, format = "%m/%d/%Y" )
product_movement$date=as.Date(product_movement$date, format = "%m/%d/%Y" )


class(eod_inventory$date)
class(product_movement$date)

#dev.off()
#material fix
product_details$material=factor(product_details$material)
eod_inventory$material=factor(eod_inventory$material)
product_movement$material=factor(product_movement$material)

# produce movement aggregation
head(product_movement)


product_movement=sqldf("select 
        material,location,date, move_type, sum(total_units) as total_units
        from product_movement
        group by  material,location,date, move_type")


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
max(max(product_movement$date),max(eod_inventory$date))-min(min(product_movement$date),min(eod_inventory$date))

#every date in the data
date.range.array=seq(as.Date("2011-11-30"), as.Date("2014-12-09"), by="days")

length(date.range.array)

#creating the data frame for the repeat
date.array.full=rep(date.range.array,length(unique(product_details$material)))
length(date.array.full)
dim(date.array.full)
paste("the dataframe has",length(date.array.full),"rows")

#class(date.array.full)
date.array.full=as.data.frame(date.array.full)
dim(date.array.full)

head(date.array.full)

#items for the data
items=rep(unique(product_details$material),length(date.range.array))
items=sort(items)

# an ID and product naem added to the data
cxcx=cbind.data.frame(ID=1:dim(date.array.full)[1],date=date.array.full,material=items )
colnames(cxcx)=c("ID","date","material")
dim(cxcx)
##head(cxcx)
# dim(eod_inventory)

#first merging data from eod inventory and cxcx which is combined date.
cxcx1=merge(x=cxcx,y=eod_inventory,by =c("date","material"),all.x=TRUE)
cxcx1$location="Plant A"
dim(cxcx1)

# any ship to customer is negative(outflow) OW inflow

product_movement$total_units=ifelse(product_movement$move_type=="Ship to Customer",-1*product_movement$total_units,product_movement$total_units)
dim(product_movement)
product_movement=sqldf("select material,location,date, sum(total_units) as total_units
          from product_movement
           group by  material,location,date")
dim(product_movement)
hist(product_movement$total_units)

#second merge for 
cxcx2=merge(x=cxcx1,y=product_movement,by =c("date","material","location"),all.x=TRUE)

str(cxcx2)
dim(cxcx2)
tail(cxcx2)
summary(cxcx2)

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


# final_icc=merge(x=icc_cmod[,c("ID","DISTRICT","ACCOUNTING.WEEK","PRODUCT.NUMBER","DESCRIPTION"
#                               ,"DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
#                               ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
#                               ,"VENDOR","VENDOR.NAME")],y=divs, by.x=c("DISTRICT"),by.y=c("div_nbr")
#                 ,all.x=TRUE)[,c("ID","DISTRICT","rgn_nm","div_nm","ACCOUNTING.WEEK","PRODUCT.NUMBER","DESCRIPTION"
#                                 ,"DATE","SOURCE","PO.NUMBER","NEW.UNIT.COST","PRIOR.UNIT.COST"
#                                 ,"UNIT.COST.CHANGE","INVENTORY.VALUE.CHANGE","BUYER","BUYER.NAME"
#                                 ,"VENDOR","VENDOR.NAME","zone_nm")]



# ?merge
# 
# names(product_details)
# names(eod_inventory)
# names(product_movement)

# try association rules on 
#think about some clustering method



