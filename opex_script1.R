

# data stored 
product_details=read.csv("product_details.csv")
eod_inventory=read.csv("eod_inventory.csv")
product_movement=read.csv("product_movement.csv")

#date transfer
#dat fix
eod_inventory$date<- strptime(eod_inventory$date, "%m/%d/%Y")
product_movement$date<- strptime(product_movement$date, "%m/%d/%Y")

#material fix
product_details$material=factor(product_details$material)
eod_inventory$material=factor(eod_inventory$material)
product_movement$material=factor(product_movement$material)

# data structure 
str(product_details)
str(eod_inventory)
str(product_movement)

# names in each data set
head(product_details)
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

paste("the dataframe has",length(date.array.full),"rows")


