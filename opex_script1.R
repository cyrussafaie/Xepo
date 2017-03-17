

# data stored 
product_details=read.csv("product_details.csv")
eod_inventory=read.csv("eod_inventory.csv")
product_movement=read.csv("product_movement.csv")

#date transfer
#dat fix
eod_inventory$date<- strptime(eod_inventory$date, "%m/%d/%Y")
product_movement$date<- strptime(product_movement$date, "%m/%d/%Y")

#material fix
product_details$material=as.factor(product_details$material)
eod_inventory$material=as.factor(eod_inventory$material)
product_movement$material=as.factor(product_movement$material)

# data structure 
str(product_details)
str(eod_inventory)
str(product_movement)

# names in each data set
head(product_details)
head(eod_inventory)
head(product_movement)

# summary
summary(product_details,maxsum=100)
summary(eod_inventory)
summary(product_movement)

