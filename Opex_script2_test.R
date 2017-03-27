
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