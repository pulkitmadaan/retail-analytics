
# PLEASE CHANGE THE PATH OF FILES IN LINES 18,19 AND 20
# LZN FILE IS IN ASHISH LOCAL REMOTE MACHINE.
# CHANGE LINES 149 AND 154 IN CASE THE LZN FILE IS UPDATED


library(reshape)
library(dplyr)
library(RODBC)
library(scales)
library(lubridate)
# library(reshape)

hive_connect <- odbcConnect("Hive_DB")
proc_connect<-  odbcConnect("Proc_b2b_flo")

#Defining date variables
today<-Sys.Date()
wk_end_date <-today-as.POSIXlt(today)$wday-1
wk_start_date <-wk_end_date-6
leadtime_start_date<-wk_start_date-70
#reco_start_date<-wk_start_date-21
#reco_end_date <- wk_end_date-14
lifestyle_end_date <- as.numeric(format(today-wday(today), format="%Y%m%d"))
lifestyle_start_date <- as.numeric(format(today-wday(today)-28, format="%Y%m%d"))
#reco_start_week<- week(format(today-wday(today)-21, format="%Y-%m-%d"))
#reco_end_week<- week(format(today-wday(today)-14, format="%Y-%m-%d"))
temp_week<-week(format(today-wday(today), format="%Y-%m-%d"))
#reco_start_week<-as.numeric(format(Sys.Date(), "%U"))-4
#reco_end_week <-as.numeric(format(Sys.Date(), "%U"))-3
detach(package:lubridate)

# Reading the files - change the file paths
sales<- read.csv("\\\\WIN-7DI0HOLLT49\\QV App\\RRU Inputs\\WK22\\sales.csv")
inventory <- read.csv("\\\\WIN-7DI0HOLLT49\\QV App\\RRU Inputs\\WK22\\inventory.csv")
sales_day <-read.csv("\\\\WIN-7DI0HOLLT49\\QV App\\RRU Inputs\\WK22\\sales_day.csv")

#Reading the static files
lzn <- read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\Overall Mapping.csv")
tax <- read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\Tax based output.csv")
vendor<- read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\Vendor_availability.csv")
U2S <-read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\U2S.csv")
leadtime<-read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\LeadTime.csv")   ####### CHANGE TO NW LOCATION
cat_leadtime<-read.csv("\\\\Win-7di0hollt49\\ipc\\Inventory Policy\\Inputs\\Lead_time_floor.csv")
min_sale_depth <- read.csv("\\\\WIN-7DI0HOLLT49\\Output2\\min_sales_depth.csv")
exclusion_list<- read.csv("\\\\Win-7di0hollt49\\ipc\\Instock Report\\Input\\Exclusion_list.csv")

#Leadtime fix
#library(reshape)
#lead<-readRDS('D:/IPC/Fill_Rate_Lead_time/Lead_time.rds')
##lead<-subset(lead,select = c('fsn','po_oroginal_warehouse_id','lead_time'))
#lead<-cast(lead,fsn~po_oroginal_warehouse_id,mean)
#lead[is.na(lead)]<-0
#leadtime<-lead

#Reading & Cleaning Reco file for National DOH Loss and PO Adherence
temp_date<-wk_start_date - 7+as.POSIXlt(wk_start_date - 5)$wday
reco_wk1<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date,".csv"))
reco_wk2<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-7,".csv"))
reco_wk3<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-14,".csv"))
reco_wk4<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-21,".csv"))
reco_wk5<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-28,".csv"))
reco_wk6<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-35,".csv"))
reco_wk7<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-42,".csv"))
reco_wk8<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-49,".csv"))
reco_wk9<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-56,".csv"))
reco_wk10<-read.csv(paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Projections For Waterfall\\System_generated_auto_projections\\Auto_Reco_File_",temp_date-63,".csv"))


reco_file<-rbind(reco_wk1,reco_wk2,reco_wk3,reco_wk4,reco_wk5,reco_wk6,reco_wk7,reco_wk8)#,reco_wk9,reco_wk10)
#reco_file_ndoh1<-reco_wk1

# Query to pull the lifestyle sales data for 4 weeks

#min_sale <- max(min_sale_depth$min_sales_depth)

odbcDataSources(type = c("all", "user", "system"))
hiveConnect <- odbcConnect("Hive_DB")
ls_4wk_sale<-sqlQuery(hiveConnect,'SET mapred.job.queue.name=adhoc')

query<-paste0("
              SELECT 
              count(distinct ff.fulfill_item_unit_id) as sales,
              p.product_id as fsn,
              p.analytic_vertical as vertical,
              p.analytic_sub_category as sub_category,
              p.analytic_category as category,
              p.analytic_super_category as super_category,
              p.analytic_business_unit as bu,
              p.title as product_title,
              p.brand as brand
              from bigfoot_external_neo.scp_fulfillment__fulfillment_unit_hive_fact ff
              left join bigfoot_external_neo.sp_product__product_categorization_hive_dim p
              on ff.product_id_key = p.product_categorization_hive_dim_key
              left join bigfoot_external_neo.scp_oms__date_dim_fact d
              on ff.fulfill_item_unit_reserve_actual_date_key = d.date_dim_key
              left outer join bigfoot_external_neo.sp_seller__seller_hive_dim seller
              on seller.seller_hive_dim_key = ff.seller_id_key
              where fulfill_item_unit_status_modified not in ('warehouse_cancellation_requested','customer_cancelled','procurement_cancellation_requested','seller_cancelled')
              and is_first_party_seller=1
              and fulfill_item_unit_reserve_actual_date_key > " , lifestyle_start_date,
              " and fulfill_item_unit_reserve_actual_date_key <=", lifestyle_end_date,
              " 
              group by
              p.product_id,
              p.analytic_vertical,
              p.analytic_sub_category,
              p.analytic_category,
              p.analytic_super_category,
              p.analytic_business_unit,
              p.brand,
              p.title
              having count(distinct ff.fulfill_item_unit_id) < 30")

ls_4wk_sale<-sqlQuery(hiveConnect, query)

# # PO Adherence loss query
# po_query<- "select internal_id, po.status as status,origin_warehouse_id,
# po.created_at,po.updated_at,
# expiry_date  ,fsn,	quantity,received_quantity,
# cancel_quantity from purchase_orders as po
# inner join purchase_order_items as poi 
# on poi.purchase_order_id = po.id and 
# date(po.created_at) >=  '2016-11-01'"
# po_adh <- sqlQuery(proc_connect,po_query)
# 
# #Lead time Query
# odbcDataSources(type = c("all", "user", "system"))
# hiveConnect <- odbcConnect("Hive_DB")
# po<-sqlQuery(hiveConnect,'SET mapred.job.queue.name=adhoc')
# po_query<-  "select  po_oroginal_warehouse_id,
# poi_updated_date_key ,poi_initiated_date_key,fsn,poi_status,
# sum(poi_orderd_quantity) as poi_ordered_quantity ,
# sum(poi_received_quantity) as poi_received_quantity
# from  bigfoot_external_neo.retail_procurement__fki_po_poi_l0_fact as po
# where po_created_date_key >=  20161101 
# group by po_oroginal_warehouse_id, poi_updated_date_key,poi_initiated_date_key,fsn,poi_status"
# 
# po <- sqlQuery(hiveConnect, po_query)

# Defining the loss function
loss_attribution<-function(tax_flg,vendor_flag,u2s_flag){
  if(tax_flg==0){loss="tax"}else{
    if(tax_flg==1 & u2s_flag==0){loss="u2s1"}else{
      if(tax_flg==1 & u2s_flag==1 & vendor_flag==0){loss="vendor"}else{loss="N"}
    }
  }
  return(loss)
}

# Attributing  the right loss function
final_loss_class <- function(x){
  y<- unique(x[nchar(x)==max(nchar(x))])
  return(y)}

# Basic Data cleaning & creating flags for the FC unavailability
sales <- sales %>% filter(fsn!="")
names(sales)[14] <- "sales"
#sales<- sales[-1,]
#names(sales)[1] <- "brand"
sales$nat_sales <- with(sales,sales-ru_sales)
#sales_day <- sales_day[-1,]
names(sales_day)[c(1,12,15)]<- c("brand","date","sales")
sales_day$nat_sales <- with(sales_day,sales-ru_sales)
#sales_day$date <- as.Date(as.character(sales_day$date),format="%m/%d/%Y")
sales_day$date <- as.Date(sales_day$date,origin="1899-12-30")
#inventory<- inventory[-1,]
names(inventory)[c(1,4, 9)]<-c("inv_bu","inv_date","inventory")
#inventory$inv_date <- as.Date(as.character(inventory$inv_date),format="%m/%d/%Y")
inventory$inv_date <-  as.Date(inventory$inv_date,origin="1899-12-30")
#names(reco) <-c("date","fsn","FC","qty_recomended")
names(cat_leadtime)[1]='category'

# FC unavailability 
lz1<-names(lzn)[substr(names(lzn),1,2) %in% c('L1','L2','Z1','Z2')]
cols=c('Destination_Pincode',lz1,sep=",")
lzn<- lzn[,which(names(lzn)%in% cols)]	
lzn_1 <- lzn[!(duplicated(lzn)),]
lzn_1$flag <-apply(lzn_1, 1,function(x) sum(x=="0" |x==""))
lzn_1$fc_available_flag <- ifelse(lzn_1$flag==35,0,1)# change this number for RU water or the number of FCs increase

# lzn file cleaning
lzn_2<-melt(subset(lzn_1,select= -c(flag,fc_available_flag)), id=c("Destination_Pincode"))
lzn_3 <- subset(lzn_2, value!=""&value!='0')
names(lzn_3)[2:3]<- c("lzn","FC")
lzn_3$FC <- as.factor(lzn_3$FC)
lzn_3$Destination_Pincode <- as.factor(as.character(lzn_3$Destination_Pincode))

#creating combinatin of dest_pincode & FSN
dest_fc <- sales %>% select(fsn,dest_pincode,brand,bu,category,vertical) %>% distinct
fc<-data.frame(unique(lzn_3$FC)) 
dest_fsn_fc <- merge(dest_fc,fc,by=NULL)
names(dest_fsn_fc)[ncol(dest_fsn_fc)]<- "FC"
lzn_4 <- subset(lzn_3, Destination_Pincode %in% sales$dest_pincode)

#Cleaning tax, vendor and U2S files
tax_1<- subset(tax,select=-c(X)  ,FSN %in% sales$fsn)
tax_1$flag<-apply(tax_1[,-1],1,function(x) sum(x))
tax_2<- subset(tax_1,flag !=13, select=-flag)
tax_trn<- melt(tax_2, id=c("FSN"))
names(tax_trn)[2:3]<- c("FC","tax_flg")
#tax_trn$FC <- substr(tax_trn$variable,3,length(tax_trn$variable))
tax_trn_1 <- subset(tax_trn, tax_flg!=1 & FSN %in% sales$fsn)
names(vendor)[names(vendor)=='Brand']<-'brand'
vendor_1<-melt(vendor, id=c("brand","Vertical","Category"))
names(vendor_1)[4:5]<- c('FC','vendor_flag')
u2s_1<-melt(U2S, id=c('brand','vertical','super_category','category'))
names(u2s_1)[ncol(u2s_1)]<- "u2s_flag"
names(u2s_1)[ncol(u2s_1)-1]<- "FC"


# Creating Master files
master_temp <- merge(dest_fsn_fc,lzn_3,by.x=c('dest_pincode','FC'),by.y=c('Destination_Pincode','FC'),all.x=TRUE)
master_temp_1 <- subset(master_temp, !(is.na(lzn)))
master_temp_2 <- merge(master_temp_1,tax_trn_1,by.x=c('fsn','FC'),by.y=c('FSN','FC'),all.x=TRUE)
master_temp_3<- merge(master_temp_2,vendor_1,by.x=c("brand","vertical","FC"),by.y=c("brand","Vertical","FC"),all.x=TRUE)
master_temp_4 <-merge(master_temp_3,u2s_1,by=c("brand","vertical","FC"),all.x=TRUE)
master_temp_4$tax_flg <- as.numeric(as.character(master_temp_4$tax_flg))
master_temp_4$vendor_flag <- as.numeric(as.character(master_temp_4$vendor_flag))
master_temp_4$u2s_flag <- as.numeric(as.character(master_temp_4$u2s_flag))
master_temp_4$tax_flg <-ifelse(is.na(master_temp_4$tax_flg),1,master_temp_4$tax_flg)
master_temp_4$vendor_flag <-ifelse(is.na(master_temp_4$vendor_flag),1,master_temp_4$vendor_flag)
master_temp_4$u2s_flag <-ifelse(is.na(master_temp_4$u2s_flag),1,master_temp_4$u2s_flag)
master_temp_4$placement_flag <- with( master_temp_4,as.numeric(tax_flg & vendor_flag & u2s_flag))

#Dividing the loss equally for DP where placement is possible
ratio <-subset(master_temp_4, u2s_flag==1 & tax_flg==1 & vendor_flag==1)                                    
ratio_1 <- ratio%>%group_by(fsn,dest_pincode)%>%summarise(fc_count=n_distinct(FC))
ratio_2<-merge(ratio,ratio_1, by=c("fsn","dest_pincode"),all.x=TRUE)
ratio_2$ratio <- with(ratio_2,1/fc_count)
ratio_3<- ratio_2%>%distinct(fsn,FC,dest_pincode,ratio,brand)

#loss tagging for no placement pincodes
no_plc_pin <- anti_join(master_temp_4, ratio, by=c('dest_pincode','fsn'))
no_plc_pin_1<- no_plc_pin%>%rowwise()%>%mutate(loss=loss_attribution(tax_flg,vendor_flag,u2s_flag))
no_plc_pin_2 <- aggregate(loss ~ dest_pincode + fsn,no_plc_pin_1,function(x)final_loss_class(x))
no_plc_pin_2[no_plc_pin_2$loss=="u2s1","loss"]="u2s"

#Loss attribution logic for non placement pincode
sales_sum <- sales %>% group_by(dest_pincode,fsn) %>%
  summarise(sales=sum(sales),
            rru_sales=sum(ru_sales),
            nat_sales=sum(nat_sales))
sales_sum_1<- merge(sales_sum,lzn_1[c("fc_available_flag","Destination_Pincode")],by.x="dest_pincode",by.y="Destination_Pincode",all.x=TRUE)
sales_sum_2<- merge(sales_sum_1,no_plc_pin_2,by=c('dest_pincode','fsn'),all.x=TRUE)
sales_sum_3 <- subset(sales_sum_2, fc_available_flag==0 | !(is.na(loss)) ) #Filtering non placement pincode
sales_sum_3$servicibility_loss <- with(sales_sum_3,ifelse(fc_available_flag==0,nat_sales,0))
sales_sum_3$temp <- with(sales_sum_3,pmax(nat_sales-servicibility_loss,0))
sales_sum_3$tax_loss <- with(sales_sum_3,ifelse(loss=="tax",nat_sales,0))
sales_sum_3$vendor_loss <- with(sales_sum_3,ifelse(loss=="vendor",nat_sales,0))
sales_sum_3$u2s_loss <- with(sales_sum_3,ifelse(loss=="u2s",nat_sales,0))
col=c('tax_loss','vendor_loss','servicibility_loss','u2s_loss')
sales_sum_3[, col][is.na(sales_sum_3[, col])] <- 0
sales_sum_3$temp <- with(sales_sum_3,pmax(nat_sales-tax_loss-vendor_loss-servicibility_loss-u2s_loss,0))

#Loss ratio logic for pincode where placement is possible
sales_sum_4<- sales %>% group_by(dest_pincode,fsn,source_id,brand,bu,category,super_category,sub_category,vertical) %>%
  summarise(sales=sum(sales),
            rru_sales=sum(ru_sales),
            nat_sales=sum(nat_sales))
sales_sum_5 <- anti_join(sales_sum_4,sales_sum_3,by=c("fsn","dest_pincode")) # removing dest_pincode, fsn where placement is not possible
names(sales_sum_5)[names(sales_sum_5)=="source_id"]='FC'
#sales_sum_5_1 <- anti_join(sales_sum_5,subset(master_temp_4,placement_flag==0),by=c("fsn","dest_pincode","FC")) # removing placement constraint fc with DP where placement is possible
col=c('fsn','dest_pincode','FC','brand','ratio')
sales_sum_6<- merge(ratio_3[,col],sales_sum_5, by=c('fsn','dest_pincode','FC'),all.x=TRUE) 
#Note :ratio 2should be on left as sales_sum_5 might have fc's where sales has not happened and we have to divide the loss for those fc's as well
sales_sum_7 <-sales_sum_5%>%group_by(dest_pincode,fsn)%>%
  summarise(loss_at_dp_fsn_lvl=sum(nat_sales,na.rm=TRUE))
#Note : DO not sum the sales_sum_6 to get DP - FSN level sales
sales_sum_8 <-merge(sales_sum_6,sales_sum_7,by=c('fsn','dest_pincode'),all.x=TRUE)
sales_sum_8$loss_post_ratio <-with(sales_sum_8,ratio*loss_at_dp_fsn_lvl)

# Low sale depth loss

ls_4wk_sale_1 <- left_join(ls_4wk_sale,min_sale_depth,by="category",all.x=TRUE)
low_sale<- subset(ls_4wk_sale_1,sales < min_sales_depth)

#EXCLUSION LOSS

exclusion_list$exclusion_flag<-1

library(lubridate)
# #National DOH Loss
# reco_file_ndoh1$weeknum<- with(reco_file_ndoh1,week(as.POSIXct(Date,format='%d/%m/%Y')))
# #reco_file_ndoh1$weeknum<- with(reco_file_ndoh1,week(reco_date))
# reco_file_ndoh<-reco_file_ndoh1 %>% rename(fsn=FSN) %>% group_by(fsn,FC,weeknum)%>%
#   summarise(projected_qty = sum(Qty.Generated),
#             recommended_qty= sum(Qty.Recommended))
# reco_file_ndoh$national_doh<-reco_file_ndoh$projected_qty-reco_file_ndoh$recommended_qty

# PO Adherence New LOGIC

names(reco_file)[1]='reco_date'
reco_file$reco_date<- with(reco_file,as.POSIXct(reco_date,format='%d/%m/%Y'))
reco_file$weeknum<- with(reco_file,week(reco_date))
reco_file$FSN<- as.factor(reco_file$FSN)
leadtime_1<-melt(leadtime, id=c("fsn"))
names(leadtime_1)[names(leadtime_1)=='value']='policy_lead_time'
leadtime_1$FC <- substr(leadtime_1$variable,15,length(leadtime_1$variable))
sales_leadtime<-sales[!duplicated(sales[,c(3,6)]),c(3,6)]
leadtime_1<- merge(leadtime_1, sales_leadtime, by='fsn', all.x= TRUE)
leadtime_1<-merge(leadtime_1,cat_leadtime[,1:2], by='category', all.x= TRUE)
leadtime_1$policy_lead_time = ifelse(is.na(leadtime_1$policy_lead_time),7,
                                     ifelse(leadtime_1$policy_lead_time==0,leadtime_1$Lead_time_floor,leadtime_1$policy_lead_time))
leadtime_1$reco_date<- with(leadtime_1,wk_start_date - policy_lead_time-as.POSIXlt(wk_start_date - policy_lead_time)$wday)
leadtime_1$reco_week<- with(leadtime_1,week(reco_date))

reco_leadtime<-merge(leadtime_1, reco_file, by.x=c("fsn","FC","reco_week"), by.y=c("FSN","FC","weeknum"))
### FOrecast Qty #########v7

sales_forecast<- sales %>% group_by(fsn,source_id) %>% summarise(sales=sum(sales))

reco_leadtime$forecast_qty<- with(reco_leadtime,ifelse(ceiling(reco_leadtime$policy_lead_time/7)==1,Forecast_week_1,
                                                       ifelse(ceiling(reco_leadtime$policy_lead_time/7)==2,Forecast_week_2,
                                                              ifelse(ceiling(reco_leadtime$policy_lead_time/7)==3,Forecast_week_3,
                                                                     ifelse(ceiling(reco_leadtime$policy_lead_time/7)==4,Forecast_week_4,
                                                                            (Forecast_week_1+Forecast_week_2+Forecast_week_3+Forecast_week_4)/4)))))

reco_leadtime_1<- reco_leadtime%>%group_by(fsn,FC,reco_week)%>% 
  summarise(total_qty_recomended=sum(Qty.Recommended),total_forecast_qty=sum(forecast_qty),projected_qty = sum(Qty.Generated))  #####v7

forecast_error<- merge(sales_forecast,reco_leadtime_1[c(-3,-4,-6)], by.x = c("fsn","source_id"), by.y= c("fsn", "FC"), all.x=TRUE)
forecast_error$total_forecast_qty <- with(forecast_error, ifelse(is.na(total_forecast_qty),0,total_forecast_qty))
forecast_error$forecast_diff<- with(forecast_error, pmax(sales-total_forecast_qty,0))
names(forecast_error)[names(forecast_error)=="source_id"]= "FC"
forecast_error<-forecast_error[c(1,2,5)]

#NDOH
reco_leadtime_1$total_qty_recomended <- with(reco_leadtime_1, ifelse(is.na(total_qty_recomended),0,total_qty_recomended))
reco_leadtime_1$projected_qty <- with(reco_leadtime_1, ifelse(is.na(projected_qty),0,projected_qty))
reco_file_ndoh<- reco_leadtime_1[,-5] %>% mutate(national_doh = projected_qty-total_qty_recomended)
reco_file_ndoh <- reco_file_ndoh[c(1,2,6)]
##########
po_query<- "select internal_id, po.status ,origin_warehouse_id,
po.created_at,po.updated_at,
expiry_date  ,fsn,	quantity ,received_quantity,
cancel_quantity from purchase_orders as po
inner join purchase_order_items as poi 
on poi.purchase_order_id = po.id and 
date(po.created_at) >=  '2016-11-01'"
po_adh <- sqlQuery(proc_connect,po_query)

po_adh$created_at <- with(po_adh,as.Date(po_adh$created_at))
po_adh$weeknum<-week(po_adh$created_at)
names(po_adh)[3]='FC'
po_adh_1 <- po_adh %>%
  filter(status !='cancelled' )%>%
  mutate(po_raised_qty = quantity-cancel_quantity)
po_adh_2 <- po_adh_1 %>% group_by(fsn,FC,weeknum)%>%
  summarise(total_po_raised_qty = sum(po_raised_qty))

po_reco_leadtime <- merge(po_adh_2,reco_leadtime_1,by.x=c("FC","fsn","weeknum"),by.y=c("FC","fsn","reco_week"), all.y = TRUE)
po_adh_final<-po_reco_leadtime %>% group_by(fsn,FC) %>% summarise(total_po_raised_qty = sum(total_po_raised_qty, na.rm= TRUE), final_qty_recommended=sum(total_qty_recomended, na.rm= TRUE))

#LEADTIME ERROR

odbcDataSources(type = c("all", "user", "system"))
hiveConnect <- odbcConnect("Hive_DB")
po_query_1<-sqlQuery(hiveConnect,'SET mapred.job.queue.name=adhoc')
po_query<-  "select  po_oroginal_warehouse_id,
poi_updated_date_key ,poi_initiated_date_key,fsn,poi_status,
sum(poi_orderd_quantity) as poi_ordered_quantity ,
sum(poi_received_quantity) as poi_received_quantity
from  bigfoot_external_neo.retail_procurement__fki_po_poi_l0_fact as po
where po_created_date_key >=  20161101 
group by po_oroginal_warehouse_id, poi_updated_date_key,poi_initiated_date_key,fsn,poi_status"
po <- sqlQuery(hiveConnect, po_query)

po$lead_time <- with (po,  poi_updated_date_key - poi_initiated_date_key)
po_1 <- merge(po,leadtime_1,by.x=c("fsn","po_oroginal_warehouse_id"),by.y=c("fsn","FC"),all.x=TRUE)
po_2<- subset(po_1, poi_status %in% c("approved","completed"))
po_2$poi_initiated_date<- as.Date(as.character(po_2$poi_initiated_date_key),format="%Y%m%d")
po_3 <- po_2 %>%
  mutate(policy_lead_time = ifelse(is.na(policy_lead_time),7,policy_lead_time))%>%
  filter( (poi_status=='approved' & (poi_initiated_date + policy_lead_time >=leadtime_start_date & 
           poi_initiated_date + policy_lead_time<=wk_end_date)) | (poi_status=='completed' & (poi_initiated_date + policy_lead_time >=wk_start_date & 
                                                                                               poi_initiated_date + policy_lead_time<=wk_end_date)) )%>%
  dplyr::rename(FC=po_oroginal_warehouse_id)
po_4 <- po_3 %>% group_by(fsn,FC)%>%
  summarise(po_received_quantity = sum(poi_received_quantity),
            po_ordered_quantity = sum(poi_ordered_quantity))
po_4$po_received_quantity <- with(po_4, ifelse(is.na(po_received_quantity),0,po_received_quantity))
po_4$po_ordered_quantity <- with(po_4, ifelse(is.na(po_ordered_quantity),0,po_ordered_quantity))

# Promise Engine loss : Additional inventory in each FC per day
sales_day_1<- sales_day %>% group_by(fsn,source_id,date)%>%
  summarise(ru_sales=sum(ru_sales,na.rm=TRUE),
            loss =sum(nat_sales,na.rm=TRUE))
inventory_1<- inventory %>% group_by(inv_fsn,inv_source_id,inv_date)%>%summarise(inventory=sum(inventory,na.rm=TRUE))
pe_loss<-merge(sales_day_1,inventory,by.x=c('fsn','source_id','date'),by.y=c('inv_fsn','inv_source_id','inv_date'),all.x=TRUE)
pe_loss[is.na(pe_loss$inventory),"inventory"]<-0
pe_loss$pe_excess_inv <- with(pe_loss,pmin(pmax(inventory-ru_sales,0),loss))
pe_loss_1<- pe_loss %>% group_by(fsn,source_id)%>% 
  summarise(pe_excess_inv=sum(pe_excess_inv))%>%dplyr::rename(FC=source_id)

# OFFERS AND PROMOTIONS

offer_query<-paste0("select order_item_product_id, sum(gross_units_per_offer) as offer_sales
                    from bigfoot_external_neo.cp_santa__offer_sales_cancellations_hive_fact a
                    where offer_flag=1 and is_first_party_seller=1 and a.date_dim_key>=", as.numeric(format(wk_start_date,"%Y%m%d")),
                    " and a.date_dim_key <=", as.numeric(format(wk_end_date,"%Y%m%d")) ,"
                    group by
                    order_item_product_id")

offer_sales<-sqlQuery(hiveConnect, offer_query)
names(offer_sales)[c(1,2)]= c('fsn','offer_sales')
# Addding & attributing the loss for DP where placement is possible ########v7
library(plyr)
sales_sum_81<- join_all(list(sales_sum_8,exclusion_list,offer_sales), by='fsn', type='left') #CHANGE TO LEFT##########
sales_sum_9 <-join_all(list(sales_sum_81,po_4,po_adh_final,pe_loss_1,reco_file_ndoh,forecast_error), by=c('fsn','FC'), type='left') #CHANGE TO LEFT##########
detach(package:plyr)
cols=c('sales','rru_sales','nat_sales','po_received_quantity','po_ordered_quantity','final_qty_recommended','total_po_raised_qty','pe_excess_inv','national_doh','loss_post_ratio','exclusion_flag','offer_sales','forecast_diff')
sales_sum_9[cols][is.na(sales_sum_9[cols])] <- 0


sales_sum_9$pe_loss <- with(sales_sum_9,pmin(loss_post_ratio,pe_excess_inv))
sales_sum_9$temp <- with(sales_sum_9,pmax(loss_post_ratio-pe_loss,0))

sales_sum_9$forecast_error <- with(sales_sum_9,pmin(temp,forecast_diff))
sales_sum_9$temp <- with(sales_sum_9,pmax(temp-forecast_error,0))

sales_sum_9$ndoh_loss <- with(sales_sum_9,pmin(temp,national_doh))
sales_sum_9$temp <- with(sales_sum_9,pmax(temp-ndoh_loss,0))

sales_sum_9$exclusion_loss <- with(sales_sum_9,ifelse(sales_sum_9$exclusion_flag==1,temp,0))
sales_sum_9$temp <- with(sales_sum_9,pmax(temp-exclusion_loss,0))

sales_sum_9$adherence_loss <- with(sales_sum_9,pmin(temp,pmax(final_qty_recommended-total_po_raised_qty,0)))
sales_sum_9$temp <- with(sales_sum_9,pmax(temp-adherence_loss,0))

sales_sum_9$lead_time_loss <- with(sales_sum_9,pmin(temp,pmax(po_ordered_quantity-po_received_quantity,0)))
sales_sum_9$temp <- with(sales_sum_9,pmax(temp-lead_time_loss,0))

sales_sum_9$promotions_loss<-with(sales_sum_9,ifelse(sales_sum_9$offer_sales>0,pmin(temp,offer_sales),0))
sales_sum_9$residual <- with(sales_sum_9,pmax(temp-promotions_loss,0))
#Append the losses from the codes where placement is possible and not possible

n_placement_loss <-sales_sum_3%>%group_by(fsn,dest_pincode)%>%
  summarise(
    servicibility_loss=sum(servicibility_loss),
    tax_loss=sum(tax_loss),
    vendor_loss=sum(vendor_loss),
    u2s_loss=sum(u2s_loss))

y_placement_loss <- sales_sum_9%>%group_by(fsn,dest_pincode)%>%
  summarise(
    ndoh_loss=sum(ndoh_loss,na.rm=TRUE),
    pe_loss=sum(pe_loss,na.rm=TRUE),
    exclusion_loss=sum(exclusion_loss,na.rm=TRUE),
    adherence_loss=sum(adherence_loss,na.rm=TRUE),
    lead_time_loss=sum(lead_time_loss,na.rm=TRUE),
    forecast_error=sum(forecast_error,na.rm=TRUE),
    promotions_loss=sum(promotions_loss,na.rm=TRUE))


rru_waterfall<- bind_rows(n_placement_loss,y_placement_loss)
rru_waterfall_1 <- merge(sales_sum,rru_waterfall,by=c('fsn','dest_pincode'),all.x=TRUE)
rru_waterfall_1[3:16][is.na(rru_waterfall_1[3:16])] <-0
cols<- c("fsn","bu","super_category","category","sub_category","vertical","brand")
prod_temp <- sales[!duplicated(sales[cols]),]
prod_temp_1<- subset(prod_temp,select=cols)
rru_waterfall_2 <- merge(rru_waterfall_1,prod_temp_1,by="fsn")
rru_waterfall_3 <- rru_waterfall_2%>% mutate( low_sale_depth_loss = ifelse(fsn %in% low_sale$fsn,nat_sales-servicibility_loss-tax_loss-u2s_loss,0))
cols <- c("vendor_loss","ndoh_loss","pe_loss","exclusion_loss","adherence_loss","lead_time_loss","forecast_error","promotions_loss")
rru_waterfall_3[,cols][rru_waterfall_3$low_sale_depth>0,]<-0
rru_waterfall_3$residual <- with(rru_waterfall_3,
                                 pmax(nat_sales-	servicibility_loss-	tax_loss-
                                        vendor_loss-	u2s_loss-low_sale_depth_loss- ndoh_loss-	pe_loss	-adherence_loss-	lead_time_loss-forecast_error-promotions_loss-exclusion_loss,0))
names(rru_waterfall_3)[names(rru_waterfall_3)=='rru_sales']="ru_sales"
rru_waterfall_final<- rru_waterfall_3
ru_agg<-aggregate(cbind( ru_sales ,nat_sales, servicibility_loss,tax_loss,
                         u2s_loss,low_sale_depth_loss,vendor_loss,ndoh_loss,pe_loss,exclusion_loss,adherence_loss,lead_time_loss,forecast_error,promotions_loss,residual) ~ bu, rru_waterfall_3,sum)

#ru_agg_per<- as.data.frame(apply(ru_agg[,-1],1,function(x) round(x*100/sum(x[c(1,2)]),2)))
#colnames(ru_agg_per)<- ru_agg[,1]

# Title Mapping
atp<-readRDS("D:/IPC/IPC Data Crons/Historical Inventory Data/Inventory_2017-03-15.rds")
atp<-atp[,c("product_fsn","product_title")]
colnames(atp)<-c("fsn","title")
atp<-unique(atp[,1:2])

rru_waterfall_final<-merge(rru_waterfall_final,atp,by="fsn",all.x = TRUE)

# Zone Mapping
destzone<-read.csv("C:\\Users\\asan.kumar\\Desktop\\FC-Zone.csv")
destzone<-destzone[,c("Pincode","Zone")]
colnames(destzone)<-c("dest_pincode","Zone")
destzone<-unique(destzone[,1:2])

rru_waterfall_final<-merge(rru_waterfall_final,destzone,by="dest_pincode",all.x = TRUE)

#LZ Fcs
lzfc<-read.csv("D:\\IPC\\QV App\\Mapping - Concatenate.csv")
colnames(lzfc)<-c("dest_pincode","LZ-FC")
rru_waterfall_final<-merge(rru_waterfall_final,lzfc,by="dest_pincode",all.x = TRUE)


library(lubridate)
path <-paste0("\\\\WIN-7DI0HOLLT49\\QV App\\Waterfall Output\\RU\\ru_agg_",temp_week,".csv")
write.csv(ru_agg,path,row.names=FALSE)
path <-paste("\\\\WIN-7DI0HOLLT49\\QV App\\Waterfall Output\\RU\\ru_waterfall_",temp_week,".csv")
write.csv(rru_waterfall_final,path,row.names=FALSE)
detach(package:lubridate)




