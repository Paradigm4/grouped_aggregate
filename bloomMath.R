#ssdreadcost = 8MB disk write on SSD = 8/350 = 0.2285714285714 s
#8e6 bytes in one chunk / 4 bytes per string = 2e6 strings in a chunk
#9e-6 ave time for one insert into bloom
#estimated_element_write = 1e6;
#costBloom = 9e-6 (sec per 4 byte insert) * estimated_num_elements + number_element_check * 2e-6  
#costDisk  = ceil((number_element_check * 4 bytes)%8e6)*8 / 350

number_element_check <- seq(from = 1, to = 1e6, by = 100)

densedata <- TRUE
if(densedata){
  number_chunks_touched <- ceiling((number_element_check * size_element / 1e6) %% chunk_size)
}

#else{
#  dense_case <- 1 #ceiling((number_element_check * size_element / 1e6) %% chunk_size)
#}  

ssd_rate <- 350 #Megs/s
chunk_size <- 8      #Megs
estimated_element_write <- 1e6
size_element <- 4  #bytes
bloom_insert_cost <- 9e-6 #per element 4 byte insert
bloom_check_cost <- 2e-6  #per element 4 byte check
costBloom <- bloom_insert_cost * estimated_element_write + number_element_check * bloom_check_cost

#costDisk <- number_chunks_touched*chunk_size / ssd_rate
costDisk <- number_element_check*chunk_size / ssd_rate

ROI <- costDisk/costBloom
plot(number_element_check , costDisk)
par(new=T)
plot(number_element_check , costBloom)
par(new=F)
plot(number_element_check , ROI)


