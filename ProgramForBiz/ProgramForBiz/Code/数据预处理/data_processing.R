###导入数据集并查看数据集内部结构###
aurism <- read.csv("/Users/argebell/Desktop/研一课程/programming/group\ project/autism_screening.csv")#导入数据集
summary(aurism)#查看数据集内部结构
###异常值处理###
aurism$age[aurism$age==383] <- NA#将 383 视为缺失值 
summary(aurism$age)#查看处理后 age 的情况 
###属性值合并###
aurism$contry_of_res = as.factor(ifelse(aurism$contry_of_res %in% c("Canada", "Mexico", "Nicaragua", "United States"), "North America", ifelse(aurism$contry_of_res %in% c("AmericanSamoa", "Australia", "New Zealand", "Tonga"), "Oceania", ifelse(aurism$contry_of_res %in% c("Angola", "Burundi", "Egypt", "Ethiopia", "Niger", "Sierra Leone", "South Africa"), "Africa", ifelse(aurism$contry_of_res %in% c("Argentina", "Aruba", "Bahamas", "Bolivia", "Chile", "Costa Rica", "Ecuador", "Uruguay"), "South America", ifelse(aurism$contry_of_res %in% c("Austria", "Azerbaijan", "Belgium", "Brazil", "Czech Republic", "Finland", "France", "Germany", "Iceland", "Ireland", "Italy", "Netherlands", "Portugal", "Romania", "Russia", "Serbia", "Spain", "Sweden", "Ukraine", "United Kingdom"), "Europea", "Asia"))))))#将 contry_of_res 根据国家所在的洲进行划分
aurism$ethnicity[aurism$ethnicity %in% c("?","others", "Hispanic", "Latino", "Pasifika", "Turkish")]="Others"#将 ethnicity 的部分属性值合并
aurism$relation = as.factor(ifelse(aurism$relation =="Self", "Self", ifelse(aurism$relation =="?", "?", "Other")))#将 relation 划分为 Self 和 Other
aurism$age=as.factor(ifelse(aurism$age <= 27 , "≤27", ">27"))#将 age 划分为“≤27”和“> 27”
summary(aurism)#查看数据集内部结构
###查看缺失值分布###
library(VIM)#导入 vim 包
aurism[aurism == "?"] <- NA #将?转化为 NA 
aggr(aurism,prop=T,numbers=T)#缺失值分布可视化
pMiss <- function(x){sum(is.na(x)) / length(x) * 100}#定义查看缺失数据比例的函数 
apply(aurism, 2, pMiss)#查看各属性缺失值比例
###利用多重插补法处理缺失值###
library(lattice)#导入 lattice 包
library(MASS)#导入 MASS 包
library(nnet)#导入 nnet 包
library(mice)#导入 mice 包
miceMod <- mice(aurism[, !names(aurism) %in% "Class"], m = 5, seed = 1234)
aurism1 <- complete(miceMod, action = 3) # 生成完整数据 
anyNA(aurism1)#查看插补后的数据集时候存在缺失值
aurism$age = aurism1$age#将插补后的 age 放入 aurism 中 
aurism$relation = aurism1$relation#将插补后的 relation 放入 aurism 中
#进行 mice 插
summary(aurism)#查看数据集内部结构
###AVF 检测离群值###
freq_matrix <- table( unlist( unname(aurism) ) )
aurism[,"Score"] <- apply(aurism,1,function(x) { sum(freq_matrix[x]) / length(x) })#计算 AVF 
aurism1 <- aurism[order(aurism$Score),]#将数据集按照 Score 排列 
summary(aurism1)#查看数据集内部结构
aurism2<-aurism1[36:704,1:22]#删除 AVF 值较小的 5%的记录 summary(aurism2)#查看数据集内部结构
###保存预处理后的数据集###
write.csv(aurism2, "/Users/argebell/Desktop/aurism_processing.csv")#将处理后的训练集导出