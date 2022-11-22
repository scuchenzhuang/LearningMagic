aurism3 <- read.csv("/Users/argebell/Desktop/研一课程/programming/group\ project/数据预处理/aurism_processing.csv")#导入数据
###lasso 变量选择###
library(Matrix) #导入 Matrix 包
library(glmnet)#导入 glmnet 包
grid = 10^seq(10, -3, length = 100) # 生成 100 个λ值
x = model.matrix(Class~., aurism3)[, -1] # 构建回归设计矩阵
y = as.vector(ifelse(aurism3$Class=="NO", 0, 1))#将 y 转化为向量形式 
set.seed(1)#产生随机数
train = sample(1:nrow(x),nrow(x)*0.7)#将 70%的数据划分为训练集 
test = (-train)#将剩下的数据划分为测试集
y.test = y[test]#将测试集中的标记赋给 y_test
lm.las = glmnet(x[train, ], y[train], alpha = 1, lambda = grid)#lasso 回归 plot(lm.las) # 观察各变量的系数
cv.out = cv.glmnet(x[train,], y[train], alpha =1)
plot(cv.out)#绘制 MSE 关于 log(λ)的图
bestlam = cv.out$lambda.min#最佳λ值
bestlam
lasso.pred = predict(lm.las, s = bestlam, newx = x[test,])#基于训练集进行预测 mean((lasso.pred-y.test)^2)#输出 MSE
lasso.coef = predict(glmnet(x[train,], y[train], alpha = 1, lambda = grid), type = "coefficients", s = bestlam)#计算 lasso 回归各自变量系数
print(lasso.coef)#输出各自变量系数



###最优子集选择###
library(leaps)#regsubsets 函数在 leaps 宏包中 
regfit.full=regsubsets(Class~.,aurism3,really.big=T,nvmax=27)#全子集回归 
summary(regfit.full)
reg.summary=summary(regfit.full)#提取回归结果 
names(reg.summary)#查看回归后保留的变量
summary(reg.summary)
plot(reg.summary$bic,xlab="Number of Variables",ylab="BIC",type='l')#绘制 BIC 图 
which.min(reg.summary$bic) #寻求 BIC 最小值点 
points(13,reg.summary$bic[13],col="red",cex=2,pch=20) #标注最小值点 
coef(regfit.full,13) #提取 BIC 最小值点对应的参数估计值

###随机森林变量选择###
library(varSelRF)#导入 varSelRF 包
rf.vs1 <- varSelRF(aurism3[1:18], aurism3$Class, ntree = 3000, ntreeIterat = 200,vars.drop.frac = 0.2)
rf.vs1#查看选择的变量
plot(rf.vs1)

